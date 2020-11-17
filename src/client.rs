use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

use super::eval::{self, Detail};
use super::event_processor::EventProcessor;
use super::event_sink::EventSink;
use super::events::{Event, MaybeInlinedUser};
use super::store::{FeatureFlag, FeatureStore, FlagValue};
use super::update_processor::{StreamingUpdateProcessor, UpdateProcessor};
use super::users::User;

use futures::future;
use serde::Serialize;
use thiserror::Error;

const DEFAULT_STREAM_BASE_URL: &str = "https://stream.launchdarkly.com";
const DEFAULT_EVENTS_BASE_URL: &str = "https://events.launchdarkly.com";

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid client config: {0}")]
    InvalidConfig(String),
    #[error("couldn't spawn background thread for client: {0}")]
    SpawnFailed(io::Error),
    #[error("failed to flush events: {0}")]
    FlushFailed(String),
    #[error("unexpected internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Client {
    //sdk_key: String,
    config: Config,
    event_processor: EventProcessor,
    update_processor: Arc<Mutex<dyn UpdateProcessor>>,
    store: Arc<Mutex<FeatureStore>>,
}

#[derive(Clone, Copy)]
pub struct Config {
    inline_users_in_events: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            inline_users_in_events: false,
        }
    }
}

pub struct ClientBuilder {
    stream_base_url: String,
    events_base_url: String,
    config: Config,
    update_processor: Option<Arc<Mutex<dyn UpdateProcessor>>>,
    event_sink: Option<Arc<RwLock<dyn EventSink>>>,
}

impl ClientBuilder {
    pub fn stream_base_url<'a>(&'a mut self, url: &str) -> &'a mut ClientBuilder {
        let url = trim_base_url(url);
        self.stream_base_url = url.to_owned();
        self
    }

    pub fn events_base_url<'a>(&'a mut self, url: &str) -> &'a mut ClientBuilder {
        let url = trim_base_url(url);
        self.events_base_url = url.to_owned();
        self
    }

    pub fn inline_users_in_events(&mut self, inline: bool) -> &mut ClientBuilder {
        self.config.inline_users_in_events = inline;
        self
    }

    pub fn update_processor(
        &mut self,
        processor: Arc<Mutex<dyn UpdateProcessor>>,
    ) -> &mut ClientBuilder {
        self.update_processor = Some(processor);
        self
    }

    #[cfg(test)]
    fn event_sink(&mut self, sink: Arc<RwLock<dyn EventSink>>) -> &mut ClientBuilder {
        self.event_sink = Some(sink);
        self
    }

    /// Starts a client in the current thread, which must have a default tokio runtime.
    pub fn start_with_default_executor(&self, sdk_key: &str) -> Result<Client> {
        let mut client = self.build(sdk_key)?;
        client.start_with_default_executor();
        Ok(client)
    }

    /// Starts a client in a background thread, with its own tokio runtime.
    pub fn start(&self, sdk_key: &str) -> Result<Arc<RwLock<Client>>> {
        let client = self.build(sdk_key)?;

        let rw = Arc::new(RwLock::new(client));

        let mut runtime = tokio::runtime::Runtime::new().map_err(Error::SpawnFailed)?;

        let w = rw.clone();

        thread::spawn(move || {
            runtime.spawn(future::lazy(move || {
                let mut client = w.write().unwrap();
                client.start_with_default_executor();
                future::ok(())
            }));

            runtime.shutdown_on_idle()
        });

        Ok(rw)
    }

    fn build(&self, sdk_key: &str) -> Result<Client> {
        let store = FeatureStore::new();
        let event_processor = match &self.event_sink {
            None => reqwest::Url::parse(&self.events_base_url)
                .map_err(|e| Error::InvalidConfig(format!("couldn't parse events_base_url: {}", e)))
                .and_then(|base_url| {
                    EventProcessor::new(base_url, sdk_key).map_err(|e| {
                        Error::InvalidConfig(format!("invalid events_base_url: {}", e))
                    })
                })?,
            Some(sink) => {
                let sink = sink.clone(); // so we don't have to consume builder
                EventProcessor::new_with_sink(sink).map_err(Error::Internal)?
            }
        };
        let update_processor = match &self.update_processor {
            None => Arc::new(Mutex::new(
                StreamingUpdateProcessor::new(&self.stream_base_url, sdk_key).map_err(|e| {
                    Error::InvalidConfig(format!("invalid stream_base_url: {:?}", e))
                })?,
            )),
            Some(update_processor) => update_processor.clone(),
        };
        Ok(Client {
            config: self.config,
            event_processor,
            update_processor,
            store: Arc::new(Mutex::new(store)),
        })
    }
}

impl Client {
    pub fn new(sdk_key: &str) -> Client {
        Client::configure().build(sdk_key).unwrap()
    }

    pub fn configure() -> ClientBuilder {
        ClientBuilder {
            config: Config::default(),
            stream_base_url: DEFAULT_STREAM_BASE_URL.to_string(),
            events_base_url: DEFAULT_EVENTS_BASE_URL.to_string(),
            update_processor: None,
            event_sink: None,
        }
    }

    fn start_with_default_executor(&mut self) {
        self.update_processor
            .lock()
            .unwrap()
            .subscribe(self.store.clone())
    }

    pub fn flush(&self) -> Result<()> {
        self.event_processor.flush().map_err(Error::FlushFailed)
    }

    pub fn identify(&self, user: User) {
        let event = Event::new_identify(user);

        self.send_internal(event);
    }

    pub fn bool_variation(&self, user: &User, flag_key: &str, default: bool) -> bool {
        let val = self.evaluate(user, flag_key, default.into());
        if let Some(b) = val.as_bool() {
            b
        } else {
            warn!(
                "bool_variation called for a non-bool flag {:?} (got {:?})",
                flag_key, val
            );
            default
        }
    }

    pub fn str_variation(&self, user: &User, flag_key: &str, default: String) -> String {
        let val = self.evaluate(user, flag_key, default.clone().into());
        if let Some(s) = val.as_string() {
            s
        } else {
            warn!(
                "str_variation called for a non-string flag {:?} (got {:?})",
                flag_key, val
            );
            default
        }
    }

    pub fn float_variation(&self, user: &User, flag_key: &str, default: f64) -> f64 {
        let val = self.evaluate(user, flag_key, default.into());
        if let Some(f) = val.as_float() {
            f
        } else {
            warn!(
                "float_variation called for a non-float flag {:?} (got {:?})",
                flag_key, val
            );
            default
        }
    }

    pub fn int_variation(&self, user: &User, flag_key: &str, default: i64) -> i64 {
        let val = self.evaluate(user, flag_key, default.into());
        if let Some(f) = val.as_int() {
            f
        } else {
            warn!(
                "int_variation called for a non-int flag {:?} (got {:?})",
                flag_key, val
            );
            default
        }
    }

    pub fn json_variation(
        &self,
        user: &User,
        flag_key: &str,
        default: serde_json::Value,
    ) -> serde_json::Value {
        self.evaluate(user, flag_key, default.clone().into())
            .as_json()
            .unwrap_or_else(|| default.clone())
    }

    pub fn bool_variation_detail(
        &self,
        user: &User,
        flag_key: &str,
        default: bool,
    ) -> Detail<bool> {
        self.evaluate_detail(user, flag_key, default.into())
            .try_map(|val| val.as_bool(), eval::Error::Exception)
    }

    pub fn str_variation_detail(
        &self,
        user: &User,
        flag_key: &str,
        default: String,
    ) -> Detail<String> {
        self.evaluate_detail(user, flag_key, default.into())
            .try_map(|val| val.as_string(), eval::Error::Exception)
    }

    pub fn float_variation_detail(&self, user: &User, flag_key: &str, default: f64) -> Detail<f64> {
        self.evaluate_detail(user, flag_key, default.into())
            .try_map(|val| val.as_float(), eval::Error::Exception)
    }

    pub fn int_variation_detail(&self, user: &User, flag_key: &str, default: i64) -> Detail<i64> {
        self.evaluate_detail(user, flag_key, default.into())
            .try_map(|val| val.as_int(), eval::Error::Exception)
    }

    pub fn json_variation_detail(
        &self,
        user: &User,
        flag_key: &str,
        default: serde_json::Value,
    ) -> Detail<serde_json::Value> {
        self.evaluate_detail(user, flag_key, default.into())
            .try_map(|val| val.as_json(), eval::Error::Exception)
    }

    pub fn all_flags_detail(&self, user: &User) -> HashMap<String, Detail<FlagValue>> {
        let store = self.store.lock().unwrap();
        let flags = store.all_flags();
        let evals = flags.iter().map(|(key, flag)| {
            let val = flag.evaluate(user, &store).map(|v| v.clone());
            (key.clone(), val)
        });
        evals.collect()
    }

    pub fn evaluate_detail(
        &self,
        user: &User,
        flag_key: &str,
        default: FlagValue,
    ) -> Detail<FlagValue> {
        let default_for_event = default.clone();

        let (flag, result) = self.evaluate_internal(user, flag_key, default);

        let event = Event::new_feature_request(
            flag_key,
            MaybeInlinedUser::new(self.config.inline_users_in_events, user.clone()),
            flag,
            result.clone(),
            default_for_event,
            true,
        );
        self.send_internal(event);

        result
    }

    pub fn evaluate(&self, user: &User, flag_key: &str, default: FlagValue) -> FlagValue {
        let default_for_event = default.clone();

        let (flag, result) = self.evaluate_internal(user, flag_key, default);

        let event = Event::new_feature_request(
            flag_key,
            MaybeInlinedUser::new(self.config.inline_users_in_events, user.clone()),
            flag,
            result.clone(),
            default_for_event,
            false,
        );
        self.send_internal(event);

        // unwrap is safe here because value should have been replaced with default if it was None.
        // TODO that is ugly, use the type system to fix it
        result.value.unwrap()
    }

    pub fn track_event(&self, user: User, key: impl Into<String>) {
        let _ = self.track(user, key, None, serde_json::Value::Null);
    }

    pub fn track_data(
        &self,
        user: User,
        key: impl Into<String>,
        data: impl Serialize,
    ) -> serde_json::Result<()> {
        self.track(user, key, None, data)
    }

    pub fn track_metric(&self, user: User, key: impl Into<String>, value: f64) {
        let _ = self.track(user, key, Some(value), serde_json::Value::Null);
    }

    pub fn track(
        &self,
        user: User,
        key: impl Into<String>,
        metric_value: Option<f64>,
        data: impl Serialize,
    ) -> serde_json::Result<()> {
        let event = Event::new_custom(
            MaybeInlinedUser::new(self.config.inline_users_in_events, user),
            key,
            metric_value,
            data,
        )?;

        self.send_internal(event);
        Ok(())
    }

    fn evaluate_internal(
        &self,
        user: &User,
        flag_key: &str,
        default: FlagValue,
    ) -> (Option<FeatureFlag>, Detail<FlagValue>) {
        let store = self.store.lock().unwrap();
        let flag = match store.flag(flag_key) {
            // TODO eliminate this clone by wrangling lifetimes
            Some(flag) => flag.clone(),
            None => {
                return (
                    None,
                    Detail::err_default(eval::Error::FlagNotFound, default),
                )
            }
        };

        // TODO eliminate this clone by wrangling lifetimes
        let result = flag.evaluate(user, &store).map(|v| v.clone()).or(default);

        (Some(flag), result)
    }

    fn send_internal(&self, event: Event) {
        let _ = self
            .event_processor
            .send(event)
            .map_err(|e| warn!("failed to send event: {}", e));
    }
}

fn trim_base_url(mut url: &str) -> &str {
    while url.ends_with('/') {
        let untrimmed_url = url;
        url = &url[..url.len() - 1];
        debug!("trimming base url: {} -> {}", untrimmed_url, url);
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::Reason;
    use crate::event_sink::MockSink;
    use crate::store::{FeatureFlag, PatchTarget};
    use crate::update_processor::{MockUpdateProcessor, PatchData};

    use spectral::prelude::*;

    #[test]
    fn test_trim_base_url() {
        assert_eq!(trim_base_url("localhost"), "localhost");
        assert_eq!(trim_base_url("http://localhost"), "http://localhost");

        assert_eq!(trim_base_url("localhost/"), "localhost");
        assert_eq!(trim_base_url("http://localhost/"), "http://localhost");

        assert_eq!(trim_base_url("localhost////////"), "localhost");
    }

    #[test]
    // TODO split this test up: test update_processor and event_processor separately and just
    // mutate store to test evals
    fn client_receives_updates_evals_flags_and_sends_events() {
        let user = User::with_key("foo".to_string()).build();

        let (mut client, updates, _events) = make_mocked_client();

        let result = client.bool_variation_detail(&user, "someFlag", false);

        assert_that!(result.value).contains_value(false);

        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(FeatureFlag::basic_flag("myFlag")),
            })
            .expect("patch should apply");

        let result = client.bool_variation_detail(&user, "myFlag", false);

        assert_that!(result.value).contains_value(true);
        assert_that!(result.reason).is_equal_to(Reason::Fallthrough);
    }

    #[test]
    // TODO This test is copy-pasta
    fn client_receives_updates_evals_flags_and_sends_events_int() {
        let user = User::with_key("foo".to_string()).build();

        let (mut client, updates, _events) = make_mocked_client();

        let result = client.int_variation_detail(&user, "someFlag", 0);

        assert_that!(result.value).contains_value(0);

        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        let flag = FeatureFlag::basic_int_flag("myFlag");

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(flag),
            })
            .expect("patch should apply");

        let result = client.int_variation_detail(&user, "myFlag", 0);

        assert_that!(result.value).contains_value(std::i64::MAX);
        assert_that!(result.reason).is_equal_to(Reason::Fallthrough);
    }

    #[test]
    // TODO This test is copy-pasta
    fn client_receives_updates_evals_flags_and_sends_events_json() {
        use serde_json::Value;

        let user = User::with_key("foo".to_string()).build();

        let (mut client, updates, _events) = make_mocked_client();

        let result = client.json_variation_detail(&user, "someFlag", Value::Null);

        assert_that!(result.value).contains_value(Value::Null);

        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        let flag = FeatureFlag::basic_json_flag("myFlag");

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(flag),
            })
            .expect("patch should apply");

        let result = client.json_variation_detail(&user, "myFlag", Value::Null);

        let value = result.value.expect("value should not be None");
        assert!(value.is_object());
        assert_that!(result.reason).is_equal_to(Reason::Fallthrough);
    }

    #[test]
    fn identify_sends_identify_event() {
        let (mut client, _updates, events) = make_mocked_client();
        client.start_with_default_executor();

        let user = crate::users::User::with_key("bob").build();

        client.identify(user);
        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(1);
        assert_that!(events[0].kind()).is_equal_to("identify");
    }

    #[derive(Serialize)]
    struct MyCustomData {
        pub answer: u32,
    }

    #[test]
    fn track_sends_track_and_index_events() -> serde_json::Result<()> {
        let (mut client, _updates, events) = make_mocked_client();
        client.start_with_default_executor();

        let user = crate::users::User::with_key("bob").build();

        client.track_event(user.clone(), "event-with-null");
        client.track_data(user.clone(), "event-with-string", "string-data")?;
        client.track_data(user.clone(), "event-with-json", json!({"answer": 42}))?;
        client.track_data(
            user.clone(),
            "event-with-struct",
            MyCustomData { answer: 42 },
        )?;
        client.track_metric(user.clone(), "event-with-metric", 42.0);

        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(6);

        let mut events_by_type: HashMap<&str, usize> = HashMap::new();
        for event in &*events {
            if let Some(count) = events_by_type.get_mut(event.kind()) {
                *count += 1;
            } else {
                events_by_type.insert(event.kind(), 1);
            }
        }
        assert_that!(events_by_type.get("index")).contains_value(&1);
        assert_that!(events_by_type.get("custom")).contains_value(&5);

        Ok(())
    }

    fn make_mocked_client() -> (
        Client,
        Arc<Mutex<MockUpdateProcessor>>,
        Arc<RwLock<MockSink>>,
    ) {
        let updates = Arc::new(Mutex::new(MockUpdateProcessor::new()));
        let events = Arc::new(RwLock::new(MockSink::new()));

        let client = Client::configure()
            .update_processor(updates.clone())
            .event_sink(events.clone())
            .build("dummy_key")
            .expect("client should build");

        (client, updates, events)
    }
}
