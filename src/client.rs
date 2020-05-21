use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use super::eval::{self, Detail};
use super::event_processor::EventProcessor;
use super::event_sink::EventSink;
use super::events::{Event, MaybeInlinedUser};
use super::store::{FeatureFlag, FeatureStore, FlagValue};
use super::update_processor::{StreamingUpdateProcessor, UpdateProcessor};
use super::users::User;

const DEFAULT_STREAM_BASE_URL: &str = "https://stream.launchdarkly.com";
const DEFAULT_EVENTS_BASE_URL: &str = "https://events.launchdarkly.com";

#[derive(Debug)]
pub enum Error {
    FlagWrongType(String, String),
    InvalidConfig(Box<dyn std::fmt::Debug>),
    EvaluationError(eval::Error),
    NoSuchFlag(String),
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

    pub fn build(&self, sdk_key: &str) -> Result<Client> {
        let store = FeatureStore::new();
        let event_processor = match &self.event_sink {
            None => reqwest::Url::parse(&self.events_base_url)
                .map_err(|e| Error::InvalidConfig(Box::new(e)))
                .map(|base_url| EventProcessor::new(base_url, sdk_key))?,
            Some(sink) => {
                // clone sink Arc so we don't have to consume builder
                EventProcessor::new_with_sink(sink.clone())
            }
        };
        let update_processor = match &self.update_processor {
            None => Arc::new(Mutex::new(
                StreamingUpdateProcessor::new(&self.stream_base_url, sdk_key)
                    .map_err(|e| Error::InvalidConfig(Box::new(e)))?,
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

    pub fn start(&mut self) {
        self.update_processor
            .lock()
            .unwrap()
            .subscribe(self.store.clone())
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
        default: &str,
    ) -> Detail<String> {
        self.evaluate_detail(user, flag_key, default.to_string().into())
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

    pub fn all_flags_detail(&self, user: &User) -> HashMap<String, Detail<FlagValue>> {
        let store = self.store.lock().unwrap();
        let flags = store.all_flags();
        let evals = flags.iter().map(|(key, flag)| {
            // TODO don't send events
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
        self.event_processor.send(event);

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
        self.event_processor.send(event);

        // unwrap is safe here because value should have been replaced with default if it was None.
        // TODO that is ugly, use the type system to fix it
        result.value.unwrap()
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
    use crate::eval::{self, Reason};
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

        let (mut client, updates, events) = make_mocked_client();

        let result = client.bool_variation_detail(&user, "someFlag", false);

        assert_that!(result.value).contains_value(false);

        client.start();

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

        let events = events.read().unwrap();
        assert_that!(*events).matching_contains(|event| event.kind() == "feature"); // TODO test this is absent unless trackEvents = true or various other conditions
        assert_that!(*events).matching_contains(|event| event.kind() == "summary");
        assert_that!(*events).matching_contains(|event| event.kind() == "index");
    }

    #[test]
    fn user_with_no_key_still_sends_event() {
        let user = crate::users::UserBuilder::new_with_optional_key(None).build();

        let (mut client, updates, events) = make_mocked_client();
        client.start();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(FeatureFlag::basic_flag("myFlag")),
            })
            .expect("patch should apply");

        let result = client.bool_variation_detail(&user, "myFlag", false);

        assert_that!(result.value).contains_value(false);
        assert_that!(result.reason).is_equal_to(Reason::Error {
            error: eval::Error::UserNotSpecified,
        });

        let events = events.read().unwrap();
        assert_that!(*events).matching_contains(|event| event.kind() == "feature"); // TODO test this is absent unless trackEvents = true or various other conditions
        assert_that!(*events).matching_contains(|event| event.kind() == "summary")
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
