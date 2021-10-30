use futures::future;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::{io, thread};

use rust_server_sdk_evaluation::{self as eval, Detail, Flag, FlagValue, Store, User};
use serde::Serialize;
use thiserror::Error;

use super::config::Config;
use super::event_processor::EventProcessor;
use super::events::{Event, MaybeInlinedUser};
use super::store::FeatureStore;
use super::update_processor::UpdateProcessor;

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

/// A client for the LaunchDarkly API.
///
/// In order to create a client instance you must first create a [crate::Config].
///
/// # Examples
///
/// Creating a client, with default configuration.
/// ```
/// # use launchdarkly_server_sdk::{Client, ConfigBuilder, ClientError};
/// # fn main() -> Result<(), ClientError> {
///     let ld_client = Client::build(ConfigBuilder::new("sdk-key").build())?;
/// #   Ok(())
/// # }
/// ```
///
/// Creating an instance which connects to a relay proxy.
/// ```
/// # use launchdarkly_server_sdk::{Client, ConfigBuilder, ServiceEndpointsBuilder, ClientError};
/// # fn main() -> Result<(), ClientError> {
///     let ld_client = Client::build(ConfigBuilder::new("sdk-key")
///         .service_endpoints(ServiceEndpointsBuilder::new()
///             .relay_proxy("http://my-relay-hostname:8080")
///         ).build()
///     )?;
/// #   Ok(())
/// # }
/// ```
///
/// Each builder type includes usage examples for the builder.
pub struct Client {
    event_processor: Arc<Mutex<dyn EventProcessor>>,
    update_processor: Arc<Mutex<dyn UpdateProcessor>>,
    store: Arc<Mutex<FeatureStore>>,
    inline_users_in_events: bool,
    // TODO: Once we need the config for diagnostic events, then we should add this.
    // config: Arc<Mutex<Config>>,
}

impl Client {
    pub fn build(config: Config) -> Result<Self> {
        let store = FeatureStore::new();
        let endpoints = config.service_endpoints_builder().build()?;
        let update_processor = config
            .data_source_builder()
            .build(&endpoints, config.sdk_key())?;
        let event_processor = config
            .event_processor_builder()
            .build(&endpoints, config.sdk_key())?;

        Ok(Client {
            event_processor,
            update_processor,
            inline_users_in_events: config.inline_users_in_events(),
            store: Arc::new(Mutex::new(store)),
        })
    }

    /// Starts a client in the current thread, which must have a default tokio runtime.
    pub fn start_with_default_executor(&self) {
        self.update_processor
            .lock()
            .unwrap()
            .subscribe(self.store.clone());
    }

    /// Starts a client in a background thread, with its own tokio runtime.
    // TODO: Ryan. I am not sure where we should put this. It seems like configuring an
    // execution environment is different than just the client instance.
    pub fn start(config: Config) -> Result<Arc<RwLock<Client>>> {
        let client = Self::build(config)?;
        let rw = Arc::new(RwLock::new(client));

        let runtime = tokio::runtime::Runtime::new().map_err(Error::SpawnFailed)?;
        let _guard = runtime.enter();

        // Important that we take the write lock before returning the RwLock to the caller:
        // otherwise caller can grab the read lock first and prevent the client from initialising
        let w = rw.clone();
        let client = w.write().unwrap();
        client.start_with_default_executor();

        thread::spawn(move || {
            // this thread takes ownership of runtime and prevents it from being dropped before the
            // client initialises
            runtime.block_on(future::pending::<()>())
        });

        Ok(rw)
    }

    pub fn flush(&self) -> Result<()> {
        self.event_processor
            .lock()
            .unwrap()
            .flush()
            .map_err(Error::FlushFailed)
    }

    pub fn identify(&self, user: User) {
        let event = Event::new_identify(user);

        self.send_internal(event);
    }

    pub fn alias(&self, user: User, previous_user: User) {
        let event = Event::new_alias(user, previous_user);

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
            let val = flag.evaluate(user, &*store).map(|v| v.clone());
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

        let event = match flag {
            Some(f) => Event::new_eval_event(
                flag_key,
                MaybeInlinedUser::new(self.inline_users_in_events, user.clone()),
                &f,
                result.clone(),
                default_for_event,
                true,
            ),
            None => Event::new_unknown_flag_event(
                flag_key,
                MaybeInlinedUser::new(self.inline_users_in_events, user.clone()),
                result.clone(),
                default_for_event,
                true,
            ),
        };

        self.send_internal(event);

        result
    }

    pub fn evaluate(&self, user: &User, flag_key: &str, default: FlagValue) -> FlagValue {
        let default_for_event = default.clone();

        let (flag, result) = self.evaluate_internal(user, flag_key, default);

        let event = match flag {
            Some(f) => Event::new_eval_event(
                flag_key,
                MaybeInlinedUser::new(self.inline_users_in_events, user.clone()),
                &f,
                result.clone(),
                default_for_event,
                false,
            ),
            None => Event::new_unknown_flag_event(
                flag_key,
                MaybeInlinedUser::new(self.inline_users_in_events, user.clone()),
                result.clone(),
                default_for_event,
                false,
            ),
        };
        self.send_internal(event);

        // unwrap is safe here because value should have been replaced with default if it was None.
        // TODO(ch108604) that is ugly, use the type system to fix it
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
            MaybeInlinedUser::new(self.inline_users_in_events, user),
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
    ) -> (Option<Flag>, Detail<FlagValue>) {
        let store = self.store.lock().unwrap();
        let flag = if let Some(flag) = store.flag(flag_key) {
            flag
        } else {
            return (
                None,
                Detail::err_default(eval::Error::FlagNotFound, default),
            );
        };

        let result = flag.evaluate(user, &*store).map(|v| v.clone()).or(default);

        (Some(flag.clone()), result)
    }

    fn send_internal(&self, event: Event) {
        let _ = self
            .event_processor
            .lock()
            .unwrap()
            .send(event)
            .map_err(|e| warn!("failed to send event: {}", e));
    }
}

#[cfg(test)]
mod tests {
    use crate::ConfigBuilder;
    use rust_server_sdk_evaluation::{Reason, User};
    use spectral::prelude::*;

    use crate::data_source_builders::MockDataSourceBuilder;
    use crate::event_processor_builders::EventProcessorBuilder;
    use crate::event_sink::MockSink;
    use crate::events::VariationKey;
    use crate::store::PatchTarget;
    use crate::test_common::{self, basic_flag, basic_int_flag, basic_json_flag};
    use crate::update_processor::{MockUpdateProcessor, PatchData};

    use super::*;

    #[test]
    // TODO(ch107017) split this test up: test update_processor and event_processor separately and
    // just mutate store to test evals
    fn client_receives_updates_evals_flags_and_sends_events() {
        let user = User::with_key("foo".to_string()).build();

        let (client, updates, _events) = make_mocked_client();

        let result = client.bool_variation_detail(&user, "someFlag", false);

        assert_that!(result.value).contains_value(false);

        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag("myFlag")),
            })
            .expect("patch should apply");

        let result = client.bool_variation_detail(&user, "myFlag", false);

        assert_that!(result.value).contains_value(true);
        assert_that!(result.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
    }

    #[test]
    // TODO(ch107017) This test is copy-pasta
    fn client_receives_updates_evals_flags_and_sends_events_int() {
        let user = User::with_key("foo".to_string()).build();

        let (client, updates, _events) = make_mocked_client();

        let result = client.int_variation_detail(&user, "someFlag", 0);

        assert_that!(result.value).contains_value(0);

        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        let flag = basic_int_flag("myFlag");

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(flag),
            })
            .expect("patch should apply");

        let result = client.int_variation_detail(&user, "myFlag", 0);

        assert_that!(result.value).contains_value(test_common::FLOAT_TO_INT_MAX);
        assert_that!(result.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
    }

    #[test]
    // TODO(ch107017) This test is copy-pasta
    fn client_receives_updates_evals_flags_and_sends_events_json() {
        use serde_json::Value;

        let user = User::with_key("foo".to_string()).build();

        let (client, updates, _events) = make_mocked_client();

        let result = client.json_variation_detail(&user, "someFlag", Value::Null);

        assert_that!(result.value).contains_value(Value::Null);

        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        let flag = basic_json_flag("myFlag");

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(flag),
            })
            .expect("patch should apply");

        let result = client.json_variation_detail(&user, "myFlag", Value::Null);

        let value = result.value.expect("value should not be None");
        assert!(value.is_object());
        assert_that!(result.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
    }

    #[test]
    fn evaluate_tracks_events_correctly() {
        let (client, updates, events) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag("myFlag")),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let flag_value = client.evaluate(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&flag_value.as_bool().unwrap()).is_true();
        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let Event::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                flag_key: "myFlag".into(),
                version: Some(42),
                variation: Some(1),
            };
            assert_that!(event_summary.features).contains_key(variation_key);
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn evaluate_handles_unknown_flags() {
        let (client, _updates, events) = make_mocked_client();
        client.start_with_default_executor();
        let user = User::with_key("bob").build();

        let flag_value = client.evaluate(&user, "non-existent-flag", FlagValue::Bool(false));

        assert_that(&flag_value.as_bool().unwrap()).is_false();
        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let Event::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                flag_key: "non-existent-flag".into(),
                version: None,
                variation: None,
            };
            assert_that!(event_summary.features).contains_key(variation_key);
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn evaluate_detail_tracks_events_correctly() {
        let (client, updates, events) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag("myFlag")),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let detail = client.evaluate_detail(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_true();
        assert_that(&detail.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let Event::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                flag_key: "myFlag".into(),
                version: Some(42),
                variation: Some(1),
            };
            assert_that!(event_summary.features).contains_key(variation_key);
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn evaluate_detail_handles_unknown_flags() {
        let (client, _updates, events) = make_mocked_client();
        client.start_with_default_executor();
        let user = User::with_key("bob").build();

        let detail = client.evaluate_detail(&user, "non-existent-flag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_false();
        assert_that(&detail.reason).is_equal_to(Reason::Error {
            error: eval::Error::FlagNotFound,
        });
        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let Event::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                flag_key: "non-existent-flag".into(),
                version: None,
                variation: None,
            };
            assert_that!(event_summary.features).contains_key(variation_key);
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn identify_sends_identify_event() {
        let (client, _updates, events) = make_mocked_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();

        client.identify(user);
        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(1);
        assert_that!(events[0].kind()).is_equal_to("identify");
    }

    #[test]
    fn alias_sends_alias_event() {
        let (client, _updates, events) = make_mocked_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();
        let previous_user = User::with_key("previous-bob").build();

        client.alias(user, previous_user);
        client.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(1);
        assert_that!(events[0].kind()).is_equal_to("alias");
    }

    #[derive(Serialize)]
    struct MyCustomData {
        pub answer: u32,
    }

    #[test]
    fn track_sends_track_and_index_events() -> serde_json::Result<()> {
        let (client, _updates, events) = make_mocked_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();

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

        let config = ConfigBuilder::new("sdk-key")
            .data_source(MockDataSourceBuilder::new().data_source(updates.clone()))
            .event_processor(EventProcessorBuilder::new().event_sink(events.clone()))
            .build();

        let client = Client::build(config).expect("Should be built.");

        (client, updates, events)
    }
}
