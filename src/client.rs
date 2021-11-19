use futures::future;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{io, thread};

use rust_server_sdk_evaluation::{self as eval, Detail, Flag, FlagValue, User};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Semaphore;

use super::config::Config;
use super::data_source::DataSource;
use super::data_store::DataStore;
use super::event_processor::EventProcessor;
use super::events::{Event, MaybeInlinedUser};

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

#[derive(PartialEq, Copy, Clone, Debug)]
enum ClientInitState {
    Initializing = 0,
    Initialized = 1,
    InitializationFailed = 2,
}

impl PartialEq<usize> for ClientInitState {
    fn eq(&self, other: &usize) -> bool {
        *self as usize == *other
    }
}

impl From<usize> for ClientInitState {
    fn from(val: usize) -> Self {
        match val {
            0 => ClientInitState::Initializing,
            1 => ClientInitState::Initialized,
            2 => ClientInitState::InitializationFailed,
            _ => unreachable!(),
        }
    }
}

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
    data_source: Arc<Mutex<dyn DataSource>>,
    data_store: Arc<Mutex<dyn DataStore>>,
    inline_users_in_events: bool,
    init_notify: Arc<Semaphore>,
    init_state: Arc<AtomicUsize>,
    started: Cell<bool>,
    // TODO: Once we need the config for diagnostic events, then we should add this.
    // config: Arc<Mutex<Config>>
}

impl Client {
    pub fn build(config: Config) -> Result<Self> {
        let endpoints = config.service_endpoints_builder().build()?;
        let event_processor = config
            .event_processor_builder()
            .build(&endpoints, config.sdk_key())?;
        let data_source = config
            .data_source_builder()
            .build(&endpoints, config.sdk_key())?;
        let data_store = config.data_store_builder().build()?;

        Ok(Client {
            event_processor,
            data_source,
            data_store,
            inline_users_in_events: config.inline_users_in_events(),
            init_notify: Arc::new(Semaphore::new(0)),
            init_state: Arc::new(AtomicUsize::new(ClientInitState::Initializing as usize)),
            started: Cell::new(false),
        })
    }

    /// Starts a client in the current thread, which must have a default tokio runtime.
    pub fn start_with_default_executor(&self) {
        if self.started.get() {
            return;
        }
        self.started.replace(true);
        self.start_with_default_executor_internal();
    }

    fn start_with_default_executor_internal(&self) {
        // These clones are going to move into the closure, we
        // do not want to move or reference `self`, because
        // then lifetimes will get involved.
        let notify = self.init_notify.clone();
        let init_state = self.init_state.clone();

        self.data_source.lock().unwrap().subscribe(
            self.data_store.clone(),
            Arc::new(move |success| {
                init_state.store(
                    (if success {
                        ClientInitState::Initialized
                    } else {
                        ClientInitState::InitializationFailed
                    }) as usize,
                    Ordering::SeqCst,
                );
                notify.add_permits(1);
            }),
        );
    }

    /// Creates a new tokio runtime and then starts the client. Tasks from the client will
    /// be executed on created runtime.
    /// If your application already has a tokio runtime, then you can use
    /// [crate::Client::start_with_default_executor] and the client will dispatch tasks to
    /// your existing runtime.
    pub fn start_with_runtime(&self) -> Result<bool> {
        if self.started.get() {
            return Ok(true);
        }
        self.started.replace(true);

        let runtime = tokio::runtime::Runtime::new().map_err(Error::SpawnFailed)?;
        let _guard = runtime.enter();

        self.start_with_default_executor_internal();

        thread::spawn(move || {
            // this thread takes ownership of runtime and prevents it from being dropped before the
            // client initialises
            runtime.block_on(future::pending::<()>())
        });

        Ok(true)
    }

    /// This is an async method that will resolve once initialization is complete.
    /// Initialization being complete does not mean that initialization was a success.
    /// The return value from the method indicates if the client successfully initialized.
    pub async fn initialized_async(&self) -> bool {
        // If the client is not initialized, then we need to wait for it to be initialized.
        // Because we are using atomic types, and not a lock, then there is still the possibility
        // that the value will change between the read and when we wait. We use a semaphore to wait,
        // and we do not forget the permit, therefore if the permit has been added, then we will get
        // it very quickly and reduce blocking.
        if ClientInitState::Initialized != self.init_state.load(Ordering::SeqCst) {
            let _permit = self.init_notify.acquire().await;
        }
        ClientInitState::Initialized == self.init_state.load(Ordering::SeqCst)
    }

    /// This function synchronously returns if the SDK is initialized.
    /// In the case of unrecoverable errors in establishing a connection it is possible for the
    /// SDK to never become initialized.
    pub fn initialized(&self) -> bool {
        ClientInitState::Initialized == self.init_state.load(Ordering::SeqCst)
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
        let data_store = self.data_store.lock().unwrap();
        let flags = data_store.all_flags();
        let evals = flags.iter().map(|(key, flag)| {
            let val = flag
                .evaluate(user, &*data_store.to_store())
                .map(|v| v.clone());
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
        if !self.initialized() {
            return (
                None,
                Detail::err_default(eval::Error::ClientNotReady, default),
            );
        }

        let data_store = self.data_store.lock().unwrap();

        let flag = if let Some(flag) = data_store.flag(flag_key) {
            flag
        } else {
            return (
                None,
                Detail::err_default(eval::Error::FlagNotFound, default),
            );
        };

        let result = flag
            .evaluate(user, &*data_store.to_store())
            .map(|v| v.clone())
            .or(default);

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
    use std::sync::RwLock;
    use tokio::time::Instant;

    use crate::data_source::{MockDataSource, PatchData};
    use crate::data_source_builders::MockDataSourceBuilder;
    use crate::data_store::PatchTarget;
    use crate::event_processor_builders::EventProcessorBuilder;
    use crate::event_sink::MockSink;
    use crate::events::VariationKey;
    use crate::test_common::{self, basic_flag, basic_int_flag, basic_json_flag};

    use super::*;

    #[tokio::test]
    async fn client_asynchronously_initializes() {
        let (client, _updates, _events) = make_mocked_client_with_delay(1000);
        client.start_with_default_executor();

        let now = Instant::now();
        let initialized = client.initialized_async().await;
        let elapsed_time = now.elapsed();
        assert!(initialized);
        // Give ourself a good margin for thread scheduling.
        assert!(elapsed_time.as_millis() > 500)
    }

    #[test]
    // TODO(ch107017) split this test up: test data_source and event_processor separately and
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
    fn evaluate_detail_handles_flag_not_found() {
        let (client, updates, events) = make_mocked_client();
        client.start_with_default_executor();
        updates
            .lock()
            .unwrap()
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag("myFlag")),
            })
            .expect("patch should apply");

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

    #[tokio::test]
    async fn evaluate_detail_handles_client_not_ready() {
        let (client, _updates, events) = make_mocked_client_with_delay(u64::MAX);
        client.start_with_default_executor();
        let user = User::with_key("bob").build();

        let detail = client.evaluate_detail(&user, "non-existent-flag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_false();
        assert_that(&detail.reason).is_equal_to(Reason::Error {
            error: eval::Error::ClientNotReady,
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

    fn make_mocked_client_with_delay(
        delay: u64,
    ) -> (Client, Arc<Mutex<MockDataSource>>, Arc<RwLock<MockSink>>) {
        let updates = Arc::new(Mutex::new(MockDataSource::new_with_init_delay(delay)));
        let events = Arc::new(RwLock::new(MockSink::new()));

        let config = ConfigBuilder::new("sdk-key")
            .data_source(MockDataSourceBuilder::new().data_source(updates.clone()))
            .event_processor(EventProcessorBuilder::new().event_sink(events.clone()))
            .build();

        let client = Client::build(config).expect("Should be built.");

        (client, updates, events)
    }

    fn make_mocked_client() -> (Client, Arc<Mutex<MockDataSource>>, Arc<RwLock<MockSink>>) {
        make_mocked_client_with_delay(0)
    }
}
