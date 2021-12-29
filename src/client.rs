use futures::future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::{io, thread};

use launchdarkly_server_sdk_evaluation::{
    self as eval, Detail, FlagValue, PrerequisiteEvent, User,
};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::events::InputEvent;

use super::config::Config;
use super::data_source::DataSource;
use super::data_source_builders::BuildError as DataSourceError;
use super::data_store::DataStore;
use super::data_store_builders::BuildError as DataStoreError;
use super::evaluation::{FlagDetail, FlagDetailConfig};
use super::event_processor::EventProcessor;
use super::event_processor_builders::BuildError as EventProcessorError;
use super::events::EventFactory;

struct EventsScope {
    disabled: bool,
    event_factory: EventFactory,
    prerequisite_event_recorder: Box<dyn eval::PrerequisiteEventRecorder + Send + Sync>,
}

struct PrerequisiteEventRecorder {
    event_factory: EventFactory,
    event_processor: Arc<Mutex<dyn EventProcessor>>,
}

impl eval::PrerequisiteEventRecorder for PrerequisiteEventRecorder {
    fn record(&self, event: PrerequisiteEvent) {
        let evt = self.event_factory.new_eval_event(
            &event.prerequisite_flag.key,
            event.user.clone(),
            &event.prerequisite_flag,
            event.prerequisite_result,
            FlagValue::Json(serde_json::Value::Null),
            Some(event.target_flag_key),
        );

        match self.event_processor.lock() {
            Ok(ep) => ep.send(evt),
            Err(e) => error!(
                "Failed to record prerequisite event. Could not acquire event processor lock: {:?}",
                e
            ),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("invalid client config: {0}")]
    InvalidConfig(String),
}

impl From<DataSourceError> for BuildError {
    fn from(error: DataSourceError) -> Self {
        Self::InvalidConfig(error.to_string())
    }
}

impl From<DataStoreError> for BuildError {
    fn from(error: DataStoreError) -> Self {
        Self::InvalidConfig(error.to_string())
    }
}

impl From<EventProcessorError> for BuildError {
    fn from(error: EventProcessorError) -> Self {
        Self::InvalidConfig(error.to_string())
    }
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StartError {
    #[error("couldn't spawn background thread for client: {0}")]
    SpawnFailed(io::Error),
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum FlushError {
    #[error("failed to flush events: {0}")]
    FlushFailed(String),
}

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
/// # use launchdarkly_server_sdk::{Client, ConfigBuilder, BuildError};
/// # fn main() -> Result<(), BuildError> {
///     let ld_client = Client::build(ConfigBuilder::new("sdk-key").build())?;
/// #   Ok(())
/// # }
/// ```
///
/// Creating an instance which connects to a relay proxy.
/// ```
/// # use launchdarkly_server_sdk::{Client, ConfigBuilder, ServiceEndpointsBuilder, BuildError};
/// # fn main() -> Result<(), BuildError> {
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
    events_default: EventsScope,
    events_with_reasons: EventsScope,
    init_notify: Arc<Semaphore>,
    init_state: Arc<AtomicUsize>,
    started: AtomicBool,
    offline: bool,
    sdk_key: String,
    // TODO: Once we need the config for diagnostic events, then we should add this.
    // config: Arc<Mutex<Config>>
}

impl Client {
    pub fn build(config: Config) -> Result<Self, BuildError> {
        if config.offline() {
            info!("Started LaunchDarkly Client in offline mode");
        }

        let endpoints = config.service_endpoints_builder().build()?;
        let event_processor = config
            .event_processor_builder()
            .build(&endpoints, config.sdk_key())?;
        let data_source = config
            .data_source_builder()
            .build(&endpoints, config.sdk_key())?;
        let data_store = config.data_store_builder().build()?;

        let events_default = EventsScope {
            disabled: config.offline(),
            event_factory: EventFactory::new(false),
            prerequisite_event_recorder: Box::new(PrerequisiteEventRecorder {
                event_factory: EventFactory::new(false),
                event_processor: event_processor.clone(),
            }),
        };

        let events_with_reasons = EventsScope {
            disabled: config.offline(),
            event_factory: EventFactory::new(true),
            prerequisite_event_recorder: Box::new(PrerequisiteEventRecorder {
                event_factory: EventFactory::new(true),
                event_processor: event_processor.clone(),
            }),
        };

        Ok(Client {
            event_processor,
            data_source,
            data_store,
            events_default,
            events_with_reasons,
            init_notify: Arc::new(Semaphore::new(0)),
            init_state: Arc::new(AtomicUsize::new(ClientInitState::Initializing as usize)),
            started: AtomicBool::new(false),
            offline: config.offline(),
            sdk_key: config.sdk_key().into(),
        })
    }

    /// Starts a client in the current thread, which must have a default tokio runtime.
    pub fn start_with_default_executor(&self) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        self.started.store(true, Ordering::SeqCst);
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
    pub fn start_with_runtime(&self) -> Result<bool, StartError> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(true);
        }
        self.started.store(true, Ordering::SeqCst);

        let runtime = tokio::runtime::Runtime::new().map_err(StartError::SpawnFailed)?;
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
        if self.offline {
            return true;
        }

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
        self.offline || ClientInitState::Initialized == self.init_state.load(Ordering::SeqCst)
    }

    pub fn flush(&self) {
        match self.event_processor.lock() {
            Ok(ep) => ep.flush(),
            Err(e) => error!(
                "Unable to acquire event processor lock. Cannot flush. {:?}",
                e
            ),
        }
    }

    pub fn identify(&self, user: User) {
        if !self.events_default.disabled {
            self.send_internal(self.events_default.event_factory.new_identify(user));
        }
    }

    pub fn alias(&self, user: User, previous_user: User) {
        if !self.events_default.disabled {
            self.send_internal(
                self.events_default
                    .event_factory
                    .new_alias(user, previous_user),
            );
        }
    }

    pub fn bool_variation(&self, user: &User, flag_key: &str, default: bool) -> bool {
        let val = self.variation(user, flag_key, default);
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
        let val = self.variation(user, flag_key, default.clone());
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
        let val = self.variation(user, flag_key, default);
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
        let val = self.variation(user, flag_key, default);
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
        self.variation(user, flag_key, default.clone())
            .as_json()
            .unwrap_or(default)
    }

    pub fn bool_variation_detail(
        &self,
        user: &User,
        flag_key: &str,
        default: bool,
    ) -> Detail<bool> {
        self.variation_detail(user, flag_key, default)
            .try_map(|val| val.as_bool(), eval::Error::Exception)
    }

    pub fn str_variation_detail(
        &self,
        user: &User,
        flag_key: &str,
        default: String,
    ) -> Detail<String> {
        self.variation_detail(user, flag_key, default)
            .try_map(|val| val.as_string(), eval::Error::Exception)
    }

    pub fn float_variation_detail(&self, user: &User, flag_key: &str, default: f64) -> Detail<f64> {
        self.variation_detail(user, flag_key, default)
            .try_map(|val| val.as_float(), eval::Error::Exception)
    }

    pub fn int_variation_detail(&self, user: &User, flag_key: &str, default: i64) -> Detail<i64> {
        self.variation_detail(user, flag_key, default)
            .try_map(|val| val.as_int(), eval::Error::Exception)
    }

    pub fn json_variation_detail(
        &self,
        user: &User,
        flag_key: &str,
        default: serde_json::Value,
    ) -> Detail<serde_json::Value> {
        self.variation_detail(user, flag_key, default)
            .try_map(|val| val.as_json(), eval::Error::Exception)
    }

    /// secure_mode_hash generates the secure mode hash value for a user.
    ///
    /// For more information, see the Reference Guide: https://docs.launchdarkly.com/sdk/features/secure-mode#rust
    pub fn secure_mode_hash(&self, user: &User) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, self.sdk_key.as_bytes());
        let tag = ring::hmac::sign(&key, user.key().as_bytes());

        data_encoding::HEXLOWER.encode(tag.as_ref())
    }

    /// all_flags_detail returns an object that encapsulates the state of all feature flags for a given user.
    /// This includes the flag values, and also metadata that can be used on the front end.
    ///
    /// The most common use case for this method is to bootstrap a set of client-side feature flags from a
    /// back-end service.
    ///
    /// You may pass any configuration of [FlagDetailConfig] to control what data is included.
    ///
    /// For more information, see the Reference Guide: <https://docs.launchdarkly.com/sdk/features/all-flags#rust>
    pub fn all_flags_detail(&self, user: &User, flag_state_config: FlagDetailConfig) -> FlagDetail {
        if self.offline {
            warn!(
                "all_flags_detail() called, but client is in offline mode. Returning empty state"
            );
            return FlagDetail::new(false);
        }

        if !self.initialized() {
            warn!("all_flags_detail() called before client has finished initializing! Feature store unavailable - returning empty state");
            return FlagDetail::new(false);
        }

        let data_store = self.data_store.lock().unwrap();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&*data_store, user, flag_state_config);

        flag_detail
    }

    pub fn variation_detail<T: Into<FlagValue> + Clone>(
        &self,
        user: &User,
        flag_key: &str,
        default: T,
    ) -> Detail<FlagValue> {
        self.variation_internal(user, flag_key, default, &self.events_with_reasons)
    }

    pub fn variation<T: Into<FlagValue> + Clone>(
        &self,
        user: &User,
        flag_key: &str,
        default: T,
    ) -> FlagValue {
        // unwrap is safe here because value should have been replaced with default if it was None.
        // TODO(ch108604) that is ugly, use the type system to fix it
        self.variation_internal(user, flag_key, default, &self.events_default)
            .value
            .unwrap()
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
        if !self.events_default.disabled {
            let event =
                self.events_default
                    .event_factory
                    .new_custom(user, key, metric_value, data)?;

            self.send_internal(event);
        }

        Ok(())
    }

    fn variation_internal<T: Into<FlagValue> + Clone>(
        &self,
        user: &User,
        flag_key: &str,
        default: T,
        events_scope: &EventsScope,
    ) -> Detail<FlagValue> {
        if self.offline {
            return Detail::err_default(eval::Error::ClientNotReady, default.into());
        }

        let (flag, result) = match self.initialized() {
            false => (
                None,
                Detail::err_default(eval::Error::ClientNotReady, default.clone().into()),
            ),
            true => {
                let data_store = self.data_store.lock().unwrap();
                match data_store.flag(flag_key) {
                    Some(flag) => {
                        let result = eval::evaluate(
                            &*data_store.to_store(),
                            flag,
                            user,
                            Some(&*events_scope.prerequisite_event_recorder),
                        )
                        .map(|v| v.clone())
                        .or(default.clone().into());

                        (Some(flag.clone()), result)
                    }
                    None => (
                        None,
                        Detail::err_default(eval::Error::FlagNotFound, default.clone().into()),
                    ),
                }
            }
        };

        if !events_scope.disabled {
            let event = match &flag {
                Some(f) => self.events_default.event_factory.new_eval_event(
                    flag_key,
                    user.clone(),
                    f,
                    result.clone(),
                    default.into(),
                    None,
                ),
                None => self.events_default.event_factory.new_unknown_flag_event(
                    flag_key,
                    user.clone(),
                    result.clone(),
                    default.into(),
                ),
            };
            self.send_internal(event);
        }

        result
    }

    fn send_internal(&self, event: InputEvent) {
        match self.event_processor.lock() {
            Ok(ep) => ep.send(event),
            Err(e) => error!(
                "Unable to acquire event processor lock. Cannot send input event. {:?}",
                e
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ConfigBuilder;
    use launchdarkly_server_sdk_evaluation::{Reason, User};
    use spectral::prelude::*;
    use std::collections::HashMap;
    use std::sync::mpsc::Receiver;
    use std::sync::RwLock;
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::data_source::{MockDataSource, PatchData};
    use crate::data_source_builders::MockDataSourceBuilder;
    use crate::data_store::PatchTarget;
    use crate::event_processor_builders::EventProcessorBuilder;
    use crate::event_sink::MockSink;
    use crate::events::{OutputEvent, VariationKey};
    use crate::test_common::{
        self, basic_flag, basic_flag_with_prereq, basic_int_flag, basic_off_flag,
    };
    use test_case::test_case;

    use super::*;

    fn is_send_and_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_client_is_send_and_sync() {
        is_send_and_sync::<Client>()
    }

    #[tokio::test]
    async fn client_asynchronously_initializes() {
        let (client, _updates, _events, _sink_receiver) =
            make_mocked_client_with_delay(1000, false);
        client.start_with_default_executor();

        let now = Instant::now();
        let initialized = client.initialized_async().await;
        let elapsed_time = now.elapsed();
        assert!(initialized);
        // Give ourself a good margin for thread scheduling.
        assert!(elapsed_time.as_millis() > 500)
    }

    #[tokio::test]
    async fn client_initializes_immediately_in_offline_mode() {
        let (client, _updates, _events, _sink_receiver) = make_mocked_client_with_delay(1000, true);
        client.start_with_default_executor();

        assert!(client.initialized());

        let now = Instant::now();
        let initialized = client.initialized_async().await;
        let elapsed_time = now.elapsed();
        assert!(initialized);
        assert!(elapsed_time.as_millis() < 500)
    }

    #[test_case(basic_flag("myFlag"), false.into(), true.into())]
    #[test_case(basic_int_flag("myFlag"), 0.into(), test_common::FLOAT_TO_INT_MAX.into())]
    fn client_updates_changes_evaluation_results(
        flag: eval::Flag,
        default: FlagValue,
        expected: FlagValue,
    ) {
        let user = User::with_key("foo".to_string()).build();

        let (client, updates, _events, _sink_receiver) = make_mocked_client();

        let result = client.variation_detail(&user, "myFlag", default.clone());
        assert_that!(result.value).contains_value(default.clone());

        client.start_with_default_executor();

        let updates = updates.lock().unwrap();
        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(flag),
            })
            .expect("patch should apply");

        let result = client.variation_detail(&user, "myFlag", default);
        assert_that!(result.value).contains_value(expected);
        assert_that!(result.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
    }

    #[test]
    fn variation_tracks_events_correctly() {
        let (client, updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag("myFlag")),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let flag_value = client.variation(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&flag_value.as_bool().unwrap()).is_true();
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
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
    fn variation_handles_offline_mode() {
        let (client, _, events, sink_receiver) = make_mocked_offline_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();
        let flag_value = client.variation(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&flag_value.as_bool().unwrap()).is_false();
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(0);
    }

    #[test]
    fn variation_handles_unknown_flags() {
        let (client, _updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();
        let user = User::with_key("bob").build();

        let flag_value = client.variation(&user, "non-existent-flag", FlagValue::Bool(false));

        assert_that(&flag_value.as_bool().unwrap()).is_false();
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
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
    fn variation_detail_handles_debug_events_correctly() {
        let (client, updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();
        let mut flag = basic_flag("myFlag");
        flag.debug_events_until_date = Some(64_060_606_800_000); // Jan. 1st, 4000

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(flag),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let detail = client.variation_detail(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_true();
        assert_that(&detail.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(3);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("debug");
        assert_that!(events[2].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[2].clone() {
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
    fn variation_detail_tracks_events_correctly() {
        let (client, updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag("myFlag")),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let detail = client.variation_detail(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_true();
        assert_that(&detail.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
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
    fn variation_detail_handles_offline_mode() {
        let (client, _, events, sink_receiver) = make_mocked_offline_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();

        let detail = client.variation_detail(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_false();
        assert_that(&detail.reason).is_equal_to(Reason::Error {
            error: eval::Error::ClientNotReady,
        });
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(0);
    }

    #[test]
    fn variation_handles_off_flag_without_variation() {
        let (client, updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_off_flag("myFlag")),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let result = client.variation(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&result.as_bool().unwrap()).is_false();
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                flag_key: "myFlag".into(),
                version: Some(42),
                variation: None,
            };
            assert_that!(event_summary.features).contains_key(variation_key);
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn variation_detail_tracks_prereq_events_correctly() {
        let (client, updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        let mut basic_preqreq_flag = basic_flag("prereqFlag");
        basic_preqreq_flag.track_events = true;

        updates
            .patch(PatchData {
                path: "/flags/prereqFlag".to_string(),
                data: PatchTarget::Flag(basic_preqreq_flag),
            })
            .expect("patch should apply");

        let mut basic_flag = basic_flag_with_prereq("myFlag", "prereqFlag");
        basic_flag.track_events = true;
        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let detail = client.variation_detail(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_true();
        assert_that(&detail.reason).is_equal_to(Reason::Fallthrough {
            in_experiment: false,
        });
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(4);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("feature");
        assert_that!(events[2].kind()).is_equal_to("feature");
        assert_that!(events[3].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[3].clone() {
            let variation_key = VariationKey {
                flag_key: "myFlag".into(),
                version: Some(42),
                variation: Some(1),
            };
            assert_that!(event_summary.features).contains_key(variation_key);
            let variation_key = VariationKey {
                flag_key: "prereqFlag".into(),
                version: Some(42),
                variation: Some(1),
            };
            assert_that!(event_summary.features).contains_key(variation_key);
        }
    }

    #[test]
    fn variation_handles_failed_prereqs_correctly() {
        let (client, updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let updates = updates.lock().unwrap();

        let mut basic_preqreq_flag = basic_off_flag("prereqFlag");
        basic_preqreq_flag.track_events = true;

        updates
            .patch(PatchData {
                path: "/flags/prereqFlag".to_string(),
                data: PatchTarget::Flag(basic_preqreq_flag),
            })
            .expect("patch should apply");

        let mut basic_flag = basic_flag_with_prereq("myFlag", "prereqFlag");
        basic_flag.track_events = true;
        updates
            .patch(PatchData {
                path: "/flags/myFlag".to_string(),
                data: PatchTarget::Flag(basic_flag),
            })
            .expect("patch should apply");
        let user = User::with_key("bob").build();

        let detail = client.variation(&user, "myFlag", FlagValue::Bool(false));

        assert_that(&detail.as_bool().unwrap()).is_false();
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(4);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("feature");
        assert_that!(events[2].kind()).is_equal_to("feature");
        assert_that!(events[3].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[3].clone() {
            let variation_key = VariationKey {
                flag_key: "myFlag".into(),
                version: Some(42),
                variation: Some(0),
            };
            assert_that!(event_summary.features).contains_key(variation_key);
            let variation_key = VariationKey {
                flag_key: "prereqFlag".into(),
                version: Some(42),
                variation: None,
            };
            assert_that!(event_summary.features).contains_key(variation_key);
        }
    }

    #[test]
    fn variation_detail_handles_flag_not_found() {
        let (client, _updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();
        let detail = client.variation_detail(&user, "non-existent-flag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_false();
        assert_that(&detail.reason).is_equal_to(Reason::Error {
            error: eval::Error::FlagNotFound,
        });
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
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
    async fn variation_detail_handles_client_not_ready() {
        let (client, _updates, events, sink_receiver) =
            make_mocked_client_with_delay(u64::MAX, false);
        client.start_with_default_executor();
        let user = User::with_key("bob").build();

        let detail = client.variation_detail(&user, "non-existent-flag", FlagValue::Bool(false));

        assert_that(&detail.value.unwrap().as_bool().unwrap()).is_false();
        assert_that(&detail.reason).is_equal_to(Reason::Error {
            error: eval::Error::ClientNotReady,
        });
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(2);
        assert_that!(events[0].kind()).is_equal_to("index");
        assert_that!(events[1].kind()).is_equal_to("summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
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
        let (client, _updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();

        client.identify(user);
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(1);
        assert_that!(events[0].kind()).is_equal_to("identify");
    }

    #[test]
    fn identify_sends_sends_nothing_in_offline_mode() {
        let (client, _updates, events, sink_receiver) = make_mocked_offline_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();

        client.identify(user);
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(0);
    }

    #[test]
    fn secure_mode_hash() {
        let config = ConfigBuilder::new("secret").offline(true).build();
        let client = Client::build(config).expect("Should be built.");
        let user = User::with_key("Message").build();

        assert_eq!(
            client.secure_mode_hash(&user),
            "aa747c502a898200f9e4fa21bac68136f886a0e27aec70ba06daf2e2a5cb5597"
        );
    }

    #[test]
    fn alias_sends_alias_event() {
        let (client, _updates, events, sink_receiver) = make_mocked_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();
        let previous_user = User::with_key("previous-bob").build();

        client.alias(user, previous_user);
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(1);
        assert_that!(events[0].kind()).is_equal_to("alias");
    }

    #[test]
    fn alias_sends_nothing_in_offline_mode() {
        let (client, _updates, events, sink_receiver) = make_mocked_offline_client();
        client.start_with_default_executor();

        let user = User::with_key("bob").build();
        let previous_user = User::with_key("previous-bob").build();

        client.alias(user, previous_user);
        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(0);
    }

    #[derive(Serialize)]
    struct MyCustomData {
        pub answer: u32,
    }

    #[test]
    fn track_sends_track_and_index_events() -> serde_json::Result<()> {
        let (client, _updates, events, sink_receiver) = make_mocked_client();
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

        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
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

    #[test]
    fn track_sends_nothing_in_offline_mode() -> serde_json::Result<()> {
        let (client, _updates, events, sink_receiver) = make_mocked_offline_client();
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

        client.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(0);

        Ok(())
    }

    fn make_mocked_client_with_delay(
        delay: u64,
        offline: bool,
    ) -> (
        Client,
        Arc<Mutex<MockDataSource>>,
        Arc<RwLock<MockSink>>,
        Receiver<()>,
    ) {
        let updates = Arc::new(Mutex::new(MockDataSource::new_with_init_delay(delay)));
        let (sink, sink_receiver) = MockSink::new(0);
        let events = Arc::new(RwLock::new(sink));

        let config = ConfigBuilder::new("sdk-key")
            .offline(offline)
            .data_source(MockDataSourceBuilder::new().data_source(updates.clone()))
            .event_processor(EventProcessorBuilder::new().event_sink(events.clone()))
            .build();

        let client = Client::build(config).expect("Should be built.");

        (client, updates, events, sink_receiver)
    }

    fn make_mocked_offline_client() -> (
        Client,
        Arc<Mutex<MockDataSource>>,
        Arc<RwLock<MockSink>>,
        Receiver<()>,
    ) {
        make_mocked_client_with_delay(0, true)
    }

    fn make_mocked_client() -> (
        Client,
        Arc<Mutex<MockDataSource>>,
        Arc<RwLock<MockSink>>,
        Receiver<()>,
    ) {
        make_mocked_client_with_delay(0, false)
    }
}
