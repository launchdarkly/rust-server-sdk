use eval::Context;
use parking_lot::RwLock;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;

use launchdarkly_server_sdk_evaluation::{self as eval, Detail, FlagValue, PrerequisiteEvent};
use serde::Serialize;
use thiserror::Error;
use tokio::sync::{broadcast, Semaphore};

use super::config::Config;
use super::data_source::DataSource;
use super::data_source_builders::BuildError as DataSourceError;
use super::evaluation::{FlagDetail, FlagDetailConfig};
use super::stores::store::DataStore;
use super::stores::store_builders::BuildError as DataStoreError;
use crate::config::BuildError as ConfigBuildError;
use crate::events::event::EventFactory;
use crate::events::event::InputEvent;
use crate::events::processor::EventProcessor;
use crate::events::processor_builders::BuildError as EventProcessorError;
use crate::{MigrationOpTracker, Stage};

struct EventsScope {
    disabled: bool,
    event_factory: EventFactory,
    prerequisite_event_recorder: Box<dyn eval::PrerequisiteEventRecorder + Send + Sync>,
}

struct PrerequisiteEventRecorder {
    event_factory: EventFactory,
    event_processor: Arc<dyn EventProcessor>,
}

impl eval::PrerequisiteEventRecorder for PrerequisiteEventRecorder {
    fn record(&self, event: PrerequisiteEvent) {
        let evt = self.event_factory.new_eval_event(
            &event.prerequisite_flag.key,
            event.context.clone(),
            &event.prerequisite_flag,
            event.prerequisite_result,
            FlagValue::Json(serde_json::Value::Null),
            Some(event.target_flag_key),
        );

        self.event_processor.send(evt);
    }
}

/// Error type used to represent failures when building a [Client] instance.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    /// Error used when a configuration setting is invalid. This typically indicates an invalid URL.
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

impl From<ConfigBuildError> for BuildError {
    fn from(error: ConfigBuildError) -> Self {
        Self::InvalidConfig(error.to_string())
    }
}

/// Error type used to represent failures when starting the [Client].
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StartError {
    /// Error used when spawning a background there fails.
    #[error("couldn't spawn background thread for client: {0}")]
    SpawnFailed(io::Error),
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
/// In order to create a client instance, first create a config using [crate::ConfigBuilder].
///
/// # Examples
///
/// Creating a client, with default configuration.
/// ```
/// # use launchdarkly_server_sdk::{Client, ConfigBuilder, BuildError};
/// # fn main() -> Result<(), BuildError> {
///     let ld_client = Client::build(ConfigBuilder::new("sdk-key").build()?)?;
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
///         ).build()?
///     )?;
/// #   Ok(())
/// # }
/// ```
///
/// Each builder type includes usage examples for the builder.
pub struct Client {
    event_processor: Arc<dyn EventProcessor>,
    data_source: Arc<dyn DataSource>,
    data_store: Arc<RwLock<dyn DataStore>>,
    events_default: EventsScope,
    events_with_reasons: EventsScope,
    init_notify: Arc<Semaphore>,
    init_state: Arc<AtomicUsize>,
    started: AtomicBool,
    offline: bool,
    sdk_key: String,
    shutdown_broadcast: broadcast::Sender<()>,
    runtime: RwLock<Option<Runtime>>,
}

impl Client {
    /// Create a new instance of a [Client] based on the provided [Config] parameter.
    pub fn build(config: Config) -> Result<Self, BuildError> {
        if config.offline() {
            info!("Started LaunchDarkly Client in offline mode");
        }

        let tags = config.application_tag();

        let endpoints = config.service_endpoints_builder().build()?;
        let event_processor =
            config
                .event_processor_builder()
                .build(&endpoints, config.sdk_key(), tags.clone())?;
        let data_source =
            config
                .data_source_builder()
                .build(&endpoints, config.sdk_key(), tags.clone())?;
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

        let (shutdown_tx, _) = broadcast::channel(1);

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
            shutdown_broadcast: shutdown_tx,
            runtime: RwLock::new(None),
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

        self.data_source.subscribe(
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
            self.shutdown_broadcast.subscribe(),
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

        let runtime = Runtime::new().map_err(StartError::SpawnFailed)?;
        let _guard = runtime.enter();
        self.runtime.write().replace(runtime);

        self.start_with_default_executor_internal();

        Ok(true)
    }

    /// This is an async method that will resolve once initialization is complete.
    /// Initialization being complete does not mean that initialization was a success.
    /// The return value from the method indicates if the client successfully initialized.
    #[deprecated(
        note = "blocking without a timeout is discouraged, use wait_for_initialization instead"
    )]
    pub async fn initialized_async(&self) -> bool {
        self.initialized_async_internal().await
    }

    /// This is an async method that will resolve once initialization is complete or the specified
    /// timeout has occurred.
    ///
    /// If the timeout is triggered, this method will return `None`. Otherwise, the method will
    /// return a boolean indicating whether or not the SDK has successfully initialized.
    pub async fn wait_for_initialization(&self, timeout: Duration) -> Option<bool> {
        if timeout > Duration::from_secs(60) {
            warn!("wait_for_initialization was configured to block for up to {} seconds. We recommend blocking no longer than 60 seconds.", timeout.as_secs());
        }

        let initialized = tokio::time::timeout(timeout, self.initialized_async_internal()).await;
        match initialized {
            Ok(result) => Some(result),
            Err(_) => None,
        }
    }

    async fn initialized_async_internal(&self) -> bool {
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

    /// Close shuts down the LaunchDarkly client. After calling this, the LaunchDarkly client
    /// should no longer be used. The method will block until all pending analytics events (if any)
    /// been sent.
    pub fn close(&self) {
        self.event_processor.close();

        // If the system is in offline mode, no receiver will be listening to this broadcast
        // channel, so sending on it would always result in an error.
        if !self.offline {
            if let Err(e) = self.shutdown_broadcast.send(()) {
                error!("Failed to shutdown client appropriately: {}", e);
            }
        }

        // Potentially take the runtime we created when starting the client and do nothing with it
        // so it drops, closing out all spawned tasks.
        self.runtime.write().take();
    }

    /// Flush tells the client that all pending analytics events (if any) should be delivered as
    /// soon as possible. Flushing is asynchronous, so this method will return before it is
    /// complete. However, if you call [Client::close], events are guaranteed to be sent before
    /// that method returns.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/flush#rust>.
    pub fn flush(&self) {
        self.event_processor.flush();
    }

    /// Identify reports details about a context.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/identify#rust>
    pub fn identify(&self, context: Context) {
        if self.events_default.disabled {
            return;
        }

        self.send_internal(self.events_default.event_factory.new_identify(context));
    }

    /// Returns the value of a boolean feature flag for a given context.
    ///
    /// Returns `default` if there is an error, if the flag doesn't exist, or the feature is turned
    /// off and has no off variation.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluating#rust>.
    pub fn bool_variation(&self, context: &Context, flag_key: &str, default: bool) -> bool {
        let val = self.variation(context, flag_key, default);
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

    /// Returns the value of a string feature flag for a given context.
    ///
    /// Returns `default` if there is an error, if the flag doesn't exist, or the feature is turned
    /// off and has no off variation.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluating#rust>.
    pub fn str_variation(&self, context: &Context, flag_key: &str, default: String) -> String {
        let val = self.variation(context, flag_key, default.clone());
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

    /// Returns the value of a float feature flag for a given context.
    ///
    /// Returns `default` if there is an error, if the flag doesn't exist, or the feature is turned
    /// off and has no off variation.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluating#rust>.
    pub fn float_variation(&self, context: &Context, flag_key: &str, default: f64) -> f64 {
        let val = self.variation(context, flag_key, default);
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

    /// Returns the value of a integer feature flag for a given context.
    ///
    /// Returns `default` if there is an error, if the flag doesn't exist, or the feature is turned
    /// off and has no off variation.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluating#rust>.
    pub fn int_variation(&self, context: &Context, flag_key: &str, default: i64) -> i64 {
        let val = self.variation(context, flag_key, default);
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

    /// Returns the value of a feature flag for the given context, allowing the value to be
    /// of any JSON type.
    ///
    /// The value is returned as an [serde_json::Value].
    ///
    /// Returns `default` if there is an error, if the flag doesn't exist, or the feature is turned off.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluating#rust>.
    pub fn json_variation(
        &self,
        context: &Context,
        flag_key: &str,
        default: serde_json::Value,
    ) -> serde_json::Value {
        self.variation(context, flag_key, default.clone())
            .as_json()
            .unwrap_or(default)
    }

    /// This method is the same as [Client::bool_variation], but also returns further information
    /// about how the value was calculated. The "reason" data will also be included in analytics
    /// events.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluation-reasons#rust>.
    pub fn bool_variation_detail(
        &self,
        context: &Context,
        flag_key: &str,
        default: bool,
    ) -> Detail<bool> {
        self.variation_detail(context, flag_key, default).try_map(
            |val| val.as_bool(),
            default,
            eval::Error::WrongType,
        )
    }

    /// This method is the same as [Client::str_variation], but also returns further information
    /// about how the value was calculated. The "reason" data will also be included in analytics
    /// events.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluation-reasons#rust>.
    pub fn str_variation_detail(
        &self,
        context: &Context,
        flag_key: &str,
        default: String,
    ) -> Detail<String> {
        self.variation_detail(context, flag_key, default.clone())
            .try_map(|val| val.as_string(), default, eval::Error::WrongType)
    }

    /// This method is the same as [Client::float_variation], but also returns further information
    /// about how the value was calculated. The "reason" data will also be included in analytics
    /// events.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluation-reasons#rust>.
    pub fn float_variation_detail(
        &self,
        context: &Context,
        flag_key: &str,
        default: f64,
    ) -> Detail<f64> {
        self.variation_detail(context, flag_key, default).try_map(
            |val| val.as_float(),
            default,
            eval::Error::WrongType,
        )
    }

    /// This method is the same as [Client::int_variation], but also returns further information
    /// about how the value was calculated. The "reason" data will also be included in analytics
    /// events.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluation-reasons#rust>.
    pub fn int_variation_detail(
        &self,
        context: &Context,
        flag_key: &str,
        default: i64,
    ) -> Detail<i64> {
        self.variation_detail(context, flag_key, default).try_map(
            |val| val.as_int(),
            default,
            eval::Error::WrongType,
        )
    }

    /// This method is the same as [Client::json_variation], but also returns further information
    /// about how the value was calculated. The "reason" data will also be included in analytics
    /// events.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluation-reasons#rust>.
    pub fn json_variation_detail(
        &self,
        context: &Context,
        flag_key: &str,
        default: serde_json::Value,
    ) -> Detail<serde_json::Value> {
        self.variation_detail(context, flag_key, default.clone())
            .try_map(|val| val.as_json(), default, eval::Error::WrongType)
    }

    /// Generates the secure mode hash value for a context.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/secure-mode#rust>.
    pub fn secure_mode_hash(&self, context: &Context) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, self.sdk_key.as_bytes());
        let tag = ring::hmac::sign(&key, context.canonical_key().as_bytes());

        data_encoding::HEXLOWER.encode(tag.as_ref())
    }

    /// Returns an object that encapsulates the state of all feature flags for a given context. This
    /// includes the flag values, and also metadata that can be used on the front end.
    ///
    /// The most common use case for this method is to bootstrap a set of client-side feature flags
    /// from a back-end service.
    ///
    /// You may pass any configuration of [FlagDetailConfig] to control what data is included.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/all-flags#rust>
    pub fn all_flags_detail(
        &self,
        context: &Context,
        flag_state_config: FlagDetailConfig,
    ) -> FlagDetail {
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

        let data_store = self.data_store.read();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&*data_store, context, flag_state_config);

        flag_detail
    }

    /// This method is the same as [Client::variation], but also returns further information about
    /// how the value was calculated. The "reason" data will also be included in analytics events.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluation-reasons#rust>.
    pub fn variation_detail<T: Into<FlagValue> + Clone>(
        &self,
        context: &Context,
        flag_key: &str,
        default: T,
    ) -> Detail<FlagValue> {
        let (detail, _) =
            self.variation_internal(context, flag_key, default, &self.events_with_reasons);
        detail
    }

    /// This is a generic function which returns the value of a feature flag for a given context.
    ///
    /// This method is an alternatively to the type specified methods (e.g.
    /// [Client::bool_variation], [Client::int_variation], etc.).
    ///
    /// Returns `default` if there is an error, if the flag doesn't exist, or the feature is turned
    /// off and has no off variation.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/evaluating#rust>.
    pub fn variation<T: Into<FlagValue> + Clone>(
        &self,
        context: &Context,
        flag_key: &str,
        default: T,
    ) -> FlagValue {
        let (detail, _) = self.variation_internal(context, flag_key, default, &self.events_default);
        detail.value.unwrap()
    }

    /// This method returns the migration stage of the migration feature flag for the given
    /// evaluation context.
    ///
    /// This method returns the default stage if there is an error or the flag does not exist.
    pub fn migration_variation(
        &self,
        context: &Context,
        flag_key: &str,
        default_stage: Stage,
    ) -> (Stage, Arc<Mutex<MigrationOpTracker>>) {
        let (detail, flag) =
            self.variation_internal(context, flag_key, default_stage, &self.events_default);

        let migration_detail =
            detail.try_map(|v| v.try_into().ok(), default_stage, eval::Error::WrongType);
        let tracker = MigrationOpTracker::new(
            flag_key.into(),
            flag,
            context.clone(),
            migration_detail.clone(),
            default_stage,
        );

        (
            migration_detail.value.unwrap_or(default_stage),
            Arc::new(Mutex::new(tracker)),
        )
    }

    /// Reports that a context has performed an event.
    ///
    /// The `key` parameter is defined by the application and will be shown in analytics reports;
    /// it normally corresponds to the event name of a metric that you have created through the
    /// LaunchDarkly dashboard. If you want to associate additional data with this event, use
    /// [Client::track_data] or [Client::track_metric].
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/events#rust>.
    pub fn track_event(&self, context: Context, key: impl Into<String>) {
        let _ = self.track(context, key, None, serde_json::Value::Null);
    }

    /// Reports that a context has performed an event, and associates it with custom data.
    ///
    /// The `key` parameter is defined by the application and will be shown in analytics reports;
    /// it normally corresponds to the event name of a metric that you have created through the
    /// LaunchDarkly dashboard.
    ///
    /// `data` parameter is any type that implements [Serialize]. If no such value is needed, use
    /// [serde_json::Value::Null] (or call [Client::track_event] instead). To send a numeric value
    /// for experimentation, use [Client::track_metric].
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/events#rust>.
    pub fn track_data(
        &self,
        context: Context,
        key: impl Into<String>,
        data: impl Serialize,
    ) -> serde_json::Result<()> {
        self.track(context, key, None, data)
    }

    /// Reports that a context has performed an event, and associates it with a numeric value. This
    /// value is used by the LaunchDarkly experimentation feature in numeric custom metrics, and
    /// will also be returned as part of the custom event for Data Export.
    ///
    /// The `key` parameter is defined by the application and will be shown in analytics reports;
    /// it normally corresponds to the event name of a metric that you have created through the
    /// LaunchDarkly dashboard.
    ///
    /// For more information, see the Reference Guide:
    /// <https://docs.launchdarkly.com/sdk/features/events#rust>.
    pub fn track_metric(
        &self,
        context: Context,
        key: impl Into<String>,
        value: f64,
        data: impl Serialize,
    ) {
        let _ = self.track(context, key, Some(value), data);
    }

    fn track(
        &self,
        context: Context,
        key: impl Into<String>,
        metric_value: Option<f64>,
        data: impl Serialize,
    ) -> serde_json::Result<()> {
        if !self.events_default.disabled {
            let event =
                self.events_default
                    .event_factory
                    .new_custom(context, key, metric_value, data)?;

            self.send_internal(event);
        }

        Ok(())
    }

    /// Tracks the results of a migrations operation. This event includes measurements which can be
    /// used to enhance the observability of a migration within the LaunchDarkly UI.
    ///
    /// This event should be generated through [crate::MigrationOpTracker]. If you are using the
    /// [crate::Migrator] to handle migrations, this event will be created and emitted
    /// automatically.
    pub fn track_migration_op(&self, tracker: Arc<Mutex<MigrationOpTracker>>) {
        if self.events_default.disabled {
            return;
        }

        match tracker.lock() {
            Ok(tracker) => {
                let event = tracker.build();
                match event {
                    Ok(event) => {
                        self.send_internal(
                            self.events_default.event_factory.new_migration_op(event),
                        );
                    }
                    Err(e) => error!(
                        "Failed to build migration event, no event will be sent: {}",
                        e
                    ),
                }
            }
            Err(e) => error!(
                "Failed to lock migration tracker, no event will be sent: {}",
                e
            ),
        }
    }

    fn variation_internal<T: Into<FlagValue> + Clone>(
        &self,
        context: &Context,
        flag_key: &str,
        default: T,
        events_scope: &EventsScope,
    ) -> (Detail<FlagValue>, Option<eval::Flag>) {
        if self.offline {
            return (
                Detail::err_default(eval::Error::ClientNotReady, default.into()),
                None,
            );
        }

        let (flag, result) = match self.initialized() {
            false => (
                None,
                Detail::err_default(eval::Error::ClientNotReady, default.clone().into()),
            ),
            true => {
                let data_store = self.data_store.read();
                match data_store.flag(flag_key) {
                    Some(flag) => {
                        let result = eval::evaluate(
                            data_store.to_store(),
                            &flag,
                            context,
                            Some(&*events_scope.prerequisite_event_recorder),
                        )
                        .map(|v| v.clone())
                        .or(default.clone().into());

                        (Some(flag), result)
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
                Some(f) => events_scope.event_factory.new_eval_event(
                    flag_key,
                    context.clone(),
                    f,
                    result.clone(),
                    default.into(),
                    None,
                ),
                None => events_scope.event_factory.new_unknown_flag_event(
                    flag_key,
                    context.clone(),
                    result.clone(),
                    default.into(),
                ),
            };
            self.send_internal(event);
        }

        (result, flag)
    }

    fn send_internal(&self, event: InputEvent) {
        self.event_processor.send(event);
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::Receiver;
    use eval::{ContextBuilder, MultiContextBuilder};
    use futures::FutureExt;
    use hyper::client::HttpConnector;
    use launchdarkly_server_sdk_evaluation::Reason;
    use std::collections::HashMap;

    use tokio::time::Instant;

    use crate::data_source::MockDataSource;
    use crate::data_source_builders::MockDataSourceBuilder;
    use crate::events::create_event_sender;
    use crate::events::event::{OutputEvent, VariationKey};
    use crate::events::processor_builders::EventProcessorBuilder;
    use crate::stores::store_types::{PatchTarget, StorageItem};
    use crate::test_common::{
        self, basic_flag, basic_flag_with_prereq, basic_int_flag, basic_migration_flag,
        basic_off_flag,
    };
    use crate::{ConfigBuilder, MigratorBuilder, Operation, Origin};
    use test_case::test_case;

    use super::*;

    fn is_send_and_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_client_is_send_and_sync() {
        is_send_and_sync::<Client>()
    }

    #[tokio::test]
    async fn client_asynchronously_initializes() {
        let (client, _event_rx) = make_mocked_client_with_delay(1000, false);
        client.start_with_default_executor();

        let now = Instant::now();
        let initialized = client.initialized_async().await;
        let elapsed_time = now.elapsed();
        assert!(initialized);
        // Give ourself a good margin for thread scheduling.
        assert!(elapsed_time.as_millis() > 500)
    }

    #[tokio::test]
    async fn client_asynchronously_initializes_within_timeout() {
        let (client, _event_rx) = make_mocked_client_with_delay(1000, false);
        client.start_with_default_executor();

        let now = Instant::now();
        let initialized = client
            .wait_for_initialization(Duration::from_millis(1500))
            .await;
        let elapsed_time = now.elapsed();
        // Give ourself a good margin for thread scheduling.
        assert!(elapsed_time.as_millis() > 500);
        assert_eq!(initialized, Some(true));
    }

    #[tokio::test]
    async fn client_asynchronously_initializes_slower_than_timeout() {
        let (client, _event_rx) = make_mocked_client_with_delay(2000, false);
        client.start_with_default_executor();

        let now = Instant::now();
        let initialized = client
            .wait_for_initialization(Duration::from_millis(500))
            .await;
        let elapsed_time = now.elapsed();
        // Give ourself a good margin for thread scheduling.
        assert!(elapsed_time.as_millis() < 750);
        assert!(initialized.is_none());
    }

    #[tokio::test]
    async fn client_initializes_immediately_in_offline_mode() {
        let (client, _event_rx) = make_mocked_client_with_delay(1000, true);
        client.start_with_default_executor();

        assert!(client.initialized());

        let now = Instant::now();
        let initialized = client
            .wait_for_initialization(Duration::from_millis(2000))
            .await;
        let elapsed_time = now.elapsed();
        assert_eq!(initialized, Some(true));
        assert!(elapsed_time.as_millis() < 500)
    }

    #[test_case(basic_flag("myFlag"), false.into(), true.into())]
    #[test_case(basic_int_flag("myFlag"), 0.into(), test_common::FLOAT_TO_INT_MAX.into())]
    fn client_updates_changes_evaluation_results(
        flag: eval::Flag,
        default: FlagValue,
        expected: FlagValue,
    ) {
        let context = ContextBuilder::new("foo")
            .build()
            .expect("Failed to create context");

        let (client, _event_rx) = make_mocked_client();

        let result = client.variation_detail(&context, "myFlag", default.clone());
        assert_eq!(result.value.unwrap(), default);

        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                &flag.key,
                PatchTarget::Flag(StorageItem::Item(flag.clone())),
            )
            .expect("patch should apply");

        let result = client.variation_detail(&context, "myFlag", default);
        assert_eq!(result.value.unwrap(), expected);
        assert!(matches!(
            result.reason,
            Reason::Fallthrough {
                in_experiment: false
            }
        ));
    }

    #[test]
    fn variation_tracks_events_correctly() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "myFlag",
                PatchTarget::Flag(StorageItem::Item(basic_flag("myFlag"))),
            )
            .expect("patch should apply");
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let flag_value = client.variation(&context, "myFlag", FlagValue::Bool(false));

        assert!(flag_value.as_bool().unwrap());
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                version: Some(42),
                variation: Some(1),
            };
            let feature = event_summary.features.get("myFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn variation_handles_offline_mode() {
        let (client, event_rx) = make_mocked_offline_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let flag_value = client.variation(&context, "myFlag", FlagValue::Bool(false));

        assert!(!flag_value.as_bool().unwrap());
        client.flush();
        client.close();

        assert_eq!(event_rx.iter().count(), 0);
    }

    #[test]
    fn variation_handles_unknown_flags() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let flag_value = client.variation(&context, "non-existent-flag", FlagValue::Bool(false));

        assert!(!flag_value.as_bool().unwrap());
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                version: None,
                variation: None,
            };

            let feature = event_summary.features.get("non-existent-flag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn variation_detail_handles_debug_events_correctly() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        let mut flag = basic_flag("myFlag");
        flag.debug_events_until_date = Some(64_060_606_800_000); // Jan. 1st, 4000

        client
            .data_store
            .write()
            .upsert(
                &flag.key,
                PatchTarget::Flag(StorageItem::Item(flag.clone())),
            )
            .expect("patch should apply");
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let detail = client.variation_detail(&context, "myFlag", FlagValue::Bool(false));

        assert!(detail.value.unwrap().as_bool().unwrap());
        assert!(matches!(
            detail.reason,
            Reason::Fallthrough {
                in_experiment: false
            }
        ));
        client.flush();
        client.close();

        let events = event_rx.try_iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "debug");
        assert_eq!(events[2].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[2].clone() {
            let variation_key = VariationKey {
                version: Some(42),
                variation: Some(1),
            };

            let feature = event_summary.features.get("myFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn variation_detail_tracks_events_correctly() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        client
            .data_store
            .write()
            .upsert(
                "myFlag",
                PatchTarget::Flag(StorageItem::Item(basic_flag("myFlag"))),
            )
            .expect("patch should apply");
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let detail = client.variation_detail(&context, "myFlag", FlagValue::Bool(false));

        assert!(detail.value.unwrap().as_bool().unwrap());
        assert!(matches!(
            detail.reason,
            Reason::Fallthrough {
                in_experiment: false
            }
        ));
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                version: Some(42),
                variation: Some(1),
            };

            let feature = event_summary.features.get("myFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn variation_detail_handles_offline_mode() {
        let (client, event_rx) = make_mocked_offline_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let detail = client.variation_detail(&context, "myFlag", FlagValue::Bool(false));

        assert!(!detail.value.unwrap().as_bool().unwrap());
        assert!(matches!(
            detail.reason,
            Reason::Error {
                error: eval::Error::ClientNotReady
            }
        ));
        client.flush();
        client.close();

        assert_eq!(event_rx.iter().count(), 0);
    }

    #[test]
    fn variation_handles_off_flag_without_variation() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        client
            .data_store
            .write()
            .upsert(
                "myFlag",
                PatchTarget::Flag(StorageItem::Item(basic_off_flag("myFlag"))),
            )
            .expect("patch should apply");
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let result = client.variation(&context, "myFlag", FlagValue::Bool(false));

        assert!(!result.as_bool().unwrap());
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                version: Some(42),
                variation: None,
            };
            let feature = event_summary.features.get("myFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn variation_detail_tracks_prereq_events_correctly() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        let mut basic_preqreq_flag = basic_flag("prereqFlag");
        basic_preqreq_flag.track_events = true;

        client
            .data_store
            .write()
            .upsert(
                "prereqFlag",
                PatchTarget::Flag(StorageItem::Item(basic_preqreq_flag)),
            )
            .expect("patch should apply");

        let mut basic_flag = basic_flag_with_prereq("myFlag", "prereqFlag");
        basic_flag.track_events = true;
        client
            .data_store
            .write()
            .upsert("myFlag", PatchTarget::Flag(StorageItem::Item(basic_flag)))
            .expect("patch should apply");
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let detail = client.variation_detail(&context, "myFlag", FlagValue::Bool(false));

        assert!(detail.value.unwrap().as_bool().unwrap());
        assert!(matches!(
            detail.reason,
            Reason::Fallthrough {
                in_experiment: false
            }
        ));
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "feature");
        assert_eq!(events[2].kind(), "feature");
        assert_eq!(events[3].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[3].clone() {
            let variation_key = VariationKey {
                version: Some(42),
                variation: Some(1),
            };
            let feature = event_summary.features.get("myFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));

            let variation_key = VariationKey {
                version: Some(42),
                variation: Some(1),
            };
            let feature = event_summary.features.get("prereqFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        }
    }

    #[test]
    fn variation_handles_failed_prereqs_correctly() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        let mut basic_preqreq_flag = basic_off_flag("prereqFlag");
        basic_preqreq_flag.track_events = true;

        client
            .data_store
            .write()
            .upsert(
                "prereqFlag",
                PatchTarget::Flag(StorageItem::Item(basic_preqreq_flag)),
            )
            .expect("patch should apply");

        let mut basic_flag = basic_flag_with_prereq("myFlag", "prereqFlag");
        basic_flag.track_events = true;
        client
            .data_store
            .write()
            .upsert("myFlag", PatchTarget::Flag(StorageItem::Item(basic_flag)))
            .expect("patch should apply");
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let detail = client.variation(&context, "myFlag", FlagValue::Bool(false));

        assert!(!detail.as_bool().unwrap());
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "feature");
        assert_eq!(events[2].kind(), "feature");
        assert_eq!(events[3].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[3].clone() {
            let variation_key = VariationKey {
                version: Some(42),
                variation: Some(0),
            };
            let feature = event_summary.features.get("myFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));

            let variation_key = VariationKey {
                version: Some(42),
                variation: None,
            };
            let feature = event_summary.features.get("prereqFlag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        }
    }

    #[test]
    fn variation_detail_handles_flag_not_found() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let detail = client.variation_detail(&context, "non-existent-flag", FlagValue::Bool(false));

        assert!(!detail.value.unwrap().as_bool().unwrap());
        assert!(matches!(
            detail.reason,
            Reason::Error {
                error: eval::Error::FlagNotFound
            }
        ));
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                version: None,
                variation: None,
            };
            let feature = event_summary.features.get("non-existent-flag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[tokio::test]
    async fn variation_detail_handles_client_not_ready() {
        let (client, event_rx) = make_mocked_client_with_delay(u64::MAX, false);
        client.start_with_default_executor();
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let detail = client.variation_detail(&context, "non-existent-flag", FlagValue::Bool(false));

        assert!(!detail.value.unwrap().as_bool().unwrap());
        assert!(matches!(
            detail.reason,
            Reason::Error {
                error: eval::Error::ClientNotReady
            }
        ));
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind(), "index");
        assert_eq!(events[1].kind(), "summary");

        if let OutputEvent::Summary(event_summary) = events[1].clone() {
            let variation_key = VariationKey {
                version: None,
                variation: None,
            };
            let feature = event_summary.features.get("non-existent-flag");
            assert!(feature.is_some());

            let feature = feature.unwrap();
            assert!(feature.counters.contains_key(&variation_key));
        } else {
            panic!("Event should be a summary type");
        }
    }

    #[test]
    fn identify_sends_identify_event() {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        client.identify(context);
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind(), "identify");
    }

    #[test]
    fn identify_sends_sends_nothing_in_offline_mode() {
        let (client, event_rx) = make_mocked_offline_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        client.identify(context);
        client.flush();
        client.close();

        assert_eq!(event_rx.iter().count(), 0);
    }

    #[test]
    fn secure_mode_hash() {
        let config = ConfigBuilder::new("secret")
            .offline(true)
            .build()
            .expect("config should build");
        let client = Client::build(config).expect("Should be built.");
        let context = ContextBuilder::new("Message")
            .build()
            .expect("Failed to create context");

        assert_eq!(
            client.secure_mode_hash(&context),
            "aa747c502a898200f9e4fa21bac68136f886a0e27aec70ba06daf2e2a5cb5597"
        );
    }

    #[test]
    fn secure_mode_hash_with_multi_kind() {
        let config = ConfigBuilder::new("secret")
            .offline(true)
            .build()
            .expect("config should build");
        let client = Client::build(config).expect("Should be built.");

        let org = ContextBuilder::new("org-key|1")
            .kind("org")
            .build()
            .expect("Failed to create context");
        let user = ContextBuilder::new("user-key:2")
            .build()
            .expect("Failed to create context");

        let context = MultiContextBuilder::new()
            .add_context(org)
            .add_context(user)
            .build()
            .expect("failed to build multi-context");

        assert_eq!(
            client.secure_mode_hash(&context),
            "5687e6383b920582ed50c2a96c98a115f1b6aad85a60579d761d9b8797415163"
        );
    }

    #[derive(Serialize)]
    struct MyCustomData {
        pub answer: u32,
    }

    #[test]
    fn track_sends_track_and_index_events() -> serde_json::Result<()> {
        let (client, event_rx) = make_mocked_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        client.track_event(context.clone(), "event-with-null");
        client.track_data(context.clone(), "event-with-string", "string-data")?;
        client.track_data(context.clone(), "event-with-json", json!({"answer": 42}))?;
        client.track_data(
            context.clone(),
            "event-with-struct",
            MyCustomData { answer: 42 },
        )?;
        client.track_metric(context, "event-with-metric", 42.0, serde_json::Value::Null);

        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 6);

        let mut events_by_type: HashMap<&str, usize> = HashMap::new();
        for event in events {
            if let Some(count) = events_by_type.get_mut(event.kind()) {
                *count += 1;
            } else {
                events_by_type.insert(event.kind(), 1);
            }
        }
        assert!(matches!(events_by_type.get("index"), Some(1)));
        assert!(matches!(events_by_type.get("custom"), Some(5)));

        Ok(())
    }

    #[test]
    fn track_sends_nothing_in_offline_mode() -> serde_json::Result<()> {
        let (client, event_rx) = make_mocked_offline_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        client.track_event(context.clone(), "event-with-null");
        client.track_data(context.clone(), "event-with-string", "string-data")?;
        client.track_data(context.clone(), "event-with-json", json!({"answer": 42}))?;
        client.track_data(
            context.clone(),
            "event-with-struct",
            MyCustomData { answer: 42 },
        )?;
        client.track_metric(context, "event-with-metric", 42.0, serde_json::Value::Null);

        client.flush();
        client.close();

        assert_eq!(event_rx.iter().count(), 0);

        Ok(())
    }

    #[test]
    fn migration_handles_flag_not_found() {
        let (client, _event_rx) = make_mocked_client();
        client.start_with_default_executor();

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let (stage, _tracker) =
            client.migration_variation(&context, "non-existent-flag-key", Stage::Off);

        assert_eq!(stage, Stage::Off);
    }

    #[test]
    fn migration_uses_non_migration_flag() {
        let (client, _event_rx) = make_mocked_client();
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "boolean-flag",
                PatchTarget::Flag(StorageItem::Item(basic_flag("boolean-flag"))),
            )
            .expect("patch should apply");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let (stage, _tracker) = client.migration_variation(&context, "boolean-flag", Stage::Off);

        assert_eq!(stage, Stage::Off);
    }

    #[test_case(Stage::Off)]
    #[test_case(Stage::DualWrite)]
    #[test_case(Stage::Shadow)]
    #[test_case(Stage::Live)]
    #[test_case(Stage::Rampdown)]
    #[test_case(Stage::Complete)]
    fn migration_can_determine_correct_stage_from_flag(stage: Stage) {
        let (client, _event_rx) = make_mocked_client();
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "stage-flag",
                PatchTarget::Flag(StorageItem::Item(basic_migration_flag("stage-flag", stage))),
            )
            .expect("patch should apply");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        let (evaluated_stage, _tracker) =
            client.migration_variation(&context, "stage-flag", Stage::Off);

        assert_eq!(evaluated_stage, stage);
    }

    #[tokio::test]
    async fn migration_tracks_invoked_correctly() {
        migration_tracks_invoked_correctly_driver(Stage::Off, Operation::Read, vec![Origin::Old])
            .await;
        migration_tracks_invoked_correctly_driver(
            Stage::DualWrite,
            Operation::Read,
            vec![Origin::Old],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Shadow,
            Operation::Read,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Live,
            Operation::Read,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Rampdown,
            Operation::Read,
            vec![Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Complete,
            Operation::Read,
            vec![Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(Stage::Off, Operation::Write, vec![Origin::Old])
            .await;
        migration_tracks_invoked_correctly_driver(
            Stage::DualWrite,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Shadow,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Live,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Rampdown,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_invoked_correctly_driver(
            Stage::Complete,
            Operation::Write,
            vec![Origin::New],
        )
        .await;
    }

    async fn migration_tracks_invoked_correctly_driver(
        stage: Stage,
        operation: Operation,
        origins: Vec<Origin>,
    ) {
        let (client, event_rx) = make_mocked_client();
        let client = Arc::new(client);
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "stage-flag",
                PatchTarget::Flag(StorageItem::Item(basic_migration_flag("stage-flag", stage))),
            )
            .expect("patch should apply");

        let mut migrator = MigratorBuilder::new(client.clone())
            .read(
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
                Some(|_, _| true),
            )
            .write(
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
            )
            .build()
            .expect("migrator should build");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        if let Operation::Read = operation {
            migrator
                .read(
                    &context,
                    "stage-flag".into(),
                    Stage::Off,
                    serde_json::Value::Null,
                )
                .await;
        } else {
            migrator
                .write(
                    &context,
                    "stage-flag".into(),
                    Stage::Off,
                    serde_json::Value::Null,
                )
                .await;
        }

        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 3);
        match &events[1] {
            OutputEvent::MigrationOp(event) => {
                assert!(event.invoked.len() == origins.len());
                assert!(event.invoked.iter().all(|i| origins.contains(i)));
            }
            _ => panic!("Expected migration event"),
        }
    }

    #[tokio::test]
    async fn migration_tracks_latency() {
        migration_tracks_latency_driver(Stage::Off, Operation::Read, vec![Origin::Old]).await;
        migration_tracks_latency_driver(Stage::DualWrite, Operation::Read, vec![Origin::Old]).await;
        migration_tracks_latency_driver(
            Stage::Shadow,
            Operation::Read,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_latency_driver(
            Stage::Live,
            Operation::Read,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_latency_driver(Stage::Rampdown, Operation::Read, vec![Origin::New]).await;
        migration_tracks_latency_driver(Stage::Complete, Operation::Read, vec![Origin::New]).await;
        migration_tracks_latency_driver(Stage::Off, Operation::Write, vec![Origin::Old]).await;
        migration_tracks_latency_driver(
            Stage::DualWrite,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_latency_driver(
            Stage::Shadow,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_latency_driver(
            Stage::Live,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_latency_driver(
            Stage::Rampdown,
            Operation::Write,
            vec![Origin::Old, Origin::New],
        )
        .await;
        migration_tracks_latency_driver(Stage::Complete, Operation::Write, vec![Origin::New]).await;
    }

    async fn migration_tracks_latency_driver(
        stage: Stage,
        operation: Operation,
        origins: Vec<Origin>,
    ) {
        let (client, event_rx) = make_mocked_client();
        let client = Arc::new(client);
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "stage-flag",
                PatchTarget::Flag(StorageItem::Item(basic_migration_flag("stage-flag", stage))),
            )
            .expect("patch should apply");

        let mut migrator = MigratorBuilder::new(client.clone())
            .track_latency(true)
            .read(
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::Null)
                    }
                    .boxed()
                },
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::Null)
                    }
                    .boxed()
                },
                Some(|_, _| true),
            )
            .write(
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::Null)
                    }
                    .boxed()
                },
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::Null)
                    }
                    .boxed()
                },
            )
            .build()
            .expect("migrator should build");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        if let Operation::Read = operation {
            migrator
                .read(
                    &context,
                    "stage-flag".into(),
                    Stage::Off,
                    serde_json::Value::Null,
                )
                .await;
        } else {
            migrator
                .write(
                    &context,
                    "stage-flag".into(),
                    Stage::Off,
                    serde_json::Value::Null,
                )
                .await;
        }

        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 3);
        match &events[1] {
            OutputEvent::MigrationOp(event) => {
                assert!(event.latency.len() == origins.len());
                assert!(event
                    .latency
                    .values()
                    .all(|l| l > &Duration::from_millis(100)));
            }
            _ => panic!("Expected migration event"),
        }
    }

    #[tokio::test]
    async fn migration_tracks_read_errors() {
        migration_tracks_read_errors_driver(Stage::Off, vec![Origin::Old]).await;
        migration_tracks_read_errors_driver(Stage::DualWrite, vec![Origin::Old]).await;
        migration_tracks_read_errors_driver(Stage::Shadow, vec![Origin::Old, Origin::New]).await;
        migration_tracks_read_errors_driver(Stage::Live, vec![Origin::Old, Origin::New]).await;
        migration_tracks_read_errors_driver(Stage::Rampdown, vec![Origin::New]).await;
        migration_tracks_read_errors_driver(Stage::Complete, vec![Origin::New]).await;
    }

    async fn migration_tracks_read_errors_driver(stage: Stage, origins: Vec<Origin>) {
        let (client, event_rx) = make_mocked_client();
        let client = Arc::new(client);
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "stage-flag",
                PatchTarget::Flag(StorageItem::Item(basic_migration_flag("stage-flag", stage))),
            )
            .expect("patch should apply");

        let mut migrator = MigratorBuilder::new(client.clone())
            .track_latency(true)
            .read(
                |_| async move { Err("fail".into()) }.boxed(),
                |_| async move { Err("fail".into()) }.boxed(),
                Some(|_: &String, _: &String| true),
            )
            .write(
                |_| async move { Err("fail".into()) }.boxed(),
                |_| async move { Err("fail".into()) }.boxed(),
            )
            .build()
            .expect("migrator should build");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        migrator
            .read(
                &context,
                "stage-flag".into(),
                Stage::Off,
                serde_json::Value::Null,
            )
            .await;
        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 3);
        match &events[1] {
            OutputEvent::MigrationOp(event) => {
                assert!(event.errors.len() == origins.len());
                assert!(event.errors.iter().all(|i| origins.contains(i)));
            }
            _ => panic!("Expected migration event"),
        }
    }

    #[tokio::test]
    async fn migration_tracks_authoritative_write_errors() {
        migration_tracks_authoritative_write_errors_driver(Stage::Off, vec![Origin::Old]).await;
        migration_tracks_authoritative_write_errors_driver(Stage::DualWrite, vec![Origin::Old])
            .await;
        migration_tracks_authoritative_write_errors_driver(Stage::Shadow, vec![Origin::Old]).await;
        migration_tracks_authoritative_write_errors_driver(Stage::Live, vec![Origin::New]).await;
        migration_tracks_authoritative_write_errors_driver(Stage::Rampdown, vec![Origin::New])
            .await;
        migration_tracks_authoritative_write_errors_driver(Stage::Complete, vec![Origin::New])
            .await;
    }

    async fn migration_tracks_authoritative_write_errors_driver(
        stage: Stage,
        origins: Vec<Origin>,
    ) {
        let (client, event_rx) = make_mocked_client();
        let client = Arc::new(client);
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "stage-flag",
                PatchTarget::Flag(StorageItem::Item(basic_migration_flag("stage-flag", stage))),
            )
            .expect("patch should apply");

        let mut migrator = MigratorBuilder::new(client.clone())
            .track_latency(true)
            .read(
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
                None,
            )
            .write(
                |_| async move { Err("fail".into()) }.boxed(),
                |_| async move { Err("fail".into()) }.boxed(),
            )
            .build()
            .expect("migrator should build");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        migrator
            .write(
                &context,
                "stage-flag".into(),
                Stage::Off,
                serde_json::Value::Null,
            )
            .await;

        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 3);
        match &events[1] {
            OutputEvent::MigrationOp(event) => {
                assert!(event.errors.len() == origins.len());
                assert!(event.errors.iter().all(|i| origins.contains(i)));
            }
            _ => panic!("Expected migration event"),
        }
    }

    #[tokio::test]
    async fn migration_tracks_nonauthoritative_write_errors() {
        migration_tracks_nonauthoritative_write_errors_driver(
            Stage::DualWrite,
            false,
            true,
            vec![Origin::New],
        )
        .await;
        migration_tracks_nonauthoritative_write_errors_driver(
            Stage::Shadow,
            false,
            true,
            vec![Origin::New],
        )
        .await;
        migration_tracks_nonauthoritative_write_errors_driver(
            Stage::Live,
            true,
            false,
            vec![Origin::Old],
        )
        .await;
        migration_tracks_nonauthoritative_write_errors_driver(
            Stage::Rampdown,
            true,
            false,
            vec![Origin::Old],
        )
        .await;
    }

    async fn migration_tracks_nonauthoritative_write_errors_driver(
        stage: Stage,
        fail_old: bool,
        fail_new: bool,
        origins: Vec<Origin>,
    ) {
        let (client, event_rx) = make_mocked_client();
        let client = Arc::new(client);
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "stage-flag",
                PatchTarget::Flag(StorageItem::Item(basic_migration_flag("stage-flag", stage))),
            )
            .expect("patch should apply");

        let mut migrator = MigratorBuilder::new(client.clone())
            .track_latency(true)
            .read(
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
                |_| async move { Ok(serde_json::Value::Null) }.boxed(),
                None,
            )
            .write(
                move |_| {
                    async move {
                        if fail_old {
                            Err("fail".into())
                        } else {
                            Ok(serde_json::Value::Null)
                        }
                    }
                    .boxed()
                },
                move |_| {
                    async move {
                        if fail_new {
                            Err("fail".into())
                        } else {
                            Ok(serde_json::Value::Null)
                        }
                    }
                    .boxed()
                },
            )
            .build()
            .expect("migrator should build");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        migrator
            .write(
                &context,
                "stage-flag".into(),
                Stage::Off,
                serde_json::Value::Null,
            )
            .await;

        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 3);
        match &events[1] {
            OutputEvent::MigrationOp(event) => {
                assert!(event.errors.len() == origins.len());
                assert!(event.errors.iter().all(|i| origins.contains(i)));
            }
            _ => panic!("Expected migration event"),
        }
    }

    #[tokio::test]
    async fn migration_tracks_consistency() {
        migration_tracks_consistency_driver(Stage::Shadow, "same", "same", true).await;
        migration_tracks_consistency_driver(Stage::Shadow, "same", "different", false).await;
        migration_tracks_consistency_driver(Stage::Live, "same", "same", true).await;
        migration_tracks_consistency_driver(Stage::Live, "same", "different", false).await;
    }

    async fn migration_tracks_consistency_driver(
        stage: Stage,
        old_return: &'static str,
        new_return: &'static str,
        expected_consistency: bool,
    ) {
        let (client, event_rx) = make_mocked_client();
        let client = Arc::new(client);
        client.start_with_default_executor();
        client
            .data_store
            .write()
            .upsert(
                "stage-flag",
                PatchTarget::Flag(StorageItem::Item(basic_migration_flag("stage-flag", stage))),
            )
            .expect("patch should apply");

        let mut migrator = MigratorBuilder::new(client.clone())
            .track_latency(true)
            .read(
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::String(old_return.to_string()))
                    }
                    .boxed()
                },
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::String(new_return.to_string()))
                    }
                    .boxed()
                },
                Some(|lhs, rhs| lhs == rhs),
            )
            .write(
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::Null)
                    }
                    .boxed()
                },
                |_| {
                    async move {
                        async_std::task::sleep(Duration::from_millis(100)).await;
                        Ok(serde_json::Value::Null)
                    }
                    .boxed()
                },
            )
            .build()
            .expect("migrator should build");

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");

        migrator
            .read(
                &context,
                "stage-flag".into(),
                Stage::Off,
                serde_json::Value::Null,
            )
            .await;

        client.flush();
        client.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 3);
        match &events[1] {
            OutputEvent::MigrationOp(event) => {
                assert!(event.consistency_check == Some(expected_consistency))
            }
            _ => panic!("Expected migration event"),
        }
    }

    fn make_mocked_client_with_delay(delay: u64, offline: bool) -> (Client, Receiver<OutputEvent>) {
        let updates = Arc::new(MockDataSource::new_with_init_delay(delay));
        let (event_sender, event_rx) = create_event_sender();

        let config = ConfigBuilder::new("sdk-key")
            .offline(offline)
            .data_source(MockDataSourceBuilder::new().data_source(updates))
            .event_processor(
                EventProcessorBuilder::<HttpConnector>::new().event_sender(Arc::new(event_sender)),
            )
            .build()
            .expect("config should build");

        let client = Client::build(config).expect("Should be built.");

        (client, event_rx)
    }

    fn make_mocked_offline_client() -> (Client, Receiver<OutputEvent>) {
        make_mocked_client_with_delay(0, true)
    }

    fn make_mocked_client() -> (Client, Receiver<OutputEvent>) {
        make_mocked_client_with_delay(0, false)
    }
}
