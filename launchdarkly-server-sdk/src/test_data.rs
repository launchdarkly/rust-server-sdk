//! Test data source for use in tests.
//!
//! The [`TestData`] type provides a way to inject feature flag data into the SDK for testing,
//! without needing a LaunchDarkly connection. Pass it to [`crate::ConfigBuilder::data_source`]
//! to use it with the FDv1 data source, or to [`crate::DataSystemBuilder::synchronizer`] to use
//! it with the FDv2 data system.
//!
//! # Examples
//!
//! ```no_run
//! use launchdarkly_server_sdk::{TestData, FlagBuilder};
//!
//! let td = TestData::new();
//! td.update(FlagBuilder::new("flag-key-1").variation_for_all(true));
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use launchdarkly_server_sdk_evaluation::{Flag, FlagBuilder, Segment};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{broadcast, mpsc};

use crate::data_source::DataSource;
use crate::data_source_builders::{BuildError, DataSourceFactory};
use crate::data_system_builders::{
    BuildError as DataSystemBuildError, DataSourceBuildContext, FDv2SynchronizerConfig,
};
use crate::fdv2::data_system::SynchronizerFactory;
use crate::fdv2::model::{ChangeSetKind, Selector};
use crate::fdv2::source::{FDv2SourceEvent, FDv2SourceEventFuture, FDv2SourceResult, Synchronizer};
use crate::service_endpoints;
use crate::stores::change_set::{ChangeSet, ItemChange};
use crate::stores::store::DataStore;
use crate::stores::store_types::{AllData, PatchTarget, StorageItem};

/// Tracks whether a flag was created via a builder or set as a preconfigured flag.
enum FlagOrigin {
    Builder(FlagBuilder),
    Preconfigured,
}

struct TestDataInner {
    flag_origins: HashMap<String, FlagOrigin>,
    current_flags: HashMap<String, Flag>,
    flag_versions: HashMap<String, u64>,
    current_segments: HashMap<String, Segment>,
    segment_versions: HashMap<String, u64>,
    instances: Vec<Arc<RwLock<dyn DataStore>>>,
    fdv2_senders: Vec<mpsc::UnboundedSender<ChangeSet>>,
}

impl TestDataInner {
    /// Sends a partial change set for one flag to every registered FDv2 synchronizer.
    fn broadcast_flag(&mut self, flag: &Flag) {
        self.broadcast(ItemChange::Flag {
            key: flag.key.clone(),
            item: StorageItem::Item(flag.clone()),
        });
    }

    /// Sends a partial change set for one segment to every registered FDv2 synchronizer.
    fn broadcast_segment(&mut self, segment: &Segment) {
        self.broadcast(ItemChange::Segment {
            key: segment.key.clone(),
            item: StorageItem::Item(segment.clone()),
        });
    }

    /// Sends `change` to every registered FDv2 synchronizer, dropping any whose
    /// receiver has closed.
    fn broadcast(&mut self, change: ItemChange) {
        self.fdv2_senders.retain(|sender| {
            let change_set = ChangeSet {
                kind: ChangeSetKind::Partial,
                changes: vec![change.clone()],
                selector: None,
            };
            sender.send(change_set).is_ok()
        });
    }
}

/// A mechanism for providing dynamically updatable feature flag state in a simplified form to an
/// SDK client.
///
/// `TestData` can be passed to [`crate::ConfigBuilder::data_source`] to drive the FDv1 data
/// source, or to [`crate::DataSystemBuilder::synchronizer`] to drive the FDv2 data system. When
/// the SDK client is started, it receives the current flag state from `TestData` and is notified
/// of subsequent changes.
///
/// With the FDv2 data system, updates are delivered asynchronously. A change made with
/// [`TestData::update`] may take a moment to be reflected in flag evaluations.
///
/// Flag data can be provided using [`FlagBuilder`](crate::FlagBuilder) (via [`TestData::update`])
/// or by passing fully constructed [`Flag`] objects (via [`TestData::use_preconfigured_flag`]).
///
/// Cloning a `TestData` creates a new handle that shares the same underlying state.
#[derive(Clone)]
pub struct TestData {
    inner: Arc<Mutex<TestDataInner>>,
}

impl TestData {
    /// Creates a new `TestData` instance with no initial flag data.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestDataInner {
                flag_origins: HashMap::new(),
                current_flags: HashMap::new(),
                flag_versions: HashMap::new(),
                current_segments: HashMap::new(),
                segment_versions: HashMap::new(),
                instances: Vec::new(),
                fdv2_senders: Vec::new(),
            })),
        }
    }

    /// Returns a copy of the current [`FlagBuilder`](crate::FlagBuilder) for the specified flag key, or a new default
    /// builder if the flag has not been configured yet.
    ///
    /// If the flag was previously set via [`TestData::use_preconfigured_flag`], this returns a new
    /// default builder since [`FlagBuilder`](crate::FlagBuilder) cannot represent all flag fields
    /// (e.g. prerequisites, migration settings, client visibility) and hydrating from a [`Flag`]
    /// would silently lose that data.
    pub fn flag(&self, key: &str) -> FlagBuilder {
        let inner = self.inner.lock();
        match inner.flag_origins.get(key) {
            Some(FlagOrigin::Builder(builder)) => builder.clone(),
            _ => FlagBuilder::new(key),
        }
    }

    /// Updates the test data with the given flag builder.
    ///
    /// If there are any connected SDK clients, they will be notified of the flag change. The flag
    /// version is automatically incremented.
    pub fn update(&self, builder: FlagBuilder) {
        let mut inner = self.inner.lock();

        let key = builder.key().to_owned();
        let stored_builder = builder.clone();
        let mut flag = builder.build();

        let version = inner.flag_versions.entry(key.clone()).or_insert(0);
        *version += 1;
        flag.version = *version;

        inner
            .flag_origins
            .insert(key.clone(), FlagOrigin::Builder(stored_builder));
        inner.current_flags.insert(key.clone(), flag.clone());

        for store in &inner.instances {
            let mut store = store.write();
            let _ = store.upsert(&key, PatchTarget::Flag(StorageItem::Item(flag.clone())));
        }

        inner.broadcast_flag(&flag);
    }

    /// Sets a preconfigured [`Flag`] directly.
    ///
    /// Use this when you need to set a flag with a configuration that cannot be expressed through
    /// [`FlagBuilder`](crate::FlagBuilder). The flag version is automatically managed.
    ///
    /// Note: calling [`TestData::flag`] after this will return a new default builder, not a
    /// builder derived from the preconfigured flag.
    pub fn use_preconfigured_flag(&self, mut flag: Flag) {
        let mut inner = self.inner.lock();

        let key = flag.key.clone();
        let version = inner.flag_versions.entry(key.clone()).or_insert(0);
        *version += 1;
        flag.version = *version;

        inner
            .flag_origins
            .insert(key.clone(), FlagOrigin::Preconfigured);
        inner.current_flags.insert(key.clone(), flag.clone());

        for store in &inner.instances {
            let mut store = store.write();
            let _ = store.upsert(&key, PatchTarget::Flag(StorageItem::Item(flag.clone())));
        }

        inner.broadcast_flag(&flag);
    }

    /// Sets a preconfigured [`Segment`] directly.
    ///
    /// The segment version is automatically managed.
    pub fn use_preconfigured_segment(&self, mut segment: Segment) {
        let mut inner = self.inner.lock();

        let key = segment.key.clone();
        let version = inner.segment_versions.entry(key.clone()).or_insert(0);
        *version += 1;
        segment.version = *version;

        inner.current_segments.insert(key.clone(), segment.clone());

        for store in &inner.instances {
            let mut store = store.write();
            let _ = store.upsert(
                &key,
                PatchTarget::Segment(StorageItem::Item(segment.clone())),
            );
        }

        inner.broadcast_segment(&segment);
    }
}

impl Default for TestData {
    fn default() -> Self {
        Self::new()
    }
}

impl DataSourceFactory for TestData {
    fn build(
        &self,
        _endpoints: &service_endpoints::ServiceEndpoints,
        _sdk_key: &str,
        _tags: Option<String>,
    ) -> Result<Arc<dyn DataSource>, BuildError> {
        Ok(Arc::new(TestDataSource {
            inner: self.inner.clone(),
        }))
    }

    fn to_owned(&self) -> Box<dyn DataSourceFactory> {
        Box::new(self.clone())
    }
}

struct TestDataSource {
    inner: Arc<Mutex<TestDataInner>>,
}

impl DataSource for TestDataSource {
    fn subscribe(
        &self,
        data_store: Arc<RwLock<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    ) {
        let mut inner = self.inner.lock();

        // Initialize the store with all current data
        let all_data = AllData {
            flags: inner.current_flags.clone(),
            segments: inner.current_segments.clone(),
        };

        {
            let mut store = data_store.write();
            store.init(all_data);
        }

        // Register this store for future updates
        inner.instances.push(data_store.clone());

        (init_complete)(true);

        // Spawn a task to unregister the store on shutdown
        let inner_ref = self.inner.clone();
        let store_ref = data_store.clone();
        tokio::spawn(async move {
            let mut shutdown = shutdown_receiver;
            let _ = shutdown.recv().await;
            let mut inner = inner_ref.lock();
            inner.instances.retain(|s| !Arc::ptr_eq(s, &store_ref));
        });
    }
}

impl FDv2SynchronizerConfig for TestData {
    fn build_synchronizer(
        &self,
        _context: &DataSourceBuildContext,
    ) -> Result<Box<dyn SynchronizerFactory>, DataSystemBuildError> {
        Ok(Box::new(TestDataSynchronizerFactory {
            inner: self.inner.clone(),
        }))
    }

    fn to_owned(&self) -> Box<dyn FDv2SynchronizerConfig> {
        Box::new(self.clone())
    }
}

struct TestDataSynchronizerFactory {
    inner: Arc<Mutex<TestDataInner>>,
}

impl SynchronizerFactory for TestDataSynchronizerFactory {
    fn create(&self) -> Box<dyn Synchronizer> {
        let mut inner = self.inner.lock();

        // Snapshot the current flags and segments as the initial full payload.
        let mut changes = Vec::new();
        for flag in inner.current_flags.values() {
            changes.push(ItemChange::Flag {
                key: flag.key.clone(),
                item: StorageItem::Item(flag.clone()),
            });
        }
        for segment in inner.current_segments.values() {
            changes.push(ItemChange::Segment {
                key: segment.key.clone(),
                item: StorageItem::Item(segment.clone()),
            });
        }
        let initial = ChangeSet {
            kind: ChangeSetKind::Full,
            changes,
            selector: None,
        };

        // Register a channel so later updates reach this synchronizer.
        let (sender, receiver) = mpsc::unbounded_channel();
        inner.fdv2_senders.push(sender);

        Box::new(TestDataSynchronizer {
            initial: Some(initial),
            receiver,
        })
    }
}

struct TestDataSynchronizer {
    initial: Option<ChangeSet>,
    receiver: mpsc::UnboundedReceiver<ChangeSet>,
}

impl Synchronizer for TestDataSynchronizer {
    fn next(&mut self, _selector: Selector) -> FDv2SourceEventFuture<'_> {
        if let Some(change_set) = self.initial.take() {
            return Box::pin(async move {
                FDv2SourceEvent {
                    result: FDv2SourceResult::ChangeSet(change_set),
                    fdv1_fallback: None,
                }
            });
        }
        Box::pin(async move {
            match self.receiver.recv().await {
                Some(change_set) => FDv2SourceEvent {
                    result: FDv2SourceResult::ChangeSet(change_set),
                    fdv1_fallback: None,
                },
                // The test data was dropped, so idle instead of terminating.
                None => std::future::pending().await,
            }
        })
    }

    fn name(&self) -> &str {
        "test-data"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::store::InMemoryDataStore;
    use launchdarkly_server_sdk_evaluation::FlagBuilder;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn make_store() -> Arc<RwLock<dyn DataStore>> {
        Arc::new(RwLock::new(InMemoryDataStore::new()))
    }

    fn subscribe_store(td: &TestData, store: &Arc<RwLock<dyn DataStore>>) -> broadcast::Sender<()> {
        let factory: &dyn DataSourceFactory = td;
        let endpoints = crate::ServiceEndpointsBuilder::new().build().unwrap();
        let ds = factory.build(&endpoints, "fake-key", None).unwrap();

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        ds.subscribe(store.clone(), Arc::new(|_| {}), shutdown_rx);
        shutdown_tx
    }

    #[test]
    fn flag_returns_default_builder_for_unknown_key() {
        let td = TestData::new();
        let builder = td.flag("unknown");
        assert_eq!(builder.key(), "unknown");
        let flag = builder.build();
        assert_eq!(flag.key, "unknown");
    }

    #[test]
    fn flag_returns_cloned_builder_after_update() {
        let td = TestData::new();
        td.update(FlagBuilder::new("my-flag").variation_for_all(false));
        let builder = td.flag("my-flag");
        assert_eq!(builder.key(), "my-flag");
    }

    #[test]
    fn flag_returns_default_builder_for_preconfigured_flag() {
        let td = TestData::new();
        let flag = FlagBuilder::new("preconf").build();
        td.use_preconfigured_flag(flag);
        let builder = td.flag("preconf");
        assert_eq!(builder.key(), "preconf");
    }

    #[test]
    fn update_increments_version_each_call() {
        let td = TestData::new();
        td.update(FlagBuilder::new("my-flag"));
        td.update(FlagBuilder::new("my-flag"));
        td.update(FlagBuilder::new("my-flag"));

        let inner = td.inner.lock();
        let flag = inner.current_flags.get("my-flag").unwrap();
        assert_eq!(flag.version, 3);
    }

    #[test]
    fn use_preconfigured_flag_increments_version_each_call() {
        let td = TestData::new();
        td.use_preconfigured_flag(FlagBuilder::new("my-flag").build());
        td.use_preconfigured_flag(FlagBuilder::new("my-flag").build());
        td.use_preconfigured_flag(FlagBuilder::new("my-flag").build());

        let inner = td.inner.lock();
        let flag = inner.current_flags.get("my-flag").unwrap();
        assert_eq!(flag.version, 3);
    }

    #[test]
    fn flag_builder_increments_version_each_call() {
        let td = TestData::new();
        td.update(td.flag("my-flag").variation_for_all(true));
        td.update(td.flag("my-flag").variation_for_all(false));
        td.update(td.flag("my-flag").variation_for_all(true));

        let inner = td.inner.lock();
        let flag = inner.current_flags.get("my-flag").unwrap();
        assert_eq!(flag.version, 3);
    }

    #[tokio::test]
    async fn update_propagates_to_connected_store() {
        let td = TestData::new();
        let store = make_store();
        let _shutdown = subscribe_store(&td, &store);

        td.update(FlagBuilder::new("my-flag").variation_for_all(true));

        let s = store.read();
        let flag = s.flag("my-flag").unwrap();
        assert_eq!(flag.key, "my-flag");
    }

    #[tokio::test]
    async fn subscribe_initializes_store_with_all_current_data() {
        let td = TestData::new();
        td.update(FlagBuilder::new("flag-1").variation_for_all(true));
        td.update(FlagBuilder::new("flag-2").variation_for_all(false));

        let store = make_store();
        let _shutdown = subscribe_store(&td, &store);

        let s = store.read();
        assert!(s.flag("flag-1").is_some());
        assert!(s.flag("flag-2").is_some());
    }

    #[tokio::test]
    async fn subscribe_calls_init_complete_true() {
        let td = TestData::new();
        let store = make_store();

        let factory: &dyn DataSourceFactory = &td;
        let endpoints = crate::ServiceEndpointsBuilder::new().build().unwrap();
        let ds = factory.build(&endpoints, "fake-key", None).unwrap();

        let initialized = Arc::new(AtomicBool::new(false));
        let init_clone = initialized.clone();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        ds.subscribe(
            store,
            Arc::new(move |success| init_clone.store(success, Ordering::SeqCst)),
            shutdown_rx,
        );

        assert!(initialized.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn multiple_stores_receive_updates() {
        let td = TestData::new();
        let store1 = make_store();
        let store2 = make_store();
        let _shutdown1 = subscribe_store(&td, &store1);
        let _shutdown2 = subscribe_store(&td, &store2);

        td.update(FlagBuilder::new("shared-flag"));

        assert!(store1.read().flag("shared-flag").is_some());
        assert!(store2.read().flag("shared-flag").is_some());
    }

    #[tokio::test]
    async fn use_preconfigured_flag_propagates() {
        let td = TestData::new();
        let store = make_store();
        let _shutdown = subscribe_store(&td, &store);

        let flag = FlagBuilder::new("preconf").variation_for_all(true).build();
        td.use_preconfigured_flag(flag);

        let s = store.read();
        let stored_flag = s.flag("preconf").unwrap();
        assert_eq!(stored_flag.version, 1);
    }

    #[tokio::test]
    async fn use_preconfigured_segment_propagates() {
        let td = TestData::new();
        let store = make_store();
        let _shutdown = subscribe_store(&td, &store);

        let segment: Segment = serde_json::from_str(
            r#"{
                "key": "seg-1",
                "included": ["alice"],
                "excluded": [],
                "rules": [],
                "salt": "salty",
                "version": 999
            }"#,
        )
        .unwrap();

        td.use_preconfigured_segment(segment);

        let s = store.read();
        let stored = s.segment("seg-1").unwrap();
        assert_eq!(stored.version, 1); // version auto-managed, not 999
    }

    #[test]
    fn version_counters_are_independent_per_flag() {
        let td = TestData::new();
        td.update(FlagBuilder::new("a"));
        td.update(FlagBuilder::new("a"));
        td.update(FlagBuilder::new("b"));

        let inner = td.inner.lock();
        assert_eq!(inner.current_flags.get("a").unwrap().version, 2);
        assert_eq!(inner.current_flags.get("b").unwrap().version, 1);
    }

    #[tokio::test]
    async fn data_source_factory_build_returns_working_data_source() {
        let td = TestData::new();
        td.update(FlagBuilder::new("factory-flag"));

        let factory: &dyn DataSourceFactory = &td;
        let endpoints = crate::ServiceEndpointsBuilder::new().build().unwrap();
        let ds = factory.build(&endpoints, "key", None).unwrap();

        let store = make_store();
        let (_tx, rx) = broadcast::channel(1);
        ds.subscribe(store.clone(), Arc::new(|_| {}), rx);

        assert!(store.read().flag("factory-flag").is_some());
    }

    #[tokio::test]
    async fn data_source_factory_to_owned_shares_state() {
        let td = TestData::new();
        let owned = DataSourceFactory::to_owned(&td);

        td.update(FlagBuilder::new("shared-state"));

        let endpoints = crate::ServiceEndpointsBuilder::new().build().unwrap();
        let ds = owned.build(&endpoints, "key", None).unwrap();

        let store = make_store();
        let (_tx, rx) = broadcast::channel(1);
        ds.subscribe(store.clone(), Arc::new(|_| {}), rx);

        assert!(store.read().flag("shared-state").is_some());
    }

    #[tokio::test]
    async fn shutdown_unregisters_store() {
        let td = TestData::new();
        let store = make_store();
        let shutdown_tx = subscribe_store(&td, &store);

        assert_eq!(td.inner.lock().instances.len(), 1);

        let _ = shutdown_tx.send(());
        // Give the spawned task time to run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(td.inner.lock().instances.len(), 0);
    }

    fn build_fdv2_synchronizer(td: &TestData) -> Box<dyn crate::fdv2::source::Synchronizer> {
        use crate::fdv2::request_headers::RequestHeaders;

        let endpoints = crate::ServiceEndpointsBuilder::new().build().unwrap();
        let headers = RequestHeaders::new("fake-key", None, "test-instance");
        let context = DataSourceBuildContext {
            endpoints: &endpoints,
            headers: &headers,
        };
        FDv2SynchronizerConfig::build_synchronizer(td, &context)
            .unwrap()
            .create()
    }

    fn flag_keys(cs: &crate::stores::change_set::ChangeSet) -> Vec<&str> {
        cs.changes
            .iter()
            .filter_map(|c| match c {
                crate::stores::change_set::ItemChange::Flag { key, .. } => Some(key.as_str()),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn fdv2_synchronizer_emits_full_basis_then_partials() {
        use crate::fdv2::model::ChangeSetKind;
        use crate::fdv2::source::FDv2SourceResult;

        let td = TestData::new();
        td.update(FlagBuilder::new("flag-1").variation_for_all(true));

        let mut sync = build_fdv2_synchronizer(&td);

        // First next() delivers a full payload with the current flags.
        let event = sync.next(None).await;
        match event.result {
            FDv2SourceResult::ChangeSet(cs) => {
                assert_eq!(cs.kind, ChangeSetKind::Full);
                assert_eq!(flag_keys(&cs), vec!["flag-1"]);
            }
            other => panic!("expected a change set, got {other:?}"),
        }

        // A later update delivers a partial for just that flag.
        td.update(FlagBuilder::new("flag-2").variation_for_all(false));
        let event = sync.next(None).await;
        match event.result {
            FDv2SourceResult::ChangeSet(cs) => {
                assert_eq!(cs.kind, ChangeSetKind::Partial);
                assert_eq!(flag_keys(&cs), vec!["flag-2"]);
            }
            other => panic!("expected a change set, got {other:?}"),
        }
    }
}
