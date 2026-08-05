use std::sync::Arc;

use futures::FutureExt;
use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::data_system::DataSystem;
use crate::stores::store::{DataStore, InMemoryDataStore, TransactionalDataStore};

use super::model::Selector;
use super::source::{FDv2SourceResult, Initializer, Synchronizer};

/// Produces a fresh initializer each time the orchestrator starts a run.
pub(crate) trait InitializerFactory: Send + Sync {
    fn create(&self) -> Box<dyn Initializer>;
}

/// Produces a fresh synchronizer each time the orchestrator starts a run.
pub(crate) trait SynchronizerFactory: Send + Sync {
    fn create(&self) -> Box<dyn Synchronizer>;
}

/// FDv2 orchestrator: owns the memory store and keeps it populated by running
/// initializers to obtain a basis, then synchronizers for ongoing changes.
pub(crate) struct FDv2DataSystem {
    initializer_factories: Vec<Box<dyn InitializerFactory>>,
    synchronizer_factories: Vec<Box<dyn SynchronizerFactory>>,
    store: Arc<RwLock<InMemoryDataStore>>,
}

impl FDv2DataSystem {
    pub(crate) fn new(
        initializer_factories: Vec<Box<dyn InitializerFactory>>,
        synchronizer_factories: Vec<Box<dyn SynchronizerFactory>>,
    ) -> Self {
        Self {
            initializer_factories,
            synchronizer_factories,
            store: Arc::new(RwLock::new(InMemoryDataStore::new())),
        }
    }
}

impl DataSystem for FDv2DataSystem {
    fn start(
        &self,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    ) {
        let initializers = self
            .initializer_factories
            .iter()
            .map(|f| f.create())
            .collect();
        let synchronizers = self
            .synchronizer_factories
            .iter()
            .map(|f| f.create())
            .collect();
        let store = self.store.clone();

        tokio::spawn(run(
            initializers,
            synchronizers,
            store,
            init_complete,
            shutdown_receiver,
        ));
    }

    fn store(&self) -> Arc<RwLock<dyn DataStore>> {
        self.store.clone()
    }
}

async fn run(
    initializers: Vec<Box<dyn Initializer>>,
    synchronizers: Vec<Box<dyn Synchronizer>>,
    store: Arc<RwLock<InMemoryDataStore>>,
    init_complete: Arc<dyn Fn(bool) + Send + Sync>,
    mut shutdown_receiver: broadcast::Receiver<()>,
) {
    let mut selector: Selector = None;
    let mut initialized = false;

    // Initializer phase: try each in order until one yields a basis.
    for mut initializer in initializers {
        let mut shutdown = Box::pin(shutdown_receiver.recv()).fuse();
        futures::select! {
            _ = shutdown => return,
            event = initializer.run().fuse() => {
                if let FDv2SourceResult::ChangeSet(change_set) = event.result {
                    selector = change_set.selector.clone();
                    store.write().apply(change_set);
                    init_complete(true);
                    initialized = true;
                    break;
                }
            }
        }
    }

    // Synchronizer phase: use each in order, advancing on a terminal result.
    for mut synchronizer in synchronizers {
        loop {
            let mut shutdown = Box::pin(shutdown_receiver.recv()).fuse();
            let result = futures::select! {
                _ = shutdown => return,
                event = synchronizer.next(selector.clone()).fuse() => event.result,
            };
            match result {
                FDv2SourceResult::ChangeSet(change_set) => {
                    selector = change_set.selector.clone();
                    store.write().apply(change_set);
                    if !initialized {
                        init_complete(true);
                        initialized = true;
                    }
                }
                FDv2SourceResult::Interrupted(_) => continue,
                FDv2SourceResult::TerminalError(_)
                | FDv2SourceResult::Shutdown
                | FDv2SourceResult::Goodbye { .. } => break,
            }
        }
    }

    // Every source exhausted without ever obtaining a basis.
    if !initialized {
        init_complete(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use futures::future::BoxFuture;
    use launchdarkly_server_sdk_evaluation::Store;

    use super::super::model::ChangeSetKind;
    use super::super::source::{ErrorInfo, ErrorKind, FDv2SourceEvent};
    use crate::stores::change_set::{ChangeSet, ItemChange};
    use crate::stores::store_types::StorageItem;
    use crate::test_common::basic_flag;

    type Selectors = Arc<Mutex<Vec<Selector>>>;
    type InitCalls = Arc<Mutex<Vec<bool>>>;

    fn changeset(kind: ChangeSetKind, key: &str, selector: Selector) -> FDv2SourceResult {
        FDv2SourceResult::ChangeSet(ChangeSet {
            kind,
            changes: vec![ItemChange::Flag {
                key: key.to_string(),
                item: StorageItem::Item(basic_flag(key)),
            }],
            selector,
        })
    }

    fn interrupted() -> FDv2SourceResult {
        FDv2SourceResult::Interrupted(ErrorInfo {
            kind: ErrorKind::Unknown,
            message: "test".into(),
        })
    }

    fn terminal() -> FDv2SourceResult {
        FDv2SourceResult::TerminalError(ErrorInfo {
            kind: ErrorKind::Unknown,
            message: "test".into(),
        })
    }

    fn event(result: FDv2SourceResult) -> FDv2SourceEvent {
        FDv2SourceEvent {
            result,
            fdv1_fallback: None,
        }
    }

    struct MockInitializer {
        results: VecDeque<FDv2SourceResult>,
    }

    impl Initializer for MockInitializer {
        fn run(&mut self) -> BoxFuture<'_, FDv2SourceEvent> {
            let result = self
                .results
                .pop_front()
                .unwrap_or(FDv2SourceResult::Shutdown);
            Box::pin(async move { event(result) })
        }

        fn name(&self) -> &str {
            "mock-initializer"
        }
    }

    struct MockSynchronizer {
        results: VecDeque<FDv2SourceResult>,
        selectors_seen: Selectors,
        hang: bool,
    }

    impl Synchronizer for MockSynchronizer {
        fn next(&mut self, selector: Selector) -> BoxFuture<'_, FDv2SourceEvent> {
            self.selectors_seen.lock().unwrap().push(selector);
            if self.hang {
                return Box::pin(std::future::pending());
            }
            // Once scripted results run out, end the run instead of spinning.
            let result = self
                .results
                .pop_front()
                .unwrap_or(FDv2SourceResult::Shutdown);
            Box::pin(async move { event(result) })
        }

        fn name(&self) -> &str {
            "mock-synchronizer"
        }
    }

    struct MockInitializerFactory {
        results: Mutex<Vec<FDv2SourceResult>>,
    }

    impl InitializerFactory for MockInitializerFactory {
        fn create(&self) -> Box<dyn Initializer> {
            let results = std::mem::take(&mut *self.results.lock().unwrap());
            Box::new(MockInitializer {
                results: results.into(),
            })
        }
    }

    struct MockSynchronizerFactory {
        results: Mutex<Vec<FDv2SourceResult>>,
        selectors_seen: Selectors,
    }

    impl SynchronizerFactory for MockSynchronizerFactory {
        fn create(&self) -> Box<dyn Synchronizer> {
            let results = std::mem::take(&mut *self.results.lock().unwrap());
            Box::new(MockSynchronizer {
                results: results.into(),
                selectors_seen: self.selectors_seen.clone(),
                hang: false,
            })
        }
    }

    fn recording_init_complete() -> (Arc<dyn Fn(bool) + Send + Sync>, InitCalls) {
        let calls: InitCalls = Arc::new(Mutex::new(Vec::new()));
        let sink = calls.clone();
        let cb: Arc<dyn Fn(bool) + Send + Sync> =
            Arc::new(move |success| sink.lock().unwrap().push(success));
        (cb, calls)
    }

    #[tokio::test]
    async fn start_applies_basis_and_exposes_it_via_store_handle() {
        // A data system whose sole initializer yields one full basis.
        let system = FDv2DataSystem::new(
            vec![Box::new(MockInitializerFactory {
                results: Mutex::new(vec![changeset(
                    ChangeSetKind::Full,
                    "f1",
                    Some("s1".into()),
                )]),
            })],
            vec![Box::new(MockSynchronizerFactory {
                results: Mutex::new(vec![]),
                selectors_seen: Arc::new(Mutex::new(Vec::new())),
            })],
        );

        // Record init-complete calls and wake the test when one arrives.
        let calls: InitCalls = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(tokio::sync::Notify::new());
        let sink = calls.clone();
        let waker = notify.clone();
        let init_complete: Arc<dyn Fn(bool) + Send + Sync> = Arc::new(move |success| {
            sink.lock().unwrap().push(success);
            waker.notify_one();
        });
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Start the system and wait for initialization to finish.
        system.start(init_complete, shutdown_rx);
        notify.notified().await;

        // The basis was applied and is readable through the store() handle.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(system.store().read().flag("f1").is_some());
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn initializer_basis_signals_once_and_propagates_selector() {
        // Store and recorders shared with the mock sources.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let selectors_seen: Selectors = Arc::new(Mutex::new(Vec::new()));
        let (init_complete, calls) = recording_init_complete();

        // Initializer delivers a full basis carrying selector "sel-1".
        let initializers: Vec<Box<dyn Initializer>> = vec![Box::new(MockInitializer {
            results: VecDeque::from(vec![changeset(
                ChangeSetKind::Full,
                "init-flag",
                Some("sel-1".into()),
            )]),
        })];

        // Synchronizer delivers a partial change carrying selector "sel-2".
        let synchronizers: Vec<Box<dyn Synchronizer>> = vec![Box::new(MockSynchronizer {
            results: VecDeque::from(vec![changeset(
                ChangeSetKind::Partial,
                "sync-flag",
                Some("sel-2".into()),
            )]),
            selectors_seen: selectors_seen.clone(),
            hang: false,
        })];
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            synchronizers,
            store.clone(),
            init_complete,
            shutdown_rx,
        )
        .await;

        // init_complete fired exactly once despite two successful applies.
        assert_eq!(*calls.lock().unwrap(), vec![true]);

        // Both the basis flag and the later partial change are in the store.
        assert!(store.read().flag("init-flag").is_some());
        assert!(store.read().flag("sync-flag").is_some());

        // The synchronizer's first call received the basis selector.
        assert_eq!(selectors_seen.lock().unwrap()[0], Some("sel-1".into()));
    }

    #[tokio::test]
    async fn failed_initializers_let_synchronizer_provide_the_basis() {
        // Store and init-complete recorder.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();

        // Both initializers fail without producing a basis.
        let initializers: Vec<Box<dyn Initializer>> = vec![
            Box::new(MockInitializer {
                results: VecDeque::from(vec![interrupted()]),
            }),
            Box::new(MockInitializer {
                results: VecDeque::from(vec![terminal()]),
            }),
        ];

        // The synchronizer then delivers the basis.
        let synchronizers: Vec<Box<dyn Synchronizer>> = vec![Box::new(MockSynchronizer {
            results: VecDeque::from(vec![changeset(
                ChangeSetKind::Full,
                "sync-flag",
                Some("s".into()),
            )]),
            selectors_seen: Arc::new(Mutex::new(Vec::new())),
            hang: false,
        })];
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            synchronizers,
            store.clone(),
            init_complete,
            shutdown_rx,
        )
        .await;

        // Initialization succeeded via the synchronizer.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("sync-flag").is_some());
    }

    #[tokio::test]
    async fn exhausting_all_sources_signals_failure_once() {
        // Store and init-complete recorder.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();

        // The initializer fails.
        let initializers: Vec<Box<dyn Initializer>> = vec![Box::new(MockInitializer {
            results: VecDeque::from(vec![terminal()]),
        })];

        // The synchronizer only ever reports an interruption, never a basis.
        let synchronizers: Vec<Box<dyn Synchronizer>> = vec![Box::new(MockSynchronizer {
            results: VecDeque::from(vec![interrupted()]),
            selectors_seen: Arc::new(Mutex::new(Vec::new())),
            hang: false,
        })];
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            synchronizers,
            store,
            init_complete,
            shutdown_rx,
        )
        .await;

        // Failure was reported exactly once.
        assert_eq!(*calls.lock().unwrap(), vec![false]);
    }

    #[tokio::test]
    async fn synchronizer_terminal_error_advances_to_next() {
        // Store and init-complete recorder; no initializers.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializers: Vec<Box<dyn Initializer>> = vec![];

        // First synchronizer fails terminally; the second provides the basis.
        let synchronizers: Vec<Box<dyn Synchronizer>> = vec![
            Box::new(MockSynchronizer {
                results: VecDeque::from(vec![terminal()]),
                selectors_seen: Arc::new(Mutex::new(Vec::new())),
                hang: false,
            }),
            Box::new(MockSynchronizer {
                results: VecDeque::from(vec![changeset(
                    ChangeSetKind::Full,
                    "from-second",
                    Some("s".into()),
                )]),
                selectors_seen: Arc::new(Mutex::new(Vec::new())),
                hang: false,
            }),
        ];
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            synchronizers,
            store.clone(),
            init_complete,
            shutdown_rx,
        )
        .await;

        // The run advanced past the terminal source and applied the second's basis.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("from-second").is_some());
    }

    #[tokio::test]
    async fn synchronizer_interrupted_retries_same_source() {
        // Store and init-complete recorder; no initializers.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializers: Vec<Box<dyn Initializer>> = vec![];

        // A single synchronizer: an interruption, then a basis on the retry.
        let synchronizers: Vec<Box<dyn Synchronizer>> = vec![Box::new(MockSynchronizer {
            results: VecDeque::from(vec![
                interrupted(),
                changeset(ChangeSetKind::Full, "after-retry", Some("s".into())),
            ]),
            selectors_seen: Arc::new(Mutex::new(Vec::new())),
            hang: false,
        })];
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            synchronizers,
            store.clone(),
            init_complete,
            shutdown_rx,
        )
        .await;

        // The retry on the same synchronizer delivered the basis.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("after-retry").is_some());
    }

    #[tokio::test]
    async fn shutdown_ends_run_without_signaling() {
        // Store and init-complete recorder; no initializers.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializers: Vec<Box<dyn Initializer>> = vec![];

        // A synchronizer whose next() never resolves.
        let synchronizers: Vec<Box<dyn Synchronizer>> = vec![Box::new(MockSynchronizer {
            results: VecDeque::new(),
            selectors_seen: Arc::new(Mutex::new(Vec::new())),
            hang: true,
        })];
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Drive the run on a task, then signal shutdown.
        let handle = tokio::spawn(run(
            initializers,
            synchronizers,
            store,
            init_complete,
            shutdown_rx,
        ));
        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        // The run returned before any initialization signal.
        assert!(calls.lock().unwrap().is_empty());
    }
}
