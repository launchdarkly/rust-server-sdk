use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::time::{sleep_until, Instant};

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
    synchronizer_factories: Vec<Arc<dyn SynchronizerFactory>>,
    store: Arc<RwLock<InMemoryDataStore>>,
}

impl FDv2DataSystem {
    pub(crate) fn new(
        initializer_factories: Vec<Box<dyn InitializerFactory>>,
        synchronizer_factories: Vec<Arc<dyn SynchronizerFactory>>,
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
        let source_manager = SourceManager::new(self.synchronizer_factories.clone());
        let store = self.store.clone();

        tokio::spawn(run(
            initializers,
            source_manager,
            store,
            init_complete,
            shutdown_receiver,
        ));
    }

    fn store(&self) -> Arc<RwLock<dyn DataStore>> {
        self.store.clone()
    }
}

/// Per-factory availability used by the synchronizer rotation.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SourceState {
    Available,
    Blocked,
}

/// Owns the synchronizer factories and tracks which one is currently active.
struct SourceManager {
    factories: Vec<Arc<dyn SynchronizerFactory>>,
    states: Vec<SourceState>,
    /// Iteration cursor; `None` restarts the search from the prime.
    synchronizer_index: Option<usize>,
    /// Index of the most recently returned factory, for blocking and prime checks.
    current_factory_index: Option<usize>,
}

impl SourceManager {
    fn new(factories: Vec<Arc<dyn SynchronizerFactory>>) -> Self {
        let states = vec![SourceState::Available; factories.len()];
        Self {
            factories,
            states,
            synchronizer_index: None,
            current_factory_index: None,
        }
    }

    /// Builds the next available synchronizer, advancing cyclically past the
    /// active one and skipping blocked factories. `None` when all are blocked.
    fn next_synchronizer(&mut self) -> Option<Box<dyn Synchronizer>> {
        let n = self.factories.len();
        if n == 0 {
            self.current_factory_index = None;
            return None;
        }
        let mut i = self.synchronizer_index.map_or(0, |c| (c + 1) % n);
        for _ in 0..n {
            if self.states[i] == SourceState::Available {
                self.synchronizer_index = Some(i);
                self.current_factory_index = Some(i);
                return Some(self.factories[i].create());
            }
            i = (i + 1) % n;
        }
        self.current_factory_index = None;
        None
    }

    /// Marks the active factory blocked, used on a terminal error.
    fn block_current(&mut self) {
        if let Some(i) = self.current_factory_index {
            self.states[i] = SourceState::Blocked;
        }
    }

    /// Makes the next `next_synchronizer` restart from the prime, used on recovery.
    fn reset_source_index(&mut self) {
        self.synchronizer_index = None;
    }

    /// Whether the active factory is the first available one.
    fn is_prime(&self) -> bool {
        let first = self
            .states
            .iter()
            .position(|s| *s == SourceState::Available);
        matches!((first, self.current_factory_index), (Some(f), Some(c)) if f == c)
    }

    fn available_count(&self) -> usize {
        self.states
            .iter()
            .filter(|s| **s == SourceState::Available)
            .count()
    }
}

const FALLBACK_TIMEOUT: Duration = Duration::from_secs(120);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);

/// Sleeps until `at`, or never when `None` (an inactive timer arm).
async fn deadline(at: Option<Instant>) {
    match at {
        Some(t) => sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

async fn run(
    initializers: Vec<Box<dyn Initializer>>,
    mut source_manager: SourceManager,
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

    // Synchronizer phase: rotate through synchronizers as the timers fire.
    let mut current = source_manager.next_synchronizer();
    while let Some(mut active) = current {
        let has_fallback = source_manager.available_count() > 1;
        let has_recovery = has_fallback && !source_manager.is_prime();
        let mut fallback_at: Option<Instant> = None;
        let recovery_at = has_recovery.then(|| Instant::now() + RECOVERY_TIMEOUT);
        let mut interrupted_logged = false;

        // Drive the active synchronizer, racing its events against the timers.
        loop {
            let mut shutdown = Box::pin(shutdown_receiver.recv()).fuse();
            let mut fallback = Box::pin(deadline(fallback_at)).fuse();
            let mut recovery = Box::pin(deadline(recovery_at)).fuse();
            let mut next = active.next(selector.clone()).fuse();
            futures::select! {
                _ = shutdown => return,
                // Fall back to the next synchronizer.
                _ = fallback => break,
                // Recover to the prime.
                _ = recovery => {
                    source_manager.reset_source_index();
                    break;
                }
                event = next => match event.result {
                    FDv2SourceResult::ChangeSet(change_set) => {
                        selector = change_set.selector.clone();
                        store.write().apply(change_set);
                        if !initialized {
                            init_complete(true);
                            initialized = true;
                        }
                        // Good data clears the fallback countdown.
                        fallback_at = None;
                        interrupted_logged = false;
                    }
                    // Sustained interruption starts the fallback countdown.
                    FDv2SourceResult::Interrupted(error) => {
                        if !interrupted_logged {
                            info!("FDv2 synchronizer interrupted: {}", error.message);
                            interrupted_logged = true;
                        }
                        if has_fallback && fallback_at.is_none() {
                            fallback_at = Some(Instant::now() + FALLBACK_TIMEOUT);
                        }
                    }
                    // Handled internally by the synchronizer.
                    FDv2SourceResult::Goodbye { .. } => {}
                    FDv2SourceResult::TerminalError(error) => {
                        warn!("FDv2 synchronizer terminal error: {}", error.message);
                        // Dead source: drop it and advance.
                        source_manager.block_current();
                        break;
                    }
                    FDv2SourceResult::Shutdown => return,
                },
            }
        }

        current = source_manager.next_synchronizer();
    }

    // Every source blocked without ever obtaining a basis.
    if !initialized {
        init_complete(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
            match self.results.pop_front() {
                Some(result) => Box::pin(async move { event(result) }),
                // Out of scripted results: idle if hang, otherwise end the run.
                None if self.hang => Box::pin(std::future::pending()),
                None => Box::pin(async move { event(FDv2SourceResult::Shutdown) }),
            }
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
        hang: bool,
    }

    impl SynchronizerFactory for MockSynchronizerFactory {
        fn create(&self) -> Box<dyn Synchronizer> {
            let results = std::mem::take(&mut *self.results.lock().unwrap());
            Box::new(MockSynchronizer {
                results: results.into(),
                selectors_seen: self.selectors_seen.clone(),
                hang: self.hang,
            })
        }
    }

    /// A selector recorder that ignores what it captures.
    fn no_selectors() -> Selectors {
        Arc::new(Mutex::new(Vec::new()))
    }

    /// A single-synchronizer factory scripted with the given results.
    fn sync_factory(
        results: Vec<FDv2SourceResult>,
        selectors_seen: Selectors,
        hang: bool,
    ) -> Arc<dyn SynchronizerFactory> {
        Arc::new(MockSynchronizerFactory {
            results: Mutex::new(results),
            selectors_seen,
            hang,
        })
    }

    /// A prime factory that is down on first build and recovers on rebuild.
    struct RecoveringPrimeFactory {
        builds: AtomicUsize,
    }

    impl SynchronizerFactory for RecoveringPrimeFactory {
        fn create(&self) -> Box<dyn Synchronizer> {
            let (results, hang) = if self.builds.fetch_add(1, Ordering::SeqCst) == 0 {
                // Down: interrupt, then idle so the fallback timer fires.
                (vec![interrupted()], true)
            } else {
                // Recovered: deliver a delta once the prime is active again.
                (
                    vec![changeset(
                        ChangeSetKind::Partial,
                        "prime-recovered",
                        Some("s".into()),
                    )],
                    false,
                )
            };
            Box::new(MockSynchronizer {
                results: results.into(),
                selectors_seen: no_selectors(),
                hang,
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
            vec![sync_factory(vec![], no_selectors(), false)],
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
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![changeset(
                ChangeSetKind::Partial,
                "sync-flag",
                Some("sel-2".into()),
            )],
            selectors_seen.clone(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            source_manager,
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
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![changeset(
                ChangeSetKind::Full,
                "sync-flag",
                Some("s".into()),
            )],
            no_selectors(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            source_manager,
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

        // The synchronizer reports an interruption, then fails terminally.
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![interrupted(), terminal()],
            no_selectors(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            source_manager,
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
        let source_manager = SourceManager::new(vec![
            sync_factory(vec![terminal()], no_selectors(), false),
            sync_factory(
                vec![changeset(
                    ChangeSetKind::Full,
                    "from-second",
                    Some("s".into()),
                )],
                no_selectors(),
                false,
            ),
        ]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            source_manager,
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
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![
                interrupted(),
                changeset(ChangeSetKind::Full, "after-retry", Some("s".into())),
            ],
            no_selectors(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializers,
            source_manager,
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
        let source_manager = SourceManager::new(vec![sync_factory(vec![], no_selectors(), true)]);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Drive the run on a task, then signal shutdown.
        let handle = tokio::spawn(run(
            initializers,
            source_manager,
            store,
            init_complete,
            shutdown_rx,
        ));
        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        // The run returned before any initialization signal.
        assert!(calls.lock().unwrap().is_empty());
    }

    #[test]
    fn rotates_cyclically_skips_blocked_and_exhausts() {
        let mut sources = SourceManager::new(vec![
            sync_factory(vec![], no_selectors(), false),
            sync_factory(vec![], no_selectors(), false),
            sync_factory(vec![], no_selectors(), false),
        ]);

        // Advances cyclically from the prime.
        sources.next_synchronizer();
        assert_eq!(sources.current_factory_index, Some(0));
        sources.next_synchronizer();
        assert_eq!(sources.current_factory_index, Some(1));

        // Blocking the active factory drops it from the rotation.
        sources.block_current();
        sources.next_synchronizer();
        assert_eq!(sources.current_factory_index, Some(2));

        // The next pass wraps around and skips the blocked factory.
        sources.next_synchronizer();
        assert_eq!(sources.current_factory_index, Some(0));

        // With every factory blocked there is nothing left to return.
        sources.block_current();
        sources.next_synchronizer();
        sources.block_current();
        assert!(sources.next_synchronizer().is_none());
    }

    #[test]
    fn reset_source_index_returns_to_prime() {
        let mut sources = SourceManager::new(vec![
            sync_factory(vec![], no_selectors(), false),
            sync_factory(vec![], no_selectors(), false),
        ]);

        sources.next_synchronizer();
        sources.next_synchronizer();
        assert_eq!(sources.current_factory_index, Some(1));

        // Recovery rewinds so the next search starts from the prime.
        sources.reset_source_index();
        sources.next_synchronizer();
        assert_eq!(sources.current_factory_index, Some(0));
    }

    #[test]
    fn is_prime_and_available_count_track_state() {
        let mut sources = SourceManager::new(vec![
            sync_factory(vec![], no_selectors(), false),
            sync_factory(vec![], no_selectors(), false),
        ]);
        assert_eq!(sources.available_count(), 2);

        sources.next_synchronizer();
        assert!(sources.is_prime());
        sources.next_synchronizer();
        assert!(!sources.is_prime());

        sources.block_current();
        assert_eq!(sources.available_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_fires_after_sustained_interruption() {
        // No initializers; the prime interrupts then idles.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializers: Vec<Box<dyn Initializer>> = vec![];

        // Prime only ever interrupts; the fallback stands by with a basis.
        let source_manager = SourceManager::new(vec![
            sync_factory(vec![interrupted()], no_selectors(), true),
            sync_factory(
                vec![changeset(
                    ChangeSetKind::Full,
                    "from-fallback",
                    Some("s".into()),
                )],
                no_selectors(),
                false,
            ),
        ]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Paused time auto-advances past the fallback timeout while the prime idles.
        let handle = tokio::spawn(run(
            initializers,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
        ));
        handle.await.unwrap();

        // The run fell back to the second synchronizer and applied its basis.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("from-fallback").is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn changeset_cancels_the_fallback_timer() {
        // Notify on init so the test can act once the prime's basis lands.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let calls: InitCalls = Arc::new(Mutex::new(Vec::new()));
        let notify = Arc::new(tokio::sync::Notify::new());
        let sink = calls.clone();
        let waker = notify.clone();
        let init_complete: Arc<dyn Fn(bool) + Send + Sync> = Arc::new(move |success| {
            sink.lock().unwrap().push(success);
            waker.notify_one();
        });
        let initializers: Vec<Box<dyn Initializer>> = vec![];

        // Prime interrupts (arming the timer), delivers a basis (clearing it), then idles.
        let source_manager = SourceManager::new(vec![
            sync_factory(
                vec![
                    interrupted(),
                    changeset(ChangeSetKind::Full, "from-prime", Some("s".into())),
                ],
                no_selectors(),
                true,
            ),
            sync_factory(
                vec![changeset(
                    ChangeSetKind::Full,
                    "from-fallback",
                    Some("s".into()),
                )],
                no_selectors(),
                false,
            ),
        ]);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let handle = tokio::spawn(run(
            initializers,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
        ));

        // Wait for the basis, let well past the fallback timeout elapse, then stop.
        notify.notified().await;
        tokio::time::advance(FALLBACK_TIMEOUT * 2).await;
        shutdown_tx.send(()).unwrap();
        handle.await.unwrap();

        // The basis kept the run on the prime; it never fell back.
        assert!(store.read().flag("from-prime").is_some());
        assert!(store.read().flag("from-fallback").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_fires_and_returns_to_the_prime() {
        // No initializers; the prime is down, so the run falls back then recovers.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializers: Vec<Box<dyn Initializer>> = vec![];

        // Prime recovers on rebuild; the fallback supplies a basis then idles.
        let source_manager = SourceManager::new(vec![
            Arc::new(RecoveringPrimeFactory {
                builds: AtomicUsize::new(0),
            }) as Arc<dyn SynchronizerFactory>,
            sync_factory(
                vec![changeset(
                    ChangeSetKind::Full,
                    "from-fallback",
                    Some("s".into()),
                )],
                no_selectors(),
                true,
            ),
        ]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Paused time auto-advances through the fallback then recovery timeouts.
        let handle = tokio::spawn(run(
            initializers,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
        ));
        handle.await.unwrap();

        // Fell back to the fallback's basis, then recovered to the prime's delta.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("from-fallback").is_some());
        assert!(store.read().flag("prime-recovered").is_some());
    }
}
