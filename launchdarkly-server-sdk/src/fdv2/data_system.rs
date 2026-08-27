use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::time::{sleep_until, Instant};

use crate::data_system::DataSystem;
use crate::stores::store::{DataStore, InMemoryDataStore, TransactionalDataStore};

use super::model::{ChangeSetKind, Selector};
use super::source::{FDv2SourceEvent, FDv2SourceResult, Initializer, Synchronizer};

/// Produces a fresh initializer each time the orchestrator starts a run.
pub(crate) trait InitializerFactory: Send + Sync {
    fn create(&self) -> Box<dyn Initializer>;
}

/// Produces a fresh synchronizer each time the orchestrator starts a run.
pub(crate) trait SynchronizerFactory: Send + Sync {
    fn create(&self) -> Box<dyn Synchronizer>;

    /// Whether this factory builds the FDv1 fallback synchronizer.
    fn is_fdv1_fallback(&self) -> bool {
        false
    }
}

/// FDv2 orchestrator: owns the memory store and keeps it populated by running
/// initializers to obtain a basis, then synchronizers for ongoing changes.
pub(crate) struct FDv2DataSystem {
    initializer_factories: Vec<Arc<dyn InitializerFactory>>,
    synchronizer_factories: Vec<Arc<dyn SynchronizerFactory>>,
    fallback_timeout: Duration,
    recovery_timeout: Duration,
    store: Arc<RwLock<InMemoryDataStore>>,
}

impl FDv2DataSystem {
    pub(crate) fn new(
        initializer_factories: Vec<Arc<dyn InitializerFactory>>,
        synchronizer_factories: Vec<Arc<dyn SynchronizerFactory>>,
        fallback_timeout: Duration,
        recovery_timeout: Duration,
    ) -> Self {
        Self {
            initializer_factories,
            synchronizer_factories,
            fallback_timeout,
            recovery_timeout,
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
        let initializer_factories = self.initializer_factories.clone();
        let source_manager = SourceManager::new(self.synchronizer_factories.clone());
        let store = self.store.clone();

        tokio::spawn(run(
            initializer_factories,
            source_manager,
            store,
            init_complete,
            shutdown_receiver,
            self.fallback_timeout,
            self.recovery_timeout,
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
        // FDv1 fallback factories start blocked; they activate only on a directive.
        let states = factories
            .iter()
            .map(|f| {
                if f.is_fdv1_fallback() {
                    SourceState::Blocked
                } else {
                    SourceState::Available
                }
            })
            .collect();
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
        first == self.current_factory_index && first.is_some()
    }

    fn available_count(&self) -> usize {
        self.states
            .iter()
            .filter(|s| **s == SourceState::Available)
            .count()
    }

    /// Blocks the FDv2 synchronizers and activates the FDv1 fallback, if any.
    fn switch_to_fdv1_fallback(&mut self) {
        for (i, factory) in self.factories.iter().enumerate() {
            self.states[i] = if factory.is_fdv1_fallback() {
                SourceState::Available
            } else {
                SourceState::Blocked
            };
        }
        self.synchronizer_index = None;
    }

    /// Restores the initial state: FDv2 available, FDv1 fallback blocked.
    fn switch_back_to_fdv2(&mut self) {
        for (i, factory) in self.factories.iter().enumerate() {
            self.states[i] = if factory.is_fdv1_fallback() {
                SourceState::Blocked
            } else {
                SourceState::Available
            };
        }
        self.synchronizer_index = None;
    }

    /// Whether the active factory is the FDv1 fallback.
    fn is_current_fdv1_fallback(&self) -> bool {
        self.current_factory_index
            .is_some_and(|i| self.factories[i].is_fdv1_fallback())
    }
}

/// Sleeps until `at`, or never when `None` (an inactive timer arm).
async fn deadline(at: Option<Instant>) {
    match at {
        Some(t) => sleep_until(t).await,
        None => std::future::pending::<()>().await,
    }
}

async fn run(
    initializer_factories: Vec<Arc<dyn InitializerFactory>>,
    mut source_manager: SourceManager,
    store: Arc<RwLock<InMemoryDataStore>>,
    init_complete: Arc<dyn Fn(bool) + Send + Sync>,
    mut shutdown_receiver: broadcast::Receiver<()>,
    fallback_timeout: Duration,
    recovery_timeout: Duration,
) {
    let mut selector: Selector = None;
    let mut initialized = false;
    // Whether an initializer produced a full payload.
    let mut got_full = false;
    let mut fdv2_retry_at: Option<Instant> = None;

    // Initializer phase: try each until one yields a basis or an FDv1 fallback
    // directive.
    for factory in initializer_factories {
        let mut initializer = factory.create();
        let name = initializer.name().to_string();
        let mut shutdown = Box::pin(shutdown_receiver.recv()).fuse();
        let event = futures::select! {
            _ = shutdown => return,
            event = initializer.run().fuse() => event,
        };

        let FDv2SourceEvent {
            result,
            fdv1_fallback,
        } = event;
        let mut has_basis = false;
        match result {
            FDv2SourceResult::ChangeSet(change_set) => {
                let is_full = matches!(change_set.kind, ChangeSetKind::Full);
                has_basis = is_full && change_set.selector.is_some();
                if !matches!(change_set.kind, ChangeSetKind::None) {
                    selector = change_set.selector.clone();
                }
                store.write().apply(change_set);
                if is_full {
                    got_full = true;
                }
            }
            _ => debug!("{name} did not provide a basis"),
        }
        // An FDv1 fallback directive ends the initializer phase and switches
        // to the fallback.
        if let Some(fallback_directive) = fdv1_fallback {
            info!("FDv2 falling back to the FDv1 protocol");
            source_manager.switch_to_fdv1_fallback();
            fdv2_retry_at = Some(Instant::now() + fallback_directive.ttl);
            break;
        }
        if has_basis {
            break;
        }
    }

    if got_full && !initialized {
        init_complete(true);
        initialized = true;
    }

    // Synchronizer phase: rotate through synchronizers as the timers fire.
    let mut current = source_manager.next_synchronizer();
    loop {
        let mut active = match current {
            Some(active) => active,
            None => {
                // No synchronizer is available. While an FDv1 fallback directive's
                // retry is pending, wait it out and return to FDv2 rather than exiting
                // -- a blocked or terminal fallback must not strand the data system.
                // With no retry pending, every source hit an unrecoverable error, so stop.
                if fdv2_retry_at.is_none() {
                    break;
                }
                let mut shutdown = Box::pin(shutdown_receiver.recv()).fuse();
                let mut fdv2_retry = Box::pin(deadline(fdv2_retry_at)).fuse();
                futures::select! {
                    _ = shutdown => return,
                    _ = fdv2_retry => {
                        source_manager.switch_back_to_fdv2();
                        fdv2_retry_at = None;
                    }
                }
                current = source_manager.next_synchronizer();
                continue;
            }
        };

        let name = active.name().to_string();
        let has_fallback = source_manager.available_count() > 1;
        let has_recovery = has_fallback && !source_manager.is_prime();
        let mut fallback_at: Option<Instant> = None;
        let recovery_at = has_recovery.then(|| Instant::now() + recovery_timeout);
        let mut interrupted_logged = false;

        // Drive the active synchronizer, racing its events against the timers.
        loop {
            let mut shutdown = Box::pin(shutdown_receiver.recv()).fuse();
            let mut fallback = Box::pin(deadline(fallback_at)).fuse();
            let mut recovery = Box::pin(deadline(recovery_at)).fuse();
            let mut fdv2_retry = Box::pin(deadline(fdv2_retry_at)).fuse();
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
                // Re-engage FDv2 once the fallback directive's TTL expires.
                _ = fdv2_retry => {
                    source_manager.switch_back_to_fdv2();
                    fdv2_retry_at = None;
                    break;
                }
                event = next => {
                    let FDv2SourceEvent { result, fdv1_fallback } = event;
                    let mut terminal = false;
                    match result {
                        FDv2SourceResult::ChangeSet(change_set) => {
                            let is_full = matches!(change_set.kind, ChangeSetKind::Full);
                            if !matches!(change_set.kind, ChangeSetKind::None) {
                                selector = change_set.selector.clone();
                                store.write().apply(change_set);
                                if is_full && !initialized {
                                    init_complete(true);
                                    initialized = true;
                                }
                            }
                            // A successful response clears the countdown.
                            fallback_at = None;
                            interrupted_logged = false;
                        }
                        // Sustained interruption starts the fallback countdown.
                        FDv2SourceResult::Interrupted(error) => {
                            if !interrupted_logged {
                                info!("{name} interrupted: {}", error.message);
                                interrupted_logged = true;
                            }
                            if has_fallback && fallback_at.is_none() {
                                fallback_at = Some(Instant::now() + fallback_timeout);
                            }
                        }
                        // Handled internally by the synchronizer.
                        FDv2SourceResult::Goodbye => {}
                        FDv2SourceResult::TerminalError(error) => {
                            warn!("{name} terminal error: {}", error.message);
                            terminal = true;
                        }
                        FDv2SourceResult::Shutdown => return,
                    }
                    // An FDv1 fallback directive takes precedence over a terminal error.
                    if let Some(fallback_directive) = fdv1_fallback {
                        if !source_manager.is_current_fdv1_fallback() {
                            info!("FDv2 falling back to the FDv1 protocol");
                            source_manager.switch_to_fdv1_fallback();
                            fdv2_retry_at = Some(Instant::now() + fallback_directive.ttl);
                            break;
                        }
                    }
                    if terminal {
                        // Dead source: drop it and advance.
                        source_manager.block_current();
                        break;
                    }
                }
            }
        }

        current = source_manager.next_synchronizer();
    }

    // Every source blocked without ever obtaining a full payload.
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
    use super::super::source::{ErrorInfo, ErrorKind, FDv1FallbackDirective, FDv2SourceEvent};
    use crate::stores::change_set::{ChangeSet, ItemChange};
    use crate::stores::store_types::StorageItem;
    use crate::test_common::basic_flag;

    const FALLBACK_TIMEOUT: Duration = Duration::from_secs(120);
    const RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);

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

    /// An initializer that reports an FDv1 fallback directive with the given TTL.
    struct FallbackInitializer {
        ttl: Duration,
    }

    impl Initializer for FallbackInitializer {
        fn run(&mut self) -> BoxFuture<'_, FDv2SourceEvent> {
            let ttl = self.ttl;
            Box::pin(async move {
                FDv2SourceEvent {
                    result: interrupted(),
                    fdv1_fallback: Some(FDv1FallbackDirective { ttl }),
                }
            })
        }

        fn name(&self) -> &str {
            "fallback-initializer"
        }
    }

    struct FallbackInitializerFactory {
        ttl: Duration,
    }

    impl InitializerFactory for FallbackInitializerFactory {
        fn create(&self) -> Box<dyn Initializer> {
            Box::new(FallbackInitializer { ttl: self.ttl })
        }
    }

    struct MockSynchronizer {
        results: VecDeque<FDv2SourceResult>,
        selectors_seen: Selectors,
        hang: bool,
        fallback_directive: Option<FDv1FallbackDirective>,
    }

    impl Synchronizer for MockSynchronizer {
        fn next(&mut self, selector: Selector) -> BoxFuture<'_, FDv2SourceEvent> {
            self.selectors_seen.lock().unwrap().push(selector);
            match self.results.pop_front() {
                Some(result) => {
                    let fdv1_fallback = self.fallback_directive.clone();
                    Box::pin(async move {
                        FDv2SourceEvent {
                            result,
                            fdv1_fallback,
                        }
                    })
                }
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

    /// A single-initializer factory scripted with the given results.
    fn init_factory(results: Vec<FDv2SourceResult>) -> Arc<dyn InitializerFactory> {
        Arc::new(MockInitializerFactory {
            results: Mutex::new(results),
        })
    }

    struct MockSynchronizerFactory {
        results: Mutex<Vec<FDv2SourceResult>>,
        selectors_seen: Selectors,
        hang: bool,
        is_fdv1_fallback: bool,
        fallback_directive: Option<FDv1FallbackDirective>,
    }

    impl SynchronizerFactory for MockSynchronizerFactory {
        fn create(&self) -> Box<dyn Synchronizer> {
            let results = std::mem::take(&mut *self.results.lock().unwrap());
            Box::new(MockSynchronizer {
                results: results.into(),
                selectors_seen: self.selectors_seen.clone(),
                hang: self.hang,
                fallback_directive: self.fallback_directive.clone(),
            })
        }

        fn is_fdv1_fallback(&self) -> bool {
            self.is_fdv1_fallback
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
            is_fdv1_fallback: false,
            fallback_directive: None,
        })
    }

    /// Like `sync_factory`, but flagged as the FDv1 fallback.
    fn fdv1_factory(
        results: Vec<FDv2SourceResult>,
        selectors_seen: Selectors,
        hang: bool,
    ) -> Arc<dyn SynchronizerFactory> {
        Arc::new(MockSynchronizerFactory {
            results: Mutex::new(results),
            selectors_seen,
            hang,
            is_fdv1_fallback: true,
            fallback_directive: None,
        })
    }

    /// An FDv2 synchronizer that reports an FDv1 fallback directive.
    fn fallback_directive_factory(ttl: Duration) -> Arc<dyn SynchronizerFactory> {
        Arc::new(MockSynchronizerFactory {
            results: Mutex::new(vec![interrupted()]),
            selectors_seen: no_selectors(),
            hang: true,
            is_fdv1_fallback: false,
            fallback_directive: Some(FDv1FallbackDirective { ttl }),
        })
    }

    /// A factory that is down on its first build and delivers data on rebuild.
    struct DownThenDataFactory {
        builds: AtomicUsize,
    }

    impl SynchronizerFactory for DownThenDataFactory {
        fn create(&self) -> Box<dyn Synchronizer> {
            let (results, hang) = if self.builds.fetch_add(1, Ordering::SeqCst) == 0 {
                // Down: interrupt, then idle so the fallback timer fires.
                (vec![interrupted()], true)
            } else {
                // Rebuilt: deliver a delta.
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
                fallback_directive: None,
            })
        }
    }

    /// Reports an FDv1 fallback directive on its first build, then delivers data.
    struct FallbackThenDataFactory {
        ttl: Duration,
        reengage_kind: ChangeSetKind,
        builds: AtomicUsize,
    }

    impl SynchronizerFactory for FallbackThenDataFactory {
        fn create(&self) -> Box<dyn Synchronizer> {
            if self.builds.fetch_add(1, Ordering::SeqCst) == 0 {
                // First: report the fallback directive, then idle.
                Box::new(MockSynchronizer {
                    results: VecDeque::from(vec![interrupted()]),
                    selectors_seen: no_selectors(),
                    hang: true,
                    fallback_directive: Some(FDv1FallbackDirective { ttl: self.ttl }),
                })
            } else {
                // After the FDv2 retry: deliver the re-engagement payload.
                Box::new(MockSynchronizer {
                    results: VecDeque::from(vec![changeset(
                        self.reengage_kind,
                        "fdv2-back",
                        Some("s".into()),
                    )]),
                    selectors_seen: no_selectors(),
                    hang: false,
                    fallback_directive: None,
                })
            }
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
            vec![init_factory(vec![changeset(
                ChangeSetKind::Full,
                "f1",
                Some("s1".into()),
            )])],
            vec![sync_factory(vec![], no_selectors(), false)],
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> =
            vec![init_factory(vec![changeset(
                ChangeSetKind::Full,
                "init-flag",
                Some("sel-1".into()),
            )])];

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
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
    async fn selectorless_full_continues_to_next_initializer() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let selectors_seen: Selectors = Arc::new(Mutex::new(Vec::new()));
        let (init_complete, calls) = recording_init_complete();

        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![
            // The first initializer delivers a full payload with no selector.
            init_factory(vec![changeset(ChangeSetKind::Full, "no-basis-flag", None)]),
            // The second delivers a delta carrying a selector, so it merges over the first.
            init_factory(vec![changeset(
                ChangeSetKind::Partial,
                "merged-flag",
                Some("sel-2".into()),
            )]),
        ];

        // The synchronizer only records the selector it is started with.
        let source_manager =
            SourceManager::new(vec![sync_factory(vec![], selectors_seen.clone(), false)]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // The selector-less payload survives and the delta merged over it.
        assert!(store.read().flag("no-basis-flag").is_some());
        assert!(store.read().flag("merged-flag").is_some());

        // The selector-less payload did not stop the initializers, so the synchronizer
        // starts from the second initializer's selector.
        assert_eq!(selectors_seen.lock().unwrap()[0], Some("sel-2".into()));

        // init_complete fired exactly once.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
    }

    #[tokio::test]
    async fn initializer_selectorless_full_defers_signal() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));

        // Capture whether the second initializer's flag is present when the signal fires.
        let flag_at_signal = Arc::new(Mutex::new(None));
        let probe = store.clone();
        let sink = flag_at_signal.clone();
        let init_complete: Arc<dyn Fn(bool) + Send + Sync> = Arc::new(move |_| {
            *sink.lock().unwrap() = Some(probe.read().flag("from-second").is_some());
        });

        // Neither initializer produces a basis.
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![
            init_factory(vec![changeset(ChangeSetKind::Full, "from-first", None)]),
            init_factory(vec![changeset(ChangeSetKind::Full, "from-second", None)]),
        ];

        let source_manager = SourceManager::new(vec![sync_factory(vec![], no_selectors(), false)]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // The signal waited until the initializers were exhausted, so the second
        // initializer's payload was already applied when it fired.
        assert_eq!(*flag_at_signal.lock().unwrap(), Some(true));
    }

    #[tokio::test]
    async fn basis_stops_later_initializers() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let selectors_seen: Selectors = Arc::new(Mutex::new(Vec::new()));
        let (init_complete, calls) = recording_init_complete();

        // The first initializer yields a basis; the second would apply another flag.
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![
            init_factory(vec![changeset(
                ChangeSetKind::Full,
                "from-first",
                Some("s1".into()),
            )]),
            init_factory(vec![changeset(
                ChangeSetKind::Full,
                "from-second",
                Some("s2".into()),
            )]),
        ];

        let source_manager =
            SourceManager::new(vec![sync_factory(vec![], selectors_seen.clone(), false)]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // The basis ended the initializer phase, so the second initializer never ran.
        assert!(store.read().flag("from-first").is_some());
        assert!(store.read().flag("from-second").is_none());
        assert_eq!(*calls.lock().unwrap(), vec![true]);

        // The synchronizer starts from the basis selector.
        assert_eq!(selectors_seen.lock().unwrap()[0], Some("s1".into()));
    }

    #[tokio::test]
    async fn none_changeset_does_not_clobber_the_selector() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let selectors_seen: Selectors = Arc::new(Mutex::new(Vec::new()));
        let (init_complete, _calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // A full basis carrying selector "s1", then a "no changes" (None) changeset.
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![
                changeset(ChangeSetKind::Full, "flag", Some("s1".into())),
                changeset(ChangeSetKind::None, "flag", None),
            ],
            selectors_seen.clone(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store,
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // Exactly three requests, and the one issued after the None changeset still
        // carries "s1" -- the None must not reset the selector to None.
        let seen = selectors_seen.lock().unwrap();
        assert_eq!(*seen, vec![None, Some("s1".into()), Some("s1".into())]);
    }

    #[tokio::test]
    async fn selectorless_change_clears_the_selector() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let selectors_seen: Selectors = Arc::new(Mutex::new(Vec::new()));
        let (init_complete, _calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // A full payload carrying selector "s1", then a partial change with no selector.
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![
                changeset(ChangeSetKind::Full, "flag", Some("s1".into())),
                changeset(ChangeSetKind::Partial, "flag", None),
            ],
            selectors_seen.clone(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store,
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // The selectorless change advanced state, so the stale selector is dropped.
        let seen = selectors_seen.lock().unwrap();
        assert_eq!(*seen, vec![None, Some("s1".into()), None]);
    }

    #[tokio::test]
    async fn initializer_delta_does_not_initialize() {
        // A spec-compliant backend never returns a delta to an initializer.
        // Here the sole initializer returns one anyway, which is not a full payload.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> =
            vec![init_factory(vec![changeset(
                ChangeSetKind::Partial,
                "delta-flag",
                Some("s1".into()),
            )])];

        // The synchronizer then fails terminally, exhausting every source.
        let source_manager =
            SourceManager::new(vec![sync_factory(vec![terminal()], no_selectors(), false)]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // The delta applied, but a delta is not a full payload, so failure is signaled.
        assert!(store.read().flag("delta-flag").is_some());
        assert_eq!(*calls.lock().unwrap(), vec![false]);
    }

    #[tokio::test]
    async fn synchronizer_delta_does_not_initialize() {
        // No initializers, so the store starts uninitialized.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // A spec-compliant backend never sends a delta before a full payload.
        // Here the synchronizer delivers one first anyway, then fails terminally.
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![
                changeset(ChangeSetKind::Partial, "delta-flag", Some("s1".into())),
                terminal(),
            ],
            no_selectors(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // The delta applied, but a delta is not a full payload, so failure is signaled.
        assert!(store.read().flag("delta-flag").is_some());
        assert_eq!(*calls.lock().unwrap(), vec![false]);
    }

    #[tokio::test]
    async fn synchronizer_selectorless_full_initializes() {
        // No initializers, so the synchronizer provides the full payload.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // A full payload with no selector still initializes, as the FDv1 adapter produces.
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![changeset(ChangeSetKind::Full, "full-flag", None)],
            no_selectors(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // A selectorless full still initializes.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("full-flag").is_some());
    }

    #[tokio::test]
    async fn failed_initializers_let_synchronizer_provide_the_basis() {
        // Store and init-complete recorder.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();

        // Both initializers fail without producing a basis.
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![
            init_factory(vec![interrupted()]),
            init_factory(vec![terminal()]),
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
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> =
            vec![init_factory(vec![terminal()])];

        // The synchronizer reports an interruption, then fails terminally.
        let source_manager = SourceManager::new(vec![sync_factory(
            vec![interrupted(), terminal()],
            no_selectors(),
            false,
        )]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store,
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

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
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

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
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // A synchronizer whose next() never resolves.
        let source_manager = SourceManager::new(vec![sync_factory(vec![], no_selectors(), true)]);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Drive the run on a task, then signal shutdown.
        let handle = tokio::spawn(run(
            initializer_factories,
            source_manager,
            store,
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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

    #[test]
    fn fdv1_fallback_factory_starts_blocked() {
        let mut sources = SourceManager::new(vec![
            sync_factory(vec![], no_selectors(), false),
            fdv1_factory(vec![], no_selectors(), false),
        ]);

        // Only the FDv2 synchronizer is available; the FDv1 fallback is dormant.
        assert_eq!(sources.available_count(), 1);
        sources.next_synchronizer();
        assert!(!sources.is_current_fdv1_fallback());
    }

    #[test]
    fn switch_to_and_back_from_fdv1_fallback() {
        let mut sources = SourceManager::new(vec![
            sync_factory(vec![], no_selectors(), false),
            fdv1_factory(vec![], no_selectors(), false),
        ]);

        // The directive activates only the FDv1 fallback.
        sources.switch_to_fdv1_fallback();
        assert_eq!(sources.available_count(), 1);
        sources.next_synchronizer();
        assert!(sources.is_current_fdv1_fallback());

        // Switching back restores the FDv2 synchronizer and re-blocks FDv1.
        sources.switch_back_to_fdv2();
        assert_eq!(sources.available_count(), 1);
        sources.next_synchronizer();
        assert!(!sources.is_current_fdv1_fallback());
    }

    #[test]
    fn switch_back_unblocks_terminally_blocked_fdv2() {
        let mut sources = SourceManager::new(vec![
            sync_factory(vec![], no_selectors(), false),
            fdv1_factory(vec![], no_selectors(), false),
        ]);

        // A terminal error blocks the only FDv2 synchronizer.
        sources.next_synchronizer();
        sources.block_current();
        assert_eq!(sources.available_count(), 0);

        // A fallback round-trip makes the FDv2 synchronizer usable again.
        sources.switch_to_fdv1_fallback();
        sources.switch_back_to_fdv2();
        assert_eq!(sources.available_count(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_fires_after_sustained_interruption() {
        // No initializers; the prime interrupts then idles.
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

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
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

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
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
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
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // Prime recovers on rebuild; the fallback supplies a basis then idles.
        let source_manager = SourceManager::new(vec![
            Arc::new(DownThenDataFactory {
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
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        ));
        handle.await.unwrap();

        // Fell back to the fallback's basis, then recovered to the prime's delta.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("from-fallback").is_some());
        assert!(store.read().flag("prime-recovered").is_some());
    }

    #[tokio::test]
    async fn fallback_directive_switches_to_fdv1() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // The FDv2 source reports a fallback directive; the FDv1 fallback then
        // supplies the basis and ends the run.
        let source_manager = SourceManager::new(vec![
            fallback_directive_factory(Duration::from_secs(60)),
            fdv1_factory(
                vec![changeset(
                    ChangeSetKind::Full,
                    "from-fdv1",
                    Some("s".into()),
                )],
                no_selectors(),
                false,
            ),
        ]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // Switched to the FDv1 fallback and applied its basis.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("from-fdv1").is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn fdv2_retry_reengages_after_ttl() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // The FDv2 source reports a directive with a 60s TTL; the FDv1 fallback
        // supplies a basis and then idles until FDv2 is retried.
        let source_manager = SourceManager::new(vec![
            Arc::new(FallbackThenDataFactory {
                ttl: Duration::from_secs(60),
                reengage_kind: ChangeSetKind::Partial,
                builds: AtomicUsize::new(0),
            }) as Arc<dyn SynchronizerFactory>,
            fdv1_factory(
                vec![changeset(
                    ChangeSetKind::Full,
                    "from-fdv1",
                    Some("s".into()),
                )],
                no_selectors(),
                true,
            ),
        ]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Paused time auto-advances past the TTL, re-engaging FDv2.
        let handle = tokio::spawn(run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        ));
        handle.await.unwrap();

        // Fell back to FDv1, then re-engaged FDv2 once the TTL expired.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("from-fdv1").is_some());
        assert!(store.read().flag("fdv2-back").is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn fdv2_retry_survives_a_terminal_fdv1_fallback() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = vec![];

        // The FDv2 source reports a 60s directive; the FDv1 fallback then fails
        // terminally, blocking every source before the TTL expires.
        let source_manager = SourceManager::new(vec![
            Arc::new(FallbackThenDataFactory {
                ttl: Duration::from_secs(60),
                reengage_kind: ChangeSetKind::Full,
                builds: AtomicUsize::new(0),
            }) as Arc<dyn SynchronizerFactory>,
            fdv1_factory(vec![terminal()], no_selectors(), false),
        ]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Paused time advances past the TTL while no source is available.
        let handle = tokio::spawn(run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        ));
        handle.await.unwrap();

        // The blocked fallback did not strand the run; FDv2 re-engaged after the TTL.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("fdv2-back").is_some());
    }

    #[tokio::test]
    async fn initializer_fallback_directive_switches_to_fdv1() {
        let store = Arc::new(RwLock::new(InMemoryDataStore::new()));
        let (init_complete, calls) = recording_init_complete();
        let initializer_factories: Vec<Arc<dyn InitializerFactory>> =
            vec![Arc::new(FallbackInitializerFactory {
                ttl: Duration::from_secs(60),
            })];

        // The initializer's directive switches to the FDv1 fallback, which supplies
        // the basis and ends the run.
        let source_manager = SourceManager::new(vec![
            sync_factory(vec![], no_selectors(), false),
            fdv1_factory(
                vec![changeset(
                    ChangeSetKind::Full,
                    "from-fdv1",
                    Some("s".into()),
                )],
                no_selectors(),
                false,
            ),
        ]);
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        run(
            initializer_factories,
            source_manager,
            store.clone(),
            init_complete,
            shutdown_rx,
            FALLBACK_TIMEOUT,
            RECOVERY_TIMEOUT,
        )
        .await;

        // The initializer's directive switched straight to the FDv1 fallback.
        assert_eq!(*calls.lock().unwrap(), vec![true]);
        assert!(store.read().flag("from-fdv1").is_some());
    }
}
