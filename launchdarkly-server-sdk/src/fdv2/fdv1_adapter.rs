use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use launchdarkly_server_sdk_evaluation::{Flag, Segment, Store};
use parking_lot::RwLock;
use tokio::sync::{broadcast, mpsc};

use crate::data_source::DataSource;
use crate::stores::change_set::{ChangeSet, ItemChange};
use crate::stores::store::{DataStore, UpdateError};
use crate::stores::store_types::{AllData, PatchTarget, StorageItem};

use super::data_system::SynchronizerFactory;
use super::model::{ChangeSetKind, Selector};
use super::source::{ErrorInfo, ErrorKind, FDv2SourceEvent, FDv2SourceResult, Synchronizer};

/// Adapts the FDv1 source's store writes into FDv2 results on a channel.
struct CapturingStore {
    sender: mpsc::UnboundedSender<FDv2SourceResult>,
}

// The FDv1 source only writes to the store; these reads are never called.
impl Store for CapturingStore {
    fn flag(&self, _flag_key: &str) -> Option<Flag> {
        None
    }

    fn segment(&self, _segment_key: &str) -> Option<Segment> {
        None
    }
}

impl DataStore for CapturingStore {
    fn init(&mut self, new_data: AllData<Flag, Segment>) {
        let mut changes = Vec::new();
        for (key, flag) in new_data.flags {
            changes.push(ItemChange::Flag {
                key,
                item: StorageItem::Item(flag),
            });
        }
        for (key, segment) in new_data.segments {
            changes.push(ItemChange::Segment {
                key,
                item: StorageItem::Item(segment),
            });
        }
        // A send error means the adapter's receiver was dropped; ignore it.
        let _ = self.sender.send(FDv2SourceResult::ChangeSet(ChangeSet {
            kind: ChangeSetKind::Full,
            changes,
            selector: None,
        }));
    }

    // Unused; the FDv1 source never reads back from the store.
    fn all_flags(&self) -> HashMap<String, Flag> {
        HashMap::new()
    }

    fn upsert(&mut self, key: &str, data: PatchTarget) -> Result<(), UpdateError> {
        let change = match data {
            PatchTarget::Flag(item) => ItemChange::Flag {
                key: key.to_string(),
                item,
            },
            PatchTarget::Segment(item) => ItemChange::Segment {
                key: key.to_string(),
                item,
            },
            PatchTarget::Other(value) => {
                return Err(UpdateError::InvalidTarget(
                    "flag or segment".to_string(),
                    format!("{value:?}"),
                ))
            }
        };
        let _ = self.sender.send(FDv2SourceResult::ChangeSet(ChangeSet {
            kind: ChangeSetKind::Partial,
            changes: vec![change],
            selector: None,
        }));
        Ok(())
    }

    fn to_store(&self) -> &dyn Store {
        self
    }
}

/// Wraps an FDv1 `DataSource` as an FDv2 `Synchronizer`.
struct FDv1AdapterSynchronizer {
    results: mpsc::UnboundedReceiver<FDv2SourceResult>,
    // Dropping this stops the wrapped FDv1 source's task.
    _shutdown: broadcast::Sender<()>,
}

impl Synchronizer for FDv1AdapterSynchronizer {
    fn next(&mut self, _selector: Selector) -> BoxFuture<'_, FDv2SourceEvent> {
        Box::pin(async move {
            let result = self
                .results
                .recv()
                .await
                .unwrap_or(FDv2SourceResult::Shutdown);
            FDv2SourceEvent {
                result,
                fdv1_fallback: None,
            }
        })
    }

    fn name(&self) -> &str {
        "fdv1-adapter"
    }
}

/// Wraps a freshly built FDv1 source as the FDv2 fallback synchronizer.
pub(crate) struct FDv1AdapterFactory {
    source_builder: Box<dyn Fn() -> Arc<dyn DataSource> + Send + Sync>,
}

impl FDv1AdapterFactory {
    pub(crate) fn new(source_builder: Box<dyn Fn() -> Arc<dyn DataSource> + Send + Sync>) -> Self {
        Self { source_builder }
    }
}

impl SynchronizerFactory for FDv1AdapterFactory {
    fn create(&self) -> Box<dyn Synchronizer> {
        let (sender, results) = mpsc::unbounded_channel();
        let store: Arc<RwLock<dyn DataStore>> = Arc::new(RwLock::new(CapturingStore {
            sender: sender.clone(),
        }));
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // A permanent FDv1 failure becomes a terminal error for the orchestrator.
        let init_complete: Arc<dyn Fn(bool) + Send + Sync> = Arc::new(move |success| {
            if !success {
                let _ = sender.send(FDv2SourceResult::TerminalError(ErrorInfo {
                    kind: ErrorKind::Unknown,
                    message: "FDv1 fallback source failed to initialize".to_string(),
                }));
            }
        });

        (self.source_builder)().subscribe(store, init_complete, shutdown_rx);

        Box::new(FDv1AdapterSynchronizer {
            results,
            _shutdown: shutdown_tx,
        })
    }

    fn is_fdv1_fallback(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common::basic_flag;
    use tokio::sync::Notify;

    /// An FDv1 source that writes an init then an upsert, then reports success.
    struct WritingSource;

    impl DataSource for WritingSource {
        fn subscribe(
            &self,
            data_store: Arc<RwLock<dyn DataStore>>,
            init_complete: Arc<dyn Fn(bool) + Send + Sync>,
            _shutdown: broadcast::Receiver<()>,
        ) {
            let mut store = data_store.write();
            let mut flags = HashMap::new();
            flags.insert("init-flag".to_string(), basic_flag("init-flag"));
            store.init(AllData {
                flags,
                segments: HashMap::new(),
            });
            store
                .upsert(
                    "upsert-flag",
                    PatchTarget::Flag(StorageItem::Item(basic_flag("upsert-flag"))),
                )
                .unwrap();
            drop(store);
            init_complete(true);
        }
    }

    #[tokio::test]
    async fn translates_init_and_upsert_to_change_sets() {
        let factory =
            FDv1AdapterFactory::new(Box::new(|| Arc::new(WritingSource) as Arc<dyn DataSource>));
        let mut synchronizer = factory.create();

        // The init becomes a full change set.
        match synchronizer.next(None).await.result {
            FDv2SourceResult::ChangeSet(cs) => assert_eq!(cs.kind, ChangeSetKind::Full),
            other => panic!("expected a full change set, got {other:?}"),
        }
        // The upsert becomes a partial change set.
        match synchronizer.next(None).await.result {
            FDv2SourceResult::ChangeSet(cs) => assert_eq!(cs.kind, ChangeSetKind::Partial),
            other => panic!("expected a partial change set, got {other:?}"),
        }
    }

    /// An FDv1 source that reports a permanent initialization failure.
    struct FailingSource;

    impl DataSource for FailingSource {
        fn subscribe(
            &self,
            _data_store: Arc<RwLock<dyn DataStore>>,
            init_complete: Arc<dyn Fn(bool) + Send + Sync>,
            _shutdown: broadcast::Receiver<()>,
        ) {
            init_complete(false);
        }
    }

    #[tokio::test]
    async fn init_failure_becomes_a_terminal_error() {
        let factory =
            FDv1AdapterFactory::new(Box::new(|| Arc::new(FailingSource) as Arc<dyn DataSource>));
        let mut synchronizer = factory.create();
        assert!(matches!(
            synchronizer.next(None).await.result,
            FDv2SourceResult::TerminalError(_)
        ));
    }

    /// An FDv1 source whose task notifies when it observes shutdown.
    struct ShutdownObservingSource {
        observed: Arc<Notify>,
    }

    impl DataSource for ShutdownObservingSource {
        fn subscribe(
            &self,
            _data_store: Arc<RwLock<dyn DataStore>>,
            _init_complete: Arc<dyn Fn(bool) + Send + Sync>,
            mut shutdown: broadcast::Receiver<()>,
        ) {
            let observed = self.observed.clone();
            tokio::spawn(async move {
                let _ = shutdown.recv().await;
                observed.notify_one();
            });
        }
    }

    #[tokio::test]
    async fn dropping_the_adapter_shuts_down_the_source() {
        let observed = Arc::new(Notify::new());
        let builder_observed = observed.clone();
        let factory = FDv1AdapterFactory::new(Box::new(move || {
            Arc::new(ShutdownObservingSource {
                observed: builder_observed.clone(),
            }) as Arc<dyn DataSource>
        }));

        let synchronizer = factory.create();
        drop(synchronizer);

        // The source's task saw the shutdown signal.
        observed.notified().await;
    }
}
