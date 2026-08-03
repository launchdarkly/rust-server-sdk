use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::data_source::DataSource;
use crate::stores::store::DataStore;
use crate::stores::store_builders::{BuildError, DataStoreFactory};

/// Component that owns the data store and keeps it populated from a source.
pub(crate) trait DataSystem: Send + Sync {
    fn start(
        &self,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    );

    /// Shared handle to the store the evaluation path reads from.
    fn store(&self) -> Arc<RwLock<dyn DataStore>>;
}

/// Adapter that lets any existing FDv1 `DataSource` satisfy `DataSystem`.
pub(crate) struct FDv1DataSystem {
    source: Arc<dyn DataSource>,
    store: Arc<RwLock<dyn DataStore>>,
}

impl FDv1DataSystem {
    pub(crate) fn new(
        source: Arc<dyn DataSource>,
        data_store_builder: &dyn DataStoreFactory,
    ) -> Result<Self, BuildError> {
        let store = data_store_builder.build()?;
        Ok(Self { source, store })
    }
}

impl DataSystem for FDv1DataSystem {
    fn start(
        &self,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    ) {
        self.source
            .subscribe(self.store.clone(), init_complete, shutdown_receiver);
    }

    fn store(&self) -> Arc<RwLock<dyn DataStore>> {
        self.store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::data_source::MockDataSource;
    use crate::stores::store_builders::InMemoryDataStoreBuilder;

    #[tokio::test]
    async fn start_forwards_to_underlying_data_source() {
        let source: Arc<dyn DataSource> = Arc::new(MockDataSource::new_with_init_delay(0));
        let system = FDv1DataSystem::new(source, &InMemoryDataStoreBuilder::new())
            .expect("in-memory store builds");

        let initialized = Arc::new(AtomicBool::new(false));
        let init_state = initialized.clone();
        let init_complete: Arc<dyn Fn(bool) + Send + Sync> =
            Arc::new(move |success| init_state.store(success, Ordering::SeqCst));
        let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

        system.start(init_complete, shutdown_rx);

        assert!(initialized.load(Ordering::SeqCst));
        drop(shutdown_tx);
    }
}
