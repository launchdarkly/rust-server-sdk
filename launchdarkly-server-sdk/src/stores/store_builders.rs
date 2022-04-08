use super::store::{DataStore, InMemoryDataStore};
use parking_lot::RwLock;
use std::sync::Arc;
use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("data store factory failed to build: {0}")]
    InvalidConfig(String),
}

/// Trait which allows creation of data stores. Should be implemented by data store builder types.
pub trait DataStoreFactory {
    fn build(&self) -> Result<Arc<RwLock<dyn DataStore>>, BuildError>;
    fn to_owned(&self) -> Box<dyn DataStoreFactory>;
}

/// Contains methods for configuring the in memory data store.
///
/// By default, the SDK uses an in memory store to manage flag and segment data.
#[derive(Clone)]
pub struct InMemoryDataStoreBuilder {}

impl InMemoryDataStoreBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl DataStoreFactory for InMemoryDataStoreBuilder {
    fn build(&self) -> Result<Arc<RwLock<dyn DataStore>>, BuildError> {
        Ok(Arc::new(RwLock::new(InMemoryDataStore::new())))
    }

    fn to_owned(&self) -> Box<dyn DataStoreFactory> {
        Box::new(self.clone())
    }
}
