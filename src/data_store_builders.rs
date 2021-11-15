use super::client::Error;
use super::data_store::{DataStore, InMemoryDataStore};
use std::sync::{Arc, Mutex};

/// Trait which allows creation of data stores. Should be implemented by data store builder types.
pub trait DataStoreFactory {
    fn build(&self) -> Result<Arc<Mutex<dyn DataStore>>, Error>;
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
    fn build(&self) -> Result<Arc<Mutex<dyn DataStore>>, Error> {
        Ok(Arc::new(Mutex::new(InMemoryDataStore::new())))
    }

    fn to_owned(&self) -> Box<dyn DataStoreFactory> {
        Box::new(self.clone())
    }
}
