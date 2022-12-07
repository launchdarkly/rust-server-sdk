use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

use super::persistent_store_wrapper::PersistentDataStoreWrapper;
use super::store_builders::{BuildError, DataStoreFactory};
use super::{persistent_store::PersistentDataStore, store::DataStore};

const DEFAULT_CACHE_TIME: Duration = Duration::from_secs(15);

/// PersistentDataStoreFactory is an interface for a factory that creates some implementation of a
/// [PersistentDataStore].
///
/// This interface is implemented by database integrations. Usage is described in
/// [PersistentDataStore].
pub trait PersistentDataStoreFactory {
    /// This is called by the SDK to create the implementation instance.
    fn create_persistent_data_store(&self) -> Result<Box<dyn PersistentDataStore>, std::io::Error>;
}

/// Used to create a PersistentDataStoreWrapper instance, which wraps a [PersistentDataStore].
#[derive(Clone)]
pub struct PersistentDataStoreBuilder {
    cache_ttl: Option<Duration>,
    factory: Arc<dyn PersistentDataStoreFactory>,
}

impl PersistentDataStoreBuilder {
    /// Create a new [PersistentDataStoreBuilder] configured with the provided
    /// [PersistentDataStoreFactory] and a default cache lifetime.
    pub fn new(factory: Arc<dyn PersistentDataStoreFactory>) -> Self {
        Self {
            cache_ttl: Some(DEFAULT_CACHE_TIME),
            factory,
        }
    }

    /// Specifies the cache TTL. Items will be evicted from the cache after this amount of time
    /// from the time when they were originally cached.
    ///
    /// If the value is zero, caching is disabled (equivalent to [PersistentDataStoreBuilder::no_caching]).
    pub fn cache_time(&mut self, cache_ttl: Duration) -> &mut Self {
        self.cache_ttl = Some(cache_ttl);
        self
    }

    /// Shortcut for calling [PersistentDataStoreBuilder::cache_time] with a duration in seconds.
    pub fn cache_seconds(&mut self, seconds: u64) -> &mut Self {
        self.cache_ttl = Some(Duration::from_secs(seconds));
        self
    }

    /// Specifies that the in-memory cache should never expire. In this mode, data will be written
    /// to both the underlying persistent store and the cache, but will only ever be read from the
    /// persistent store if the SDK is restarted.
    ///
    /// Use this mode with caution: it means that in a scenario where multiple processes are
    /// sharing the database, and the current process loses connectivity to LaunchDarkly while
    /// other processes are still receiving updates and writing them to the database, the current
    /// process will have stale data.
    pub fn cache_forever(&mut self) -> &mut Self {
        self.cache_ttl = None;
        self
    }

    /// Specifies that the SDK should not use an in-memory cache for the persistent data store.
    /// This means that every feature flag evaluation will trigger a data store query.
    pub fn no_caching(&mut self) -> &mut Self {
        self.cache_ttl = Some(Duration::from_secs(0));
        self
    }
}

impl DataStoreFactory for PersistentDataStoreBuilder {
    fn build(&self) -> Result<Arc<RwLock<dyn DataStore>>, BuildError> {
        let store = self
            .factory
            .create_persistent_data_store()
            .map_err(|e| BuildError::InvalidConfig(e.to_string()))?;
        Ok(Arc::new(RwLock::new(PersistentDataStoreWrapper::new(
            store,
            self.cache_ttl,
        ))))
    }

    fn to_owned(&self) -> Box<dyn DataStoreFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::stores::persistent_store::tests::InMemoryPersistentDataStore;
    use std::collections::HashMap;

    use crate::stores::store_types::AllData;

    use super::*;

    struct InMemoryPersistentDataStoreFactory {}

    impl PersistentDataStoreFactory for InMemoryPersistentDataStoreFactory {
        fn create_persistent_data_store(
            &self,
        ) -> Result<Box<(dyn PersistentDataStore + 'static)>, std::io::Error> {
            Ok(Box::new(InMemoryPersistentDataStore {
                data: AllData {
                    flags: HashMap::new(),
                    segments: HashMap::new(),
                },
                initialized: false,
            }))
        }
    }

    #[test]
    fn builder_can_support_different_cache_ttl_options() {
        let factory = InMemoryPersistentDataStoreFactory {};
        let mut builder = PersistentDataStoreBuilder::new(Arc::new(factory));

        assert_eq!(builder.cache_ttl, Some(DEFAULT_CACHE_TIME));

        builder.cache_time(Duration::from_secs(100));
        assert_eq!(builder.cache_ttl, Some(Duration::from_secs(100)));

        builder.cache_seconds(1000);
        assert_eq!(builder.cache_ttl, Some(Duration::from_secs(1000)));

        builder.cache_forever();
        assert_eq!(builder.cache_ttl, None);
    }
}
