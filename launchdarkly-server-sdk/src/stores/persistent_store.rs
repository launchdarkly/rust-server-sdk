use core::fmt;
use std::collections::HashMap;

use super::store_types::{AllData, DataKind, SerializedItem};

/// Error type used to represent failures when interacting with the underlying persistent stores.
#[derive(Debug)]
pub struct PersistentStoreError {
    message: String,
}

impl PersistentStoreError {
    /// Create a new PersistentStoreError with the provided message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::error::Error for PersistentStoreError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for PersistentStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// PersistentDataStore is an interface for a data store that holds feature flags and related data
/// in a serialized form.
///
/// This interface should be used for database integrations, or any other data store implementation
/// that stores data in some external service. The SDK will provide its own caching layer on top of
/// the persistent data store; the data store implementation should not provide caching, but simply
/// do every query or update that the SDK tells it to do.
pub trait PersistentDataStore: Send + Sync {
    /// Overwrites the store's contents with a set of items for each collection.
    ///
    /// All previous data should be discarded, regardless of versioning.
    ///
    /// The update should be done atomically. If it cannot be done atomically, then the store
    /// must first add or update each item in the same order that they are given in the input
    /// data, and then delete any previously stored items that were not in the input data.
    fn init(
        &mut self,
        all_data: AllData<SerializedItem, SerializedItem>,
    ) -> Result<(), PersistentStoreError>;

    /// Retrieves a flag item from the specified collection, if available.
    ///
    /// If the specified key does not exist in the collection, it should result Ok(None).
    ///
    /// If the item has been deleted and the store contains a placeholder, it should return that
    /// placeholder rather than filtering it out.
    fn flag(&self, key: &str) -> Result<Option<SerializedItem>, PersistentStoreError>;

    /// Retrieves a segment item from the specified collection, if available.
    ///
    /// If the specified key does not exist in the collection, it should result Ok(None).
    ///
    /// If the item has been deleted and the store contains a placeholder, it should return that
    /// placeholder rather than filtering it out.
    fn segment(&self, key: &str) -> Result<Option<SerializedItem>, PersistentStoreError>;

    /// Retrieves all flag items from the specified collection.
    ///
    /// If the store contains placeholders for deleted items, it should include them in the results,
    /// not filter them out.
    fn all_flags(&self) -> Result<HashMap<String, SerializedItem>, PersistentStoreError>;

    /// Updates or inserts an item in the specified collection. For updates, the object will only be
    /// updated if the existing version is less than the new version.
    ///
    /// The SDK may pass a [SerializedItem] that represents a placeholder for a deleted item. In
    /// that case, assuming the version is greater than any existing version of that item, the store should
    /// retain that placeholder rather than simply not storing anything.
    fn upsert(
        &mut self,
        kind: DataKind,
        key: &str,
        serialized_item: SerializedItem,
    ) -> Result<bool, PersistentStoreError>;

    /// Returns true if the data store contains a data set, meaning that [PersistentDataStore::init] has been called at
    /// least once.
    ///
    /// In a shared data store, it should be able to detect this even if [PersistentDataStore::init] was called in a
    /// different process: that is, the test should be based on looking at what is in the data store.
    fn is_initialized(&self) -> bool;
}

#[cfg(test)]
pub(super) mod tests {
    use crate::stores::persistent_store::PersistentDataStore;
    use crate::stores::store_types::{AllData, DataKind, SerializedItem};
    use std::collections::HashMap;

    use super::PersistentStoreError;

    pub struct NullPersistentDataStore {
        pub(crate) initialized: bool,
    }

    impl PersistentDataStore for NullPersistentDataStore {
        fn init(
            &mut self,
            _all_data: AllData<SerializedItem, SerializedItem>,
        ) -> Result<(), PersistentStoreError> {
            self.initialized = true;
            Ok(())
        }

        fn flag(&self, _key: &str) -> Result<Option<SerializedItem>, PersistentStoreError> {
            Ok(None)
        }

        fn segment(&self, _key: &str) -> Result<Option<SerializedItem>, PersistentStoreError> {
            Ok(None)
        }

        fn all_flags(&self) -> Result<HashMap<String, SerializedItem>, PersistentStoreError> {
            Ok(HashMap::new())
        }

        fn upsert(
            &mut self,
            _kind: DataKind,
            _key: &str,
            _serialized_item: SerializedItem,
        ) -> Result<bool, PersistentStoreError> {
            Ok(true)
        }

        fn is_initialized(&self) -> bool {
            self.initialized
        }
    }

    pub struct InMemoryPersistentDataStore {
        pub(crate) data: AllData<SerializedItem, SerializedItem>,
        pub(crate) initialized: bool,
    }

    impl PersistentDataStore for InMemoryPersistentDataStore {
        fn init(
            &mut self,
            all_data: AllData<SerializedItem, SerializedItem>,
        ) -> Result<(), PersistentStoreError> {
            self.data = all_data;
            self.initialized = true;
            Ok(())
        }

        fn flag(&self, key: &str) -> Result<Option<SerializedItem>, PersistentStoreError> {
            Ok(self.data.flags.get(key).map(|value| (*value).clone()))
        }

        fn segment(&self, key: &str) -> Result<Option<SerializedItem>, PersistentStoreError> {
            Ok(self.data.segments.get(key).map(|value| (*value).clone()))
        }

        fn all_flags(&self) -> Result<HashMap<String, SerializedItem>, PersistentStoreError> {
            Ok(self.data.flags.clone())
        }

        fn upsert(
            &mut self,
            kind: DataKind,
            key: &str,
            serialized_item: SerializedItem,
        ) -> Result<bool, PersistentStoreError> {
            let original = match kind {
                DataKind::Flag => self.data.flags.insert(key.to_string(), serialized_item),
                DataKind::Segment => self.data.segments.insert(key.to_string(), serialized_item),
            };

            Ok(original.is_some())
        }

        fn is_initialized(&self) -> bool {
            self.initialized
        }
    }
}
