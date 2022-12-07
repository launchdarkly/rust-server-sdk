use crate::stores::store_types::{AllData, PatchTarget, StorageItem};
use std::collections::HashMap;
use thiserror::Error;

use launchdarkly_server_sdk_evaluation::{Flag, Segment, Store, Versioned};

use super::persistent_store::PersistentStoreError;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("expected a {0}, got a {0}")]
    InvalidTarget(String, String),

    #[error("couldn't parse json as a flag or segment: {0}")]
    ParseError(serde_json::Error),

    #[error("underlying persistent store returned an error: {0}")]
    PersistentStore(#[from] PersistentStoreError),
}

/// Trait for a data store that holds and updates feature flags and related data received by the
/// SDK.
pub trait DataStore: Store + Send + Sync {
    fn init(&mut self, new_data: AllData<Flag, Segment>);
    fn all_flags(&self) -> HashMap<String, Flag>;
    fn upsert(&mut self, key: &str, data: PatchTarget) -> Result<(), UpdateError>;
    fn to_store(&self) -> &dyn Store;
}

/// Default implementation of [DataStore] which holds information in-memory.
pub struct InMemoryDataStore {
    pub data: AllData<StorageItem<Flag>, StorageItem<Segment>>,
}

impl InMemoryDataStore {
    pub fn new() -> Self {
        Self {
            data: AllData {
                flags: HashMap::new(),
                segments: HashMap::new(),
            },
        }
    }

    fn upsert_flag(&mut self, key: &str, item: StorageItem<Flag>) {
        match self.data.flags.get(key) {
            Some(existing) if existing.is_greater_than_or_equal(item.version()) => None,
            _ => self.data.flags.insert(key.to_string(), item),
        };
    }

    fn upsert_segment(&mut self, key: &str, item: StorageItem<Segment>) {
        match self.data.segments.get(key) {
            Some(existing) if existing.is_greater_than_or_equal(item.version()) => None,
            _ => self.data.segments.insert(key.to_string(), item),
        };
    }
}

impl Store for InMemoryDataStore {
    fn flag(&self, flag_key: &str) -> Option<Flag> {
        match self.data.flags.get(flag_key) {
            Some(StorageItem::Item(f)) => Some(f.clone()),
            _ => None,
        }
    }

    fn segment(&self, segment_key: &str) -> Option<Segment> {
        match self.data.segments.get(segment_key) {
            Some(StorageItem::Item(s)) => Some(s.clone()),
            _ => None,
        }
    }
}

impl DataStore for InMemoryDataStore {
    fn init(&mut self, new_data: AllData<Flag, Segment>) {
        self.data = new_data.into();
        debug!("data store has been updated with new flag data");
    }

    fn all_flags(&self) -> HashMap<String, Flag> {
        self.data
            .flags
            .iter()
            .filter_map(|(key, item)| match item {
                StorageItem::Tombstone(_) => None,
                StorageItem::Item(f) => Some((key.clone(), f.clone())),
            })
            .collect()
    }

    fn upsert(&mut self, key: &str, data: PatchTarget) -> Result<(), UpdateError> {
        match data {
            PatchTarget::Flag(item) => {
                self.upsert_flag(key, item);
                Ok(())
            }
            PatchTarget::Segment(item) => {
                self.upsert_segment(key, item);
                Ok(())
            }
            PatchTarget::Other(v) => Err(UpdateError::InvalidTarget(
                "flag or segment".to_string(),
                format!("{:?}", v),
            )),
        }
    }

    fn to_store(&self) -> &dyn Store {
        self
    }
}

impl Default for InMemoryDataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common::{basic_flag, basic_segment};
    use maplit::hashmap;
    use test_case::test_case;

    fn basic_data() -> AllData<Flag, Segment> {
        AllData {
            flags: hashmap! {"flag-key".into() => basic_flag("flag-key")},
            segments: hashmap! {"segment-key".into() => basic_segment("segment-key")},
        }
    }

    #[test]
    fn in_memory_can_be_initialized() {
        let mut data_store = InMemoryDataStore::new();
        assert!(data_store.flag("flag-key").is_none());
        assert!(data_store.segment("segment-key").is_none());

        data_store.init(basic_data());

        assert_eq!(data_store.flag("flag-key").unwrap().key, "flag-key");
        assert_eq!(
            data_store.segment("segment-key").unwrap().key,
            "segment-key"
        );
    }

    #[test]
    fn in_memory_can_return_all_flags() {
        let mut data_store = InMemoryDataStore::new();

        assert!(!data_store.all_flags().contains_key("flag-key"));
        data_store.init(basic_data());
        assert!(data_store.all_flags().contains_key("flag-key"));
    }

    #[test]
    fn in_memory_patch_can_upsert_flag() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        // Verify patch can insert
        assert!(data_store.flag("flag-does-not-exist").is_none());
        let patch_target = PatchTarget::Flag(StorageItem::Item(basic_flag("flag-does-not-exist")));
        let result = data_store.upsert("flag-does-not-exist", patch_target);
        assert!(result.is_ok());
        assert_eq!(
            data_store.flag("flag-does-not-exist").unwrap().key,
            "flag-does-not-exist"
        );

        // Verify that patch can update
        let mut flag = basic_flag("new-key");
        flag.version = 43;
        let patch_target = PatchTarget::Flag(StorageItem::Item(flag));
        let result = data_store.upsert("flag-key", patch_target);
        assert!(result.is_ok());
        assert_eq!(data_store.flag("flag-key").unwrap().key, "new-key");
    }

    #[test]
    fn in_memory_patch_can_upsert_flag_deleted_flag() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        let mut flag = data_store.flag("flag-key").unwrap();
        flag.version += 1;

        assert!(data_store
            .upsert(
                "flag-key",
                PatchTarget::Flag(StorageItem::Tombstone(flag.version))
            )
            .is_ok());
        assert!(data_store.flag("flag-key").is_none());

        flag.version -= 1;

        let patch_target = PatchTarget::Flag(StorageItem::Item(flag.clone()));
        assert!(data_store.upsert("flag-key", patch_target).is_ok());
        assert!(data_store.flag("flag-key").is_none());

        flag.version += 2;
        let patch_target = PatchTarget::Flag(StorageItem::Item(flag));
        assert!(data_store.upsert("flag-key", patch_target).is_ok());
        assert!(data_store.flag("flag-key").is_some());
    }

    #[test_case(41, 42)]
    #[test_case(43, 43)]
    fn in_memory_patch_does_not_update_flag_with_older_version(
        updated_version: u64,
        expected_version: u64,
    ) {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        let mut flag = data_store.flag("flag-key").unwrap();
        assert_eq!(42, flag.version);

        flag.version = updated_version;

        let patch_target = PatchTarget::Flag(StorageItem::Item(flag));
        let result = data_store.upsert("flag-key", patch_target);
        assert!(result.is_ok());

        let flag = data_store.flag("flag-key").unwrap();
        assert_eq!(expected_version, flag.version);
    }

    #[test]
    fn in_memory_patch_can_upsert_segment() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        // Verify patch can insert
        assert!(data_store.segment("segment-does-not-exist").is_none());
        let patch_target =
            PatchTarget::Segment(StorageItem::Item(basic_segment("segment-does-not-exist")));
        let result = data_store.upsert("segment-does-not-exist", patch_target);
        assert!(result.is_ok());
        assert_eq!(
            data_store.segment("segment-does-not-exist").unwrap().key,
            "segment-does-not-exist"
        );

        // Verify that patch can update
        let patch_target = PatchTarget::Segment(StorageItem::Item(basic_segment("new-key")));
        let result = data_store.upsert("my-boolean-segment", patch_target);
        assert!(result.is_ok());
        assert_eq!(
            data_store.segment("my-boolean-segment").unwrap().key,
            "new-key"
        );
    }

    #[test]
    fn in_memory_patch_can_upsert_segment_deleted_segment() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        let mut segment = data_store.segment("segment-key").unwrap();
        segment.version += 1;

        assert!(data_store
            .upsert(
                "segment-key",
                PatchTarget::Segment(StorageItem::Tombstone(segment.version))
            )
            .is_ok());
        assert!(data_store.segment("segment-key").is_none());

        segment.version -= 1;

        let patch_target = PatchTarget::Segment(StorageItem::Item(segment.clone()));
        assert!(data_store.upsert("segment-key", patch_target).is_ok());
        assert!(data_store.segment("segment-key").is_none());

        segment.version += 2;
        let patch_target = PatchTarget::Segment(StorageItem::Item(segment));
        assert!(data_store.upsert("segment-key", patch_target).is_ok());
        assert!(data_store.segment("segment-key").is_some());
    }

    #[test_case(0, 1)]
    #[test_case(2, 2)]
    fn in_memory_patch_does_not_update_segment_with_older_version(
        updated_version: u64,
        expected_version: u64,
    ) {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        let mut segment = data_store.segment("segment-key").unwrap();
        assert_eq!(1, segment.version);

        segment.version = updated_version;

        let patch_target = PatchTarget::Segment(StorageItem::Item(segment));
        let result = data_store.upsert("segment-key", patch_target);
        assert!(result.is_ok());

        let segment = data_store.segment("segment-key").unwrap();
        assert_eq!(expected_version, segment.version);
    }

    #[test]
    fn in_memory_can_delete_flag() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        assert!(data_store.flag("flag-key").is_some());
        assert!(data_store
            .upsert("flag-key", PatchTarget::Flag(StorageItem::Tombstone(41)))
            .is_ok());
        assert!(data_store.flag("flag-key").is_some());

        assert!(data_store
            .upsert("flag-key", PatchTarget::Flag(StorageItem::Tombstone(42)))
            .is_ok());
        assert!(data_store.flag("flag-key").is_some());

        data_store.init(basic_data());

        assert!(data_store
            .upsert("flag-key", PatchTarget::Flag(StorageItem::Tombstone(43)))
            .is_ok());
        assert!(data_store.flag("flag-key").is_none());
    }

    #[test]
    fn in_memory_can_delete_segment() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        assert!(data_store.segment("segment-key").is_some());
        assert!(data_store
            .upsert(
                "segment-key",
                PatchTarget::Segment(StorageItem::Tombstone(0))
            )
            .is_ok());
        assert!(data_store.segment("segment-key").is_some());

        assert!(data_store
            .upsert(
                "segment-key",
                PatchTarget::Segment(StorageItem::Tombstone(1))
            )
            .is_ok());
        assert!(data_store.segment("segment-key").is_some());

        data_store.init(basic_data());

        assert!(data_store
            .upsert(
                "segment-key",
                PatchTarget::Segment(StorageItem::Tombstone(2))
            )
            .is_ok());
        assert!(data_store.segment("segment-key").is_none());
    }
}
