use std::collections::HashMap;

use launchdarkly_server_sdk_evaluation::{Flag, Segment, Store};
use serde::Deserialize;
use thiserror::Error;

const FLAGS_PREFIX: &str = "/flags/";
const SEGMENTS_PREFIX: &str = "/segments/";

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("expected a {0}, got a {0}")]
    InvalidTarget(String, String),

    #[error("couldn't parse json as a flag: {0}")]
    ParseError(String),
}

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum PatchTarget {
    Flag(Flag),
    Segment(Segment),
    Other(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize)]
pub enum StorageItem<T> {
    Item(T),
    Tombstone(u64),
}

impl From<Flag> for StorageItem<Flag> {
    fn from(flag: Flag) -> Self {
        Self::Item(flag)
    }
}

impl StorageItem<Flag> {
    pub fn is_newer_than(&self, version: u64) -> bool {
        let self_version = match self {
            Self::Item(f) => f.version,
            Self::Tombstone(version) => *version,
        };

        self_version > version
    }
}

impl From<Segment> for StorageItem<Segment> {
    fn from(segment: Segment) -> Self {
        Self::Item(segment)
    }
}

impl StorageItem<Segment> {
    pub fn is_newer_than(&self, version: u64) -> bool {
        let self_version = match self {
            Self::Item(s) => s.version,
            Self::Tombstone(version) => *version,
        };

        self_version > version
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct AllData<F, S> {
    flags: HashMap<String, F>,
    segments: HashMap<String, S>,
}

impl From<AllData<Flag, Segment>> for AllData<StorageItem<Flag>, StorageItem<Segment>> {
    fn from(all_data: AllData<Flag, Segment>) -> Self {
        Self {
            flags: all_data
                .flags
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            segments: all_data
                .segments
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

/// Trait for a data store that holds and updates feature flags and related data received by the
/// SDK.
pub trait DataStore: Store + Send + Sync {
    fn init(&mut self, new_data: AllData<Flag, Segment>);
    fn all_flags(&self) -> HashMap<String, &Flag>;
    fn patch(&mut self, path: &str, data: PatchTarget) -> Result<(), UpdateError>;
    fn delete(&mut self, path: &str, version: u64) -> Result<(), UpdateError>;
    fn to_store(&self) -> &dyn Store;
}

// TODO(ch108602) implement Error::ClientNotReady
/// Default implementation of the DataStore which holds information in an in-memory data store.
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

    fn patch_flag(&mut self, flag_key: &str, data: PatchTarget) -> Result<(), UpdateError> {
        let flag = match data {
            PatchTarget::Flag(f) => Ok(f),
            PatchTarget::Segment(_) => Err(UpdateError::InvalidTarget(
                String::from("flag"),
                String::from("segment"),
            )),
            PatchTarget::Other(json) => Err(UpdateError::ParseError(json.to_string())),
        }?;

        match self.data.flags.get(flag_key) {
            Some(item) if item.is_newer_than(flag.version) => None,
            _ => self.data.flags.insert(flag_key.to_string(), flag.into()),
        };

        Ok(())
    }

    fn patch_segment(&mut self, segment_key: &str, data: PatchTarget) -> Result<(), UpdateError> {
        let segment = match data {
            PatchTarget::Segment(s) => Ok(s),
            PatchTarget::Flag(_) => Err(UpdateError::InvalidTarget(
                String::from("segment"),
                String::from("flag"),
            )),
            PatchTarget::Other(json) => Err(UpdateError::ParseError(json.to_string())),
        }?;

        match self.data.segments.get(segment_key) {
            Some(item) if item.is_newer_than(segment.version) => None,
            _ => self
                .data
                .segments
                .insert(segment_key.to_string(), segment.into()),
        };

        Ok(())
    }

    fn delete_flag(&mut self, flag_key: &str, version: u64) {
        match self.data.flags.get(flag_key) {
            Some(item) if item.is_newer_than(version) => None,
            _ => self
                .data
                .flags
                .insert(flag_key.to_string(), StorageItem::Tombstone(version)),
        };
    }

    fn delete_segment(&mut self, segment_key: &str, version: u64) {
        match self.data.segments.get(segment_key) {
            Some(item) if item.is_newer_than(version) => None,
            _ => self
                .data
                .segments
                .insert(segment_key.to_string(), StorageItem::Tombstone(version)),
        };
    }
}

impl Store for InMemoryDataStore {
    fn flag(&self, flag_key: &str) -> Option<&Flag> {
        match self.data.flags.get(flag_key) {
            Some(StorageItem::Item(f)) => Some(f),
            _ => None,
        }
    }

    fn segment(&self, segment_key: &str) -> Option<&Segment> {
        match self.data.segments.get(segment_key) {
            Some(StorageItem::Item(s)) => Some(s),
            _ => None,
        }
    }
}

impl DataStore for InMemoryDataStore {
    fn init(&mut self, new_data: AllData<Flag, Segment>) {
        self.data = new_data.into();
    }

    fn all_flags(&self) -> HashMap<String, &Flag> {
        self.data
            .flags
            .iter()
            .filter_map(|(key, item)| match item {
                StorageItem::Tombstone(_) => None,
                StorageItem::Item(f) => Some((key.clone(), f)),
            })
            .collect()
    }

    fn patch(&mut self, path: &str, data: PatchTarget) -> Result<(), UpdateError> {
        if let Some(flag_key) = path.strip_prefix(FLAGS_PREFIX) {
            self.patch_flag(flag_key, data)
        } else if let Some(segment_key) = path.strip_prefix(SEGMENTS_PREFIX) {
            self.patch_segment(segment_key, data)
        } else {
            Err(UpdateError::InvalidPath(path.to_string()))
        }
    }

    fn delete(&mut self, path: &str, version: u64) -> Result<(), UpdateError> {
        if let Some(flag_key) = path.strip_prefix(FLAGS_PREFIX) {
            self.delete_flag(flag_key, version);
        } else if let Some(segment_key) = path.strip_prefix(SEGMENTS_PREFIX) {
            self.delete_segment(segment_key, version);
        } else {
            return Err(UpdateError::InvalidPath(path.to_string()));
        };

        Ok(())
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
        let patch_target = PatchTarget::Flag(basic_flag("flag-does-not-exist"));
        let result = data_store.patch("/flags/flag-does-not-exist".into(), patch_target);
        assert!(result.is_ok());
        assert_eq!(
            data_store.flag("flag-does-not-exist").unwrap().key,
            "flag-does-not-exist"
        );

        // Verify that patch can update
        let patch_target = PatchTarget::Flag(basic_flag("new-key"));
        let result = data_store.patch("/flags/flag-key".into(), patch_target);
        assert!(result.is_ok());
        assert_eq!(data_store.flag("flag-key").unwrap().key, "new-key");
    }

    #[test]
    fn in_memory_patch_can_upsert_flag_deleted_flag() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        let mut flag = data_store.flag("flag-key").unwrap().clone();

        assert!(data_store.delete("/flags/flag-key", flag.version).is_ok());
        assert!(data_store.flag("flag-key").is_none());

        flag.version = flag.version - 1;

        let patch_target = PatchTarget::Flag(flag.clone());
        assert!(data_store
            .patch("/flags/flag-key".into(), patch_target)
            .is_ok());
        assert!(data_store.flag("flag-key").is_none());

        flag.version = flag.version + 2;
        let patch_target = PatchTarget::Flag(flag);
        assert!(data_store
            .patch("/flags/flag-key".into(), patch_target)
            .is_ok());
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

        let flag = data_store.flag("flag-key").unwrap();
        assert_eq!(42, flag.version);

        let mut flag = flag.clone();
        flag.version = updated_version;

        let patch_target = PatchTarget::Flag(flag);
        let result = data_store.patch("/flags/flag-key".into(), patch_target);
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
        let patch_target = PatchTarget::Segment(basic_segment("segment-does-not-exist"));
        let result = data_store.patch("/segments/segment-does-not-exist".into(), patch_target);
        assert!(result.is_ok());
        assert_eq!(
            data_store.segment("segment-does-not-exist").unwrap().key,
            "segment-does-not-exist"
        );

        // Verify that patch can update
        let patch_target = PatchTarget::Segment(basic_segment("new-key"));
        let result = data_store.patch("/segments/my-boolean-segment".into(), patch_target);
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

        let mut segment = data_store.segment("segment-key").unwrap().clone();

        assert!(data_store
            .delete("/segments/segment-key", segment.version)
            .is_ok());
        assert!(data_store.segment("segment-key").is_none());

        segment.version = segment.version - 1;

        let patch_target = PatchTarget::Segment(segment.clone());
        assert!(data_store
            .patch("/segments/segment-key".into(), patch_target)
            .is_ok());
        assert!(data_store.segment("segment-key").is_none());

        segment.version = segment.version + 2;
        let patch_target = PatchTarget::Segment(segment);
        assert!(data_store
            .patch("/segments/segment-key".into(), patch_target)
            .is_ok());
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

        let segment = data_store.segment("segment-key").unwrap();
        assert_eq!(1, segment.version);

        let mut segment = segment.clone();
        segment.version = updated_version;

        let patch_target = PatchTarget::Segment(segment);
        let result = data_store.patch("/segments/segment-key".into(), patch_target);
        assert!(result.is_ok());

        let segment = data_store.segment("segment-key").unwrap();
        assert_eq!(expected_version, segment.version);
    }

    #[test_case("/invalid-path/flag-key", PatchTarget::Flag(basic_flag("flag-key")); "invalid path")]
    #[test_case("/flags/flag-key", PatchTarget::Segment(basic_segment("segment-key")); "flag with segment target")]
    #[test_case("/flags/flag-key", PatchTarget::Other(serde_json::Value::Null); "flag with other target")]
    #[test_case("/segments/segment-key", PatchTarget::Flag(basic_flag("flag-key")); "segment with flag target")]
    #[test_case("/segments/segment-key", PatchTarget::Other(serde_json::Value::Null); "segment with other target")]
    fn in_memory_invalid_patch_is_err(path: &str, patch_target: PatchTarget) {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        assert!(data_store.patch(path, patch_target).is_err());
    }

    #[test]
    fn in_memory_can_delete_flag() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        assert!(data_store.flag("flag-key").is_some());
        assert!(data_store.delete("/flags/flag-key", 41).is_ok());
        assert!(data_store.flag("flag-key").is_some());

        assert!(data_store.delete("/flags/flag-key", 42).is_ok());
        assert!(data_store.flag("flag-key").is_none());

        data_store.init(basic_data());

        assert!(data_store.delete("/flags/flag-key", 43).is_ok());
        assert!(data_store.flag("flag-key").is_none());
    }

    #[test]
    fn in_memory_can_delete_segment() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        assert!(data_store.segment("segment-key").is_some());
        assert!(data_store.delete("/segments/segment-key", 0).is_ok());
        assert!(data_store.segment("segment-key").is_some());

        assert!(data_store.delete("/segments/segment-key", 1).is_ok());
        assert!(data_store.segment("segment-key").is_none());

        data_store.init(basic_data());

        assert!(data_store.delete("/segments/segment-key", 2).is_ok());
        assert!(data_store.segment("segment-key").is_none());
    }
}
