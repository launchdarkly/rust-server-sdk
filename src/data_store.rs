use std::collections::HashMap;

use rust_server_sdk_evaluation::{Flag, Segment, Store};
use serde::Deserialize;

const FLAGS_PREFIX: &str = "/flags/";
const SEGMENTS_PREFIX: &str = "/segments/";

type Error = String; // TODO(ch108607) use an error enum

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum PatchTarget {
    Flag(Flag),
    Segment(Segment),
    Other(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize)]
pub struct AllData {
    flags: HashMap<String, Flag>,
    segments: HashMap<String, Segment>,
}

/// Trait for a data store that holds and updates feature flags and related data received by the
/// SDK.
pub trait DataStore: Store + Send {
    fn init(&mut self, new_data: AllData);
    fn all_flags(&self) -> &HashMap<String, Flag>;
    fn patch(&mut self, path: &str, data: PatchTarget) -> Result<(), Error>;
    fn patch_flag(&mut self, flag_key: &str, data: PatchTarget) -> Result<(), Error>;
    fn patch_segment(&mut self, segment_key: &str, data: PatchTarget) -> Result<(), Error>;
    fn delete(&mut self, path: &str) -> Result<(), Error>;
    fn to_store(&self) -> &dyn Store;
}

// TODO(ch108602) implement Error::ClientNotReady
/// Default implementation of the DataStore which holds information in an in-memory data store.
pub struct InMemoryDataStore {
    pub data: AllData,
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
}

impl Store for InMemoryDataStore {
    fn flag(&self, flag_key: &str) -> Option<&Flag> {
        self.data.flags.get(flag_key)
    }

    fn segment(&self, segment_key: &str) -> Option<&Segment> {
        self.data.segments.get(segment_key)
    }
}

impl DataStore for InMemoryDataStore {
    fn init(&mut self, new_data: AllData) {
        self.data = new_data;
    }

    fn all_flags(&self) -> &HashMap<String, Flag> {
        &self.data.flags
    }

    fn patch(&mut self, path: &str, data: PatchTarget) -> Result<(), Error> {
        if let Some(flag_key) = path.strip_prefix(FLAGS_PREFIX) {
            self.patch_flag(flag_key, data)
        } else if let Some(segment_key) = path.strip_prefix(SEGMENTS_PREFIX) {
            self.patch_segment(segment_key, data)
        } else {
            Err(format!("can't patch {}", path))
        }
    }

    fn patch_flag(&mut self, flag_key: &str, data: PatchTarget) -> Result<(), Error> {
        let flag = match data {
            PatchTarget::Flag(f) => Ok(f),
            PatchTarget::Segment(_) => Err("expected a flag, got a segment".to_string()),
            PatchTarget::Other(json) => Err(format!("couldn't parse JSON as a flag: {}", json)),
        }?;

        self.data.flags.insert(flag_key.to_string(), flag);
        Ok(())
    }

    fn patch_segment(&mut self, segment_key: &str, data: PatchTarget) -> Result<(), Error> {
        let segment = match data {
            PatchTarget::Segment(s) => Ok(s),
            PatchTarget::Flag(_) => Err("expected a segment, got a flag".to_string()),
            PatchTarget::Other(json) => Err(format!("couldn't parse JSON as a segment: {}", json)),
        }?;

        self.data.segments.insert(segment_key.to_string(), segment);
        Ok(())
    }

    fn delete(&mut self, path: &str) -> Result<(), Error> {
        if let Some(flag_key) = path.strip_prefix(FLAGS_PREFIX) {
            self.data.flags.remove(flag_key);
        } else if let Some(segment_key) = path.strip_prefix(SEGMENTS_PREFIX) {
            self.data.segments.remove(segment_key);
        } else {
            return Err(format!("can't delete {}", path));
        }

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

    fn basic_data() -> AllData {
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
        assert!(data_store.delete("/flags/flag-key").is_ok());
        assert!(data_store.flag("flag-key").is_none());
    }

    #[test]
    fn in_memory_can_delete_segment() {
        let mut data_store = InMemoryDataStore::new();
        data_store.init(basic_data());

        assert!(data_store.segment("segment-key").is_some());
        assert!(data_store.delete("/segments/segment-key").is_ok());
        assert!(data_store.segment("segment-key").is_none());
    }
}
