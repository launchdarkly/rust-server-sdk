use std::convert::TryFrom;
use std::{collections::HashMap, convert::TryInto};

use launchdarkly_server_sdk_evaluation::{Flag, Segment, Versioned};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum PatchTarget {
    Flag(StorageItem<Flag>),
    Segment(StorageItem<Segment>),
    Other(serde_json::Value),
}

/// Enum to denote whether the item is a valid type T or if it is a tombstone place holder used to
/// show the item T was deleted.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum StorageItem<T> {
    /// Variant denoted a valid item of type T.
    Item(T),
    /// Marker showing that the item of type T was deleted at some point in the past with the
    /// provided version.
    Tombstone(u64),
}

impl<T> From<T> for StorageItem<T> {
    fn from(flag: T) -> Self {
        Self::Item(flag)
    }
}

impl<T> From<StorageItem<T>> for Option<T> {
    fn from(val: StorageItem<T>) -> Self {
        match val {
            StorageItem::Item(i) => Some(i),
            _ => None,
        }
    }
}

impl<T: Versioned> Versioned for StorageItem<T> {
    fn version(&self) -> u64 {
        match self {
            Self::Item(i) => i.version(),
            Self::Tombstone(version) => *version,
        }
    }

    fn is_greater_than_or_equal(&self, version: u64) -> bool {
        self.version() >= version
    }
}

/// Used to hold store information and initial payloads from LaunchDarkly.
#[derive(Clone, Debug, Deserialize)]
pub struct AllData<F, S> {
    /// All flag information indexed by flag key.
    pub flags: HashMap<String, F>,
    /// All segment information indexed by segment key.
    pub segments: HashMap<String, S>,
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

impl TryFrom<AllData<Flag, Segment>> for AllData<SerializedItem, SerializedItem> {
    type Error = serde_json::Error;

    fn try_from(all_data: AllData<Flag, Segment>) -> Result<Self, Self::Error> {
        let flags: Result<HashMap<String, SerializedItem>, Self::Error> = all_data
            .flags
            .into_iter()
            .map(|(key, flag)| {
                let item = StorageItem::Item(flag);

                match SerializedItem::try_from(item) {
                    Ok(serialized_item) => Ok((key, serialized_item)),
                    Err(e) => Err(e),
                }
            })
            .collect();

        let segments: Result<HashMap<String, SerializedItem>, Self::Error> = all_data
            .segments
            .into_iter()
            .map(|(key, segment)| {
                let item = StorageItem::Item(segment);

                match SerializedItem::try_from(item) {
                    Ok(serialized_item) => Ok((key, serialized_item)),
                    Err(e) => Err(e),
                }
            })
            .collect();

        let flags = flags?;
        let segments = segments?;

        Ok(AllData { flags, segments })
    }
}

/// Enum which denotes the kind of data that may be persisted in our data stores.
pub enum DataKind {
    /// A feature flag
    Flag,
    /// A user segment
    Segment,
}

/// A serialized item representing either a flag or a segment.
#[derive(Clone)]
pub struct SerializedItem {
    /// The version of the underlying item.
    pub version: u64,
    /// Whether or not the serialized item is a tombstone or an actual item.
    pub deleted: bool,
    /// The serialized JSON representation of the underlying item.
    pub serialized_item: String,
}

#[derive(Serialize, Deserialize)]
struct SerializedTombstone {
    version: u64,
    key: String,
    deleted: bool,
}

impl SerializedTombstone {
    fn new(version: u64) -> Self {
        Self {
            version,
            key: "$deleted".to_string(),
            deleted: true,
        }
    }
}

impl TryInto<StorageItem<Flag>> for SerializedItem {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<StorageItem<Flag>, Self::Error> {
        if self.deleted {
            return Ok(StorageItem::Tombstone(self.version));
        }

        let flag_result: Result<Flag, serde_json::Error> =
            serde_json::from_str(&self.serialized_item);

        match flag_result {
            Ok(flag) => Ok(StorageItem::Item(flag)),
            Err(e) => {
                let serialized_tombstone: Result<SerializedTombstone, serde_json::Error> =
                    serde_json::from_str(&self.serialized_item);

                match serialized_tombstone {
                    Ok(tombstone) if tombstone.key == "$deleted" && tombstone.deleted => {
                        Ok(StorageItem::Tombstone(tombstone.version))
                    }
                    _ => Err(e),
                }
            }
        }
    }
}

pub trait SerializeToSerializedItem {
    fn serialize_to_serialized_item(self) -> Result<SerializedItem, serde_json::Error>;
}

impl<T: Versioned> SerializeToSerializedItem for StorageItem<T>
where
    T: Serialize,
{
    fn serialize_to_serialized_item(self) -> Result<SerializedItem, serde_json::Error> {
        match self {
            StorageItem::Item(flag) => Ok(SerializedItem {
                version: flag.version(),
                deleted: false,
                serialized_item: serde_json::to_string(&flag)?,
            }),
            StorageItem::Tombstone(version) => Ok(SerializedItem {
                version,
                deleted: true,
                serialized_item: json!({
                    "version": version,
                    "key": "$deleted",
                    "deleted": true
                })
                .to_string(),
            }),
        }
    }
}

impl<T> TryFrom<StorageItem<T>> for SerializedItem
where
    T: Versioned,
    T: Serialize,
{
    type Error = serde_json::Error;

    fn try_from(storage_item: StorageItem<T>) -> Result<Self, Self::Error> {
        match storage_item {
            StorageItem::Item(item) => Ok(SerializedItem {
                version: item.version(),
                deleted: false,
                serialized_item: serde_json::to_string(&item)?,
            }),
            StorageItem::Tombstone(version) => {
                let tombstone = SerializedTombstone::new(version);
                Ok(SerializedItem {
                    version,
                    deleted: true,
                    serialized_item: serde_json::to_string(&tombstone)?,
                })
            }
        }
    }
}

impl TryInto<StorageItem<Segment>> for SerializedItem {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<StorageItem<Segment>, Self::Error> {
        if self.deleted {
            return Ok(StorageItem::Tombstone(self.version));
        }

        let segment_result: Result<Segment, serde_json::Error> =
            serde_json::from_str(&self.serialized_item);

        match segment_result {
            Ok(segment) => Ok(StorageItem::Item(segment)),
            Err(e) => {
                let serialized_tombstone: Result<SerializedTombstone, serde_json::Error> =
                    serde_json::from_str(&self.serialized_item);

                match serialized_tombstone {
                    Ok(tombstone) if tombstone.key == "$deleted" && tombstone.deleted => {
                        Ok(StorageItem::Tombstone(tombstone.version))
                    }
                    _ => Err(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::{TryFrom, TryInto};

    use launchdarkly_server_sdk_evaluation::{Flag, Segment};

    use crate::{
        test_common::{basic_flag, basic_segment},
        SerializedItem,
    };

    use super::StorageItem;

    #[test]
    fn flag_can_be_serialized_and_back() {
        let flag = basic_flag("flag-key");
        let item = StorageItem::Item(flag.clone());
        let result = SerializedItem::try_from(item);

        assert!(result.is_ok());

        let serialized_item = result.unwrap();

        assert_eq!(flag.version, serialized_item.version);
        assert!(!serialized_item.deleted);

        let result: Result<StorageItem<Flag>, serde_json::Error> = serialized_item.try_into();

        match result {
            Ok(StorageItem::Item(f)) => {
                assert_eq!(f.key, flag.key);
            }
            _ => panic!("Item failed to deserialize into flag"),
        }
    }

    #[test]
    fn flag_tombstone_can_be_serialized_and_back() {
        let item: StorageItem<Flag> = StorageItem::Tombstone(42);
        let result = SerializedItem::try_from(item);

        assert!(result.is_ok());

        let serialized_item = result.unwrap();

        assert_eq!(42, serialized_item.version);
        assert!(serialized_item.deleted);

        let result: Result<StorageItem<Flag>, serde_json::Error> = serialized_item.try_into();

        match result {
            Ok(StorageItem::Tombstone(v)) => {
                assert_eq!(v, 42);
            }
            _ => panic!("Item failed to deserialize into flag"),
        }
    }

    #[test]
    fn serialized_flag_with_0_version_uses_serialied_information() {
        let item: StorageItem<Flag> = StorageItem::Tombstone(42);
        let serialized = SerializedItem::try_from(item).unwrap();

        let serialized_item = SerializedItem {
            version: 0,
            deleted: false,
            serialized_item: serialized.serialized_item,
        };

        let result: Result<StorageItem<Flag>, serde_json::Error> = serialized_item.try_into();

        match result {
            Ok(StorageItem::Tombstone(v)) => {
                assert_eq!(v, 42);
            }
            _ => panic!("Item failed to deserialize into flag"),
        }
    }

    #[test]
    fn segment_can_be_serialized_and_back() {
        let segment = basic_segment("segment-key");
        let item = StorageItem::Item(segment.clone());
        let result = SerializedItem::try_from(item);

        assert!(result.is_ok());

        let serialized_item = result.unwrap();

        assert_eq!(segment.version, serialized_item.version);
        assert!(!serialized_item.deleted);

        let result: Result<StorageItem<Segment>, serde_json::Error> = serialized_item.try_into();

        match result {
            Ok(StorageItem::Item(f)) => {
                assert_eq!(f.key, segment.key);
            }
            _ => panic!("Item failed to deserialize into segment"),
        }
    }

    #[test]
    fn segment_tombstone_can_be_serialized_and_back() {
        let item: StorageItem<Segment> = StorageItem::Tombstone(42);
        let result = SerializedItem::try_from(item);

        assert!(result.is_ok());

        let serialized_item = result.unwrap();

        assert_eq!(42, serialized_item.version);
        assert!(serialized_item.deleted);

        let result: Result<StorageItem<Segment>, serde_json::Error> = serialized_item.try_into();

        match result {
            Ok(StorageItem::Tombstone(v)) => {
                assert_eq!(v, 42);
            }
            _ => panic!("Item failed to deserialize into segment"),
        }
    }

    #[test]
    fn serialized_segment_with_0_version_uses_serialied_information() {
        let item: StorageItem<Segment> = StorageItem::Tombstone(42);
        let serialized = SerializedItem::try_from(item).unwrap();

        let serialized_item = SerializedItem {
            version: 0,
            deleted: false,
            serialized_item: serialized.serialized_item,
        };

        let result: Result<StorageItem<Segment>, serde_json::Error> = serialized_item.try_into();

        match result {
            Ok(StorageItem::Tombstone(v)) => {
                assert_eq!(v, 42);
            }
            _ => panic!("Item failed to deserialize into segment"),
        }
    }
}
