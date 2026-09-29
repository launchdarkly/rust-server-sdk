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

/// The tombstone this SDK writes to mark an item deleted.
#[derive(Serialize)]
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

/// Supports deserializing tombstone values produced by other SDKs.
#[derive(Deserialize)]
struct DeserializedTombstone {
    #[serde(default)]
    version: Option<u64>,
    #[serde(default)]
    deleted: bool,
}

impl SerializedItem {
    /// Returns the version of the tombstone this item holds, or None if it is not a tombstone.
    ///
    /// A tombstone is recognized by `deleted` being true, either on the item itself or in the
    /// stored body. The body wins on version, because a store that only keeps the body cannot
    /// report a version out of band.
    fn tombstone_version(&self) -> Option<u64> {
        if self.deleted {
            return Some(self.version);
        }

        let tombstone: DeserializedTombstone = serde_json::from_str(&self.serialized_item).ok()?;

        if !tombstone.deleted {
            return None;
        }

        Some(tombstone.version.unwrap_or(self.version))
    }
}

impl TryInto<StorageItem<Flag>> for SerializedItem {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<StorageItem<Flag>, Self::Error> {
        // Check for a tombstone first. Flag has no `deleted` field and does not reject unknown
        // fields, so a tombstone written as a complete flag object would otherwise parse as a
        // live flag and resurrect a deleted flag.
        if let Some(version) = self.tombstone_version() {
            return Ok(StorageItem::Tombstone(version));
        }

        serde_json::from_str(&self.serialized_item).map(StorageItem::Item)
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
        // Check for a tombstone first, for the same reason as the Flag conversion above.
        if let Some(version) = self.tombstone_version() {
            return Ok(StorageItem::Tombstone(version));
        }

        serde_json::from_str(&self.serialized_item).map(StorageItem::Item)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::{TryFrom, TryInto};

    use launchdarkly_server_sdk_evaluation::{Flag, Segment};

    use crate::{
        test_common::{basic_flag, basic_segment, flag_tombstone_shapes, segment_tombstone_shapes},
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

    /// Wraps a stored body the way a real persistent store hands it back. Redis, DynamoDB and
    /// Consul keep only the body, so they cannot report `deleted` out of band and always pass
    /// false.
    fn stored(body: &str) -> SerializedItem {
        SerializedItem {
            version: 0,
            deleted: false,
            serialized_item: body.to_string(),
        }
    }

    #[test]
    fn every_flag_tombstone_shape_is_read_as_a_tombstone() {
        for shape in flag_tombstone_shapes("my-flag") {
            let result: Result<StorageItem<Flag>, serde_json::Error> =
                stored(&shape.body).try_into();

            match result {
                Ok(StorageItem::Tombstone(version)) => assert_eq!(
                    shape.version, version,
                    "wrong version for flag tombstone shape: {}",
                    shape.name
                ),
                Ok(StorageItem::Item(flag)) => panic!(
                    "flag tombstone shape read as a live flag (key {:?}): {}",
                    flag.key, shape.name
                ),
                Err(e) => panic!("flag tombstone shape failed to parse: {} ({e})", shape.name),
            }
        }
    }

    #[test]
    fn every_segment_tombstone_shape_is_read_as_a_tombstone() {
        for shape in segment_tombstone_shapes("my-segment") {
            let result: Result<StorageItem<Segment>, serde_json::Error> =
                stored(&shape.body).try_into();

            match result {
                Ok(StorageItem::Tombstone(version)) => assert_eq!(
                    shape.version, version,
                    "wrong version for segment tombstone shape: {}",
                    shape.name
                ),
                Ok(StorageItem::Item(segment)) => panic!(
                    "segment tombstone shape read as a live segment (key {:?}): {}",
                    segment.key, shape.name
                ),
                Err(e) => panic!(
                    "segment tombstone shape failed to parse: {} ({e})",
                    shape.name
                ),
            }
        }
    }

    #[test]
    fn tombstone_without_a_version_uses_the_stored_version() {
        let serialized_item = SerializedItem {
            version: 77,
            deleted: false,
            serialized_item: r#"{"deleted": true}"#.to_string(),
        };

        let result: Result<StorageItem<Flag>, serde_json::Error> = serialized_item.try_into();

        match result {
            Ok(StorageItem::Tombstone(version)) => assert_eq!(77, version),
            _ => panic!("tombstone without a version failed to read"),
        }
    }

    #[test]
    fn live_flag_is_not_read_as_a_tombstone() {
        let flag = basic_flag("my-flag");
        let serialized_item = SerializedItem::try_from(StorageItem::Item(flag.clone())).unwrap();
        let body = serialized_item.serialized_item.clone();

        let result: Result<StorageItem<Flag>, serde_json::Error> = stored(&body).try_into();

        match result {
            Ok(StorageItem::Item(read)) => assert_eq!(flag.key, read.key),
            _ => panic!("live flag was not read as a live flag"),
        }
    }

    #[test]
    fn flag_marked_not_deleted_is_read_as_a_live_flag() {
        let body = r#"{
            "key": "my-flag",
            "version": 42,
            "on": true,
            "targets": [],
            "rules": [],
            "prerequisites": [],
            "fallthrough": {"variation": 1},
            "offVariation": 0,
            "variations": [false, true],
            "salt": "kosher",
            "deleted": false
        }"#;

        let result: Result<StorageItem<Flag>, serde_json::Error> = stored(body).try_into();

        match result {
            Ok(StorageItem::Item(flag)) => assert_eq!("my-flag", flag.key),
            _ => panic!("flag marked not deleted was not read as a live flag"),
        }
    }

    #[test]
    fn segment_marked_not_deleted_is_read_as_a_live_segment() {
        let body = r#"{
            "key": "my-segment",
            "version": 1,
            "included": ["alice"],
            "excluded": [],
            "rules": [],
            "salt": "salty",
            "deleted": false
        }"#;

        let result: Result<StorageItem<Segment>, serde_json::Error> = stored(body).try_into();

        match result {
            Ok(StorageItem::Item(segment)) => assert_eq!("my-segment", segment.key),
            _ => panic!("segment marked not deleted was not read as a live segment"),
        }
    }

    #[test]
    fn corrupt_body_still_reports_the_item_parse_error() {
        for body in [
            r#"{"key": "my-flag"}"#,
            r#"{}"#,
            r#"[1, 2, 3]"#,
            r#""nope""#,
        ] {
            let result: Result<StorageItem<Flag>, serde_json::Error> = stored(body).try_into();
            assert!(result.is_err(), "corrupt body was accepted: {body}");
        }
    }
}
