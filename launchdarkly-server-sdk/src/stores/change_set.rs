use launchdarkly_server_sdk_evaluation::{Flag, Segment};

use crate::fdv2::model::{ChangeSetKind, Selector};

use super::store_types::StorageItem;

/// A single flag or segment change within a change set.
#[derive(Debug)]
pub enum ItemChange {
    /// A flag was upserted or deleted.
    Flag {
        /// The flag key.
        key: String,
        /// The new flag value or a tombstone.
        item: StorageItem<Flag>,
    },
    /// A segment was upserted or deleted.
    Segment {
        /// The segment key.
        key: String,
        /// The new segment value or a tombstone.
        item: StorageItem<Segment>,
    },
}

/// A batch of flag and segment changes delivered by a data source.
#[derive(Debug)]
pub struct ChangeSet {
    /// Whether this is a full payload, an incremental update, or empty.
    pub kind: ChangeSetKind,
    /// The individual item changes.
    pub changes: Vec<ItemChange>,
    /// The selector to echo back on the next request.
    pub selector: Selector,
}
