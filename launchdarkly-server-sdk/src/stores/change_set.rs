use launchdarkly_server_sdk_evaluation::{Flag, Segment};

use crate::fdv2::model::{ChangeSetKind, Selector};

use super::store_types::StorageItem;

#[derive(Debug)]
pub(crate) enum ItemChange {
    Flag {
        key: String,
        item: StorageItem<Flag>,
    },
    Segment {
        key: String,
        item: StorageItem<Segment>,
    },
}

#[derive(Debug)]
pub(crate) struct ChangeSet {
    pub(crate) kind: ChangeSetKind,
    pub(crate) changes: Vec<ItemChange>,
    #[allow(dead_code)] // Read by the orchestrator in a later phase.
    pub(crate) selector: Selector,
}
