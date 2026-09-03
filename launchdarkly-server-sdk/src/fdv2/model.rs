use launchdarkly_server_sdk_evaluation::{Flag, Segment};

use crate::stores::change_set::{self, ItemChange};
use crate::stores::store_types::StorageItem;

use super::wire::{DeleteObject, PutObject};

/// Identifies a point in the flag-data stream, echoed back to request the next changes.
pub type Selector = Option<String>;

/// Whether a change set is a full payload, an incremental update, or carries no changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeSetKind {
    /// The change set carries no changes.
    None,
    /// The change set is a complete payload that replaces all data.
    Full,
    /// The change set is an incremental update to existing data.
    Partial,
}

#[derive(Debug)]
pub(super) enum FDv2Change {
    Put(PutObject),
    Delete(DeleteObject),
}

#[derive(Debug)]
pub(super) struct ChangeSet {
    pub(super) kind: ChangeSetKind,
    pub(super) changes: Vec<FDv2Change>,
    pub(super) selector: Selector,
}

impl TryFrom<ChangeSet> for change_set::ChangeSet {
    type Error = serde_json::Error;

    fn try_from(wire_cs: ChangeSet) -> Result<Self, Self::Error> {
        let ChangeSet {
            kind,
            changes,
            selector,
        } = wire_cs;

        let mut translated = Vec::with_capacity(changes.len());
        for change in changes {
            match change {
                FDv2Change::Put(put) => match put.kind.as_str() {
                    "flag" => translated.push(ItemChange::Flag {
                        key: put.key,
                        item: StorageItem::Item(serde_json::from_value::<Flag>(put.object)?),
                    }),
                    "segment" => translated.push(ItemChange::Segment {
                        key: put.key,
                        item: StorageItem::Item(serde_json::from_value::<Segment>(put.object)?),
                    }),
                    other => warn!("FDv2: unknown kind '{other}' in put-object, skipping"),
                },
                FDv2Change::Delete(del) => match del.kind.as_str() {
                    "flag" => translated.push(ItemChange::Flag {
                        key: del.key,
                        item: StorageItem::Tombstone(del.version),
                    }),
                    "segment" => translated.push(ItemChange::Segment {
                        key: del.key,
                        item: StorageItem::Tombstone(del.version),
                    }),
                    other => warn!("FDv2: unknown kind '{other}' in delete-object, skipping"),
                },
            }
        }

        Ok(change_set::ChangeSet {
            kind,
            changes: translated,
            selector,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common::{basic_flag, basic_segment};
    use serde_json::json;

    fn put(kind: &str, key: &str, version: u64, object: serde_json::Value) -> FDv2Change {
        FDv2Change::Put(PutObject {
            version,
            kind: kind.into(),
            key: key.into(),
            object,
        })
    }

    fn delete(kind: &str, key: &str, version: u64) -> FDv2Change {
        FDv2Change::Delete(DeleteObject {
            version,
            kind: kind.into(),
            key: key.into(),
        })
    }

    #[test]
    fn none_kind_translates_to_empty_changeset() {
        let wire = ChangeSet {
            kind: ChangeSetKind::None,
            changes: vec![],
            selector: Some("s-1".into()),
        };
        let out = change_set::ChangeSet::try_from(wire).unwrap();
        assert_eq!(out.kind, ChangeSetKind::None);
        assert!(out.changes.is_empty());
        assert_eq!(out.selector.as_deref(), Some("s-1"));
    }

    #[test]
    fn typed_puts_and_deletes_pass_through() {
        let flag_json = serde_json::to_value(basic_flag("f1")).unwrap();
        let segment_json = serde_json::to_value(basic_segment("s1")).unwrap();
        let wire = ChangeSet {
            kind: ChangeSetKind::Full,
            changes: vec![
                put("flag", "f1", 1, flag_json),
                put("segment", "s1", 2, segment_json),
                delete("flag", "old", 3),
            ],
            selector: Some("s-full".into()),
        };
        let out = change_set::ChangeSet::try_from(wire).unwrap();
        assert_eq!(out.changes.len(), 3);
        assert!(
            matches!(&out.changes[0], ItemChange::Flag { key, item: StorageItem::Item(f) }
            if key == "f1" && f.key == "f1")
        );
        assert!(
            matches!(&out.changes[1], ItemChange::Segment { key, item: StorageItem::Item(s) }
            if key == "s1" && s.key == "s1")
        );
        assert!(
            matches!(&out.changes[2], ItemChange::Flag { key, item: StorageItem::Tombstone(3) }
            if key == "old")
        );
    }

    #[test]
    fn unknown_kind_is_dropped() {
        let wire = ChangeSet {
            kind: ChangeSetKind::Partial,
            changes: vec![put("mystery", "k", 1, json!({}))],
            selector: None,
        };
        let out = change_set::ChangeSet::try_from(wire).unwrap();
        assert!(out.changes.is_empty());
    }

    #[test]
    fn parse_failure_returns_err() {
        let wire = ChangeSet {
            kind: ChangeSetKind::Full,
            changes: vec![put("flag", "bad", 1, json!({"garbage": true}))],
            selector: None,
        };
        assert!(change_set::ChangeSet::try_from(wire).is_err());
    }
}
