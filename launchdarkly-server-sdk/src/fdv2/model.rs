use super::wire::{DeleteObject, PutObject};

pub(crate) type Selector = Option<String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeSetKind {
    None,
    Full,
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
