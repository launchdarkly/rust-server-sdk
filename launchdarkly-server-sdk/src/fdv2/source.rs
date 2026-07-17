use std::time::Duration;

use futures::future::BoxFuture;

use crate::stores::change_set::ChangeSet;

use super::model::Selector;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    Unknown,
    NetworkError,
    ErrorResponse { status_code: u16 },
    InvalidData,
    StoreError,
}

#[derive(Debug, Clone)]
pub(crate) struct ErrorInfo {
    pub(crate) kind: ErrorKind,
    pub(crate) message: String,
}

#[derive(Debug)]
pub(crate) struct FDv1FallbackDirective {
    pub(crate) ttl: Duration,
}

#[derive(Debug)]
pub(crate) enum FDv2SourceResult {
    ChangeSet(ChangeSet),
    Interrupted(ErrorInfo),
    TerminalError(ErrorInfo),
    Shutdown,
    Goodbye { reason: Option<String> },
}

#[derive(Debug)]
pub(crate) struct FDv2SourceEvent {
    pub(crate) result: FDv2SourceResult,
    pub(crate) fdv1_fallback: Option<FDv1FallbackDirective>,
}

pub(crate) trait Initializer: Send + Sync {
    fn run(&mut self) -> BoxFuture<'_, FDv2SourceEvent>;
    fn name(&self) -> &str;
}

pub(crate) trait Synchronizer: Send + Sync {
    fn next(&mut self, selector: Selector) -> BoxFuture<'_, FDv2SourceEvent>;
    fn name(&self) -> &str;
}
