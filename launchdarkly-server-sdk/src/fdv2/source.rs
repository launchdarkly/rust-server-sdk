use std::time::Duration;

use futures::future::BoxFuture;

use crate::stores::change_set::ChangeSet;

use super::model::Selector;

pub(super) const FALLBACK_HEADER: &str = "X-LD-FD-Fallback";
pub(super) const FALLBACK_TTL_HEADER: &str = "X-LD-FD-Fallback-TTL";
pub(super) const DEFAULT_FALLBACK_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    Unknown,
    NetworkError,
    ErrorResponse { status_code: u16 },
    InvalidData,
}

#[derive(Debug, Clone)]
pub(crate) struct ErrorInfo {
    #[allow(dead_code)] // Will be read by data source status provider, once implemented.
    pub(crate) kind: ErrorKind,
    pub(crate) message: String,
}

#[derive(Debug)]
pub(crate) struct FDv1FallbackDirective {
    pub(crate) ttl: Duration,
}

pub(super) fn read_fallback_directive<'a>(
    lookup: impl Fn(&str) -> Option<&'a str>,
) -> Option<FDv1FallbackDirective> {
    let flag = lookup(FALLBACK_HEADER)?;
    if !flag.eq_ignore_ascii_case("true") {
        return None;
    }
    let ttl = lookup(FALLBACK_TTL_HEADER)
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_FALLBACK_TTL);
    Some(FDv1FallbackDirective { ttl })
}

#[derive(Debug)]
pub(crate) enum FDv2SourceResult {
    ChangeSet(ChangeSet),
    Interrupted(ErrorInfo),
    TerminalError(ErrorInfo),
    Shutdown,
    Goodbye,
}

#[derive(Debug)]
pub(crate) struct FDv2SourceEvent {
    pub(crate) result: FDv2SourceResult,
    pub(crate) fdv1_fallback: Option<FDv1FallbackDirective>,
}

pub(crate) trait Initializer: Send {
    fn run(&mut self) -> BoxFuture<'_, FDv2SourceEvent>;
    fn name(&self) -> &str;
}

pub(crate) trait Synchronizer: Send {
    fn next(&mut self, selector: Selector) -> BoxFuture<'_, FDv2SourceEvent>;
    fn name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn headers<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<&'a str> + 'a {
        let map: HashMap<&'a str, &'a str> = pairs.iter().copied().collect();
        move |k| map.get(k).copied()
    }

    #[test]
    fn fallback_absent_header_returns_none() {
        assert!(read_fallback_directive(headers(&[])).is_none());
    }

    #[test]
    fn fallback_header_value_other_than_true_returns_none() {
        assert!(read_fallback_directive(headers(&[("X-LD-FD-Fallback", "false")])).is_none());
    }

    #[test]
    fn fallback_header_uppercase_true_uses_default_ttl() {
        let d =
            read_fallback_directive(headers(&[("X-LD-FD-Fallback", "TRUE")])).expect("directive");
        assert_eq!(d.ttl, DEFAULT_FALLBACK_TTL);
    }

    #[test]
    fn fallback_ttl_header_is_parsed() {
        let d = read_fallback_directive(headers(&[
            ("X-LD-FD-Fallback", "true"),
            ("X-LD-FD-Fallback-TTL", "60"),
        ]))
        .expect("directive");
        assert_eq!(d.ttl, Duration::from_secs(60));
    }

    #[test]
    fn fallback_ttl_header_malformed_uses_default() {
        let d = read_fallback_directive(headers(&[
            ("X-LD-FD-Fallback", "true"),
            ("X-LD-FD-Fallback-TTL", "not-a-number"),
        ]))
        .expect("directive");
        assert_eq!(d.ttl, DEFAULT_FALLBACK_TTL);
    }
}
