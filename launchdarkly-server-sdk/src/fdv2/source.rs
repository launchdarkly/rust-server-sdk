use std::time::Duration;

use futures::future::BoxFuture;
use http::HeaderMap;

use crate::stores::change_set::ChangeSet;

use super::model::Selector;

pub(super) const FALLBACK_HEADER: &str = "X-LD-FD-Fallback";
pub(super) const FALLBACK_TTL_HEADER: &str = "X-LD-FD-Fallback-TTL";
pub(super) const DEFAULT_FALLBACK_TTL: Duration = Duration::from_secs(60 * 60);

/// The HTTP headers attached to every FDv2 request.
#[derive(Clone)]
pub(crate) struct RequestHeaders {
    headers: Vec<(&'static str, String)>,
}

impl RequestHeaders {
    pub(crate) fn new(sdk_key: &str, tags: Option<&str>, instance_id: &str) -> Self {
        let mut headers = vec![
            ("Authorization", sdk_key.to_string()),
            ("User-Agent", crate::USER_AGENT.clone()),
            (
                crate::LAUNCHDARKLY_INSTANCE_ID_HEADER,
                instance_id.to_string(),
            ),
        ];
        if let Some(tags) = tags {
            headers.push((crate::LAUNCHDARKLY_TAGS_HEADER, tags.to_string()));
        }
        Self { headers }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&'static str, &str)> + '_ {
        self.headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
    }
}

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

pub(super) fn read_fallback_directive(headers: &HeaderMap) -> Option<FDv1FallbackDirective> {
    let flag = headers.get(FALLBACK_HEADER)?;
    if !flag.to_str().is_ok_and(|s| s.eq_ignore_ascii_case("true")) {
        return None;
    }
    let ttl = headers
        .get(FALLBACK_TTL_HEADER)
        .and_then(|v| v.to_str().ok())
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

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn fallback_absent_header_returns_none() {
        assert!(read_fallback_directive(&HeaderMap::new()).is_none());
    }

    #[test]
    fn fallback_header_value_other_than_true_returns_none() {
        let h = headers(&[("X-LD-FD-Fallback", "false")]);
        assert!(read_fallback_directive(&h).is_none());
    }

    #[test]
    fn fallback_header_uppercase_true_uses_default_ttl() {
        let h = headers(&[("X-LD-FD-Fallback", "TRUE")]);
        let d = read_fallback_directive(&h).expect("directive");
        assert_eq!(d.ttl, DEFAULT_FALLBACK_TTL);
    }

    #[test]
    fn fallback_ttl_header_is_parsed() {
        let h = headers(&[("X-LD-FD-Fallback", "true"), ("X-LD-FD-Fallback-TTL", "60")]);
        let d = read_fallback_directive(&h).expect("directive");
        assert_eq!(d.ttl, Duration::from_secs(60));
    }

    #[test]
    fn fallback_ttl_header_malformed_uses_default() {
        let h = headers(&[
            ("X-LD-FD-Fallback", "true"),
            ("X-LD-FD-Fallback-TTL", "not-a-number"),
        ]);
        let d = read_fallback_directive(&h).expect("directive");
        assert_eq!(d.ttl, DEFAULT_FALLBACK_TTL);
    }
}
