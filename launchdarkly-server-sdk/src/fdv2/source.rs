use std::time::Duration;

use futures::future::BoxFuture;
use rand::Rng;

use crate::stores::change_set::ChangeSet;

use super::model::Selector;

const FALLBACK_HEADER: &str = "X-LD-FD-Fallback";
const FALLBACK_TTL_HEADER: &str = "X-LD-FD-Fallback-TTL";
const DEFAULT_FALLBACK_TTL: Duration = Duration::from_secs(60 * 60);
/// The longest server-supplied fallback TTL that is honored; longer values fall back to the default.
const MAX_FALLBACK_TTL: Duration = Duration::from_secs(60 * 60);

/// Classifies why a data source is interrupted or has failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// The cause is not one of the more specific kinds.
    Unknown,
    /// The request failed at the network layer.
    NetworkError,
    /// The server returned an error status code.
    ErrorResponse {
        /// The HTTP status code.
        status_code: u16,
    },
    /// The response could not be parsed.
    InvalidData,
}

/// Describes an error surfaced by a data source.
#[derive(Debug, Clone)]
pub struct ErrorInfo {
    /// The kind of error.
    pub kind: ErrorKind,
    /// A human-readable description.
    pub message: String,
}

/// An instruction from LaunchDarkly to fall back to the FDv1 protocol.
#[derive(Debug, Clone)]
pub struct FDv1FallbackDirective {
    /// How long to stay on FDv1 before retrying FDv2.
    pub ttl: Duration,
}

impl FDv1FallbackDirective {
    /// Builds a directive from a server-supplied TTL. A value in the `(0, 1 hour]`
    /// range is honored as-is; anything else -- absent, zero, or longer than an
    /// hour -- uses a jittered one-hour default.
    pub(super) fn from_ttl(ttl: Option<Duration>) -> Self {
        let ttl = match ttl {
            Some(t) if t > Duration::ZERO && t <= MAX_FALLBACK_TTL => t,
            _ => jittered_default_ttl(),
        };
        Self { ttl }
    }
}

/// Jitters the default TTL down by up to half so a fleet that all hits the default
/// doesn't retry FDv2 in lockstep. Server-supplied TTLs are jittered upstream and
/// are left untouched.
fn jittered_default_ttl() -> Duration {
    DEFAULT_FALLBACK_TTL.mul_f64(rand::rng().random_range(0.5..=1.0))
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
        .map(Duration::from_secs);
    Some(FDv1FallbackDirective::from_ttl(ttl))
}

/// The outcome of a single data source poll or stream read.
#[derive(Debug)]
pub enum FDv2SourceResult {
    /// A set of flag and segment changes.
    ChangeSet(ChangeSet),
    /// A transient failure; the source may recover.
    Interrupted(ErrorInfo),
    /// An unrecoverable failure; the source is done.
    TerminalError(ErrorInfo),
    /// The server asked the source to disconnect.
    Goodbye,
}

/// A source result paired with any FDv1 fallback directive seen on the same response.
#[derive(Debug)]
pub struct FDv2SourceEvent {
    /// The source result.
    pub result: FDv2SourceResult,
    /// Present when the response carried an FDv1 fallback directive.
    pub fdv1_fallback: Option<FDv1FallbackDirective>,
}

/// A data source that can obtain an initial payload.
pub trait Initializer: Send {
    /// Runs once to obtain an initial payload.
    fn run(&mut self) -> BoxFuture<'_, FDv2SourceEvent>;
    /// The name used in logs.
    fn name(&self) -> &str;
}

/// A data source that keeps flag data up to date.
pub trait Synchronizer: Send {
    /// Fetches the next batch of changes after the given selector.
    fn next(&mut self, selector: Selector) -> BoxFuture<'_, FDv2SourceEvent>;
    /// The name used in logs.
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

    // The default is jittered down by up to half, so it lands in [30 min, 1 hour].
    fn assert_is_jittered_default(ttl: Duration) {
        assert!(ttl <= DEFAULT_FALLBACK_TTL && ttl >= DEFAULT_FALLBACK_TTL / 2);
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
        assert_is_jittered_default(d.ttl);
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
        assert_is_jittered_default(d.ttl);
    }

    #[test]
    fn fallback_ttl_zero_uses_default() {
        let d = read_fallback_directive(headers(&[
            ("X-LD-FD-Fallback", "true"),
            ("X-LD-FD-Fallback-TTL", "0"),
        ]))
        .expect("directive");
        assert_is_jittered_default(d.ttl);
    }

    #[test]
    fn fallback_ttl_longer_than_an_hour_uses_default() {
        let d = read_fallback_directive(headers(&[
            ("X-LD-FD-Fallback", "true"),
            ("X-LD-FD-Fallback-TTL", "3601"),
        ]))
        .expect("directive");
        assert_is_jittered_default(d.ttl);
    }
}
