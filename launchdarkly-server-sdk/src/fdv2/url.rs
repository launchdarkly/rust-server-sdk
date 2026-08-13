use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

use super::model::Selector;

/// Percent-encoding set for URL query values: encodes everything except
/// RFC 3986 unreserved characters (A-Za-z0-9 and `- . _ ~`).
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

pub(super) fn build_fdv2_url(base_url: &str, endpoint_path: &str, selector: &Selector) -> String {
    let trimmed = base_url.trim_end_matches('/');
    let mut url = format!("{trimmed}/{endpoint_path}");

    if let Some(state) = selector.as_deref() {
        url.push_str("?basis=");
        url.push_str(&utf8_percent_encode(state, QUERY_VALUE).to_string());
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_with_subpath_trailing_slash_joins_cleanly() {
        let url = build_fdv2_url("http://example.com/relay/", "sdk/poll", &None);
        assert_eq!(url, "http://example.com/relay/sdk/poll");
    }

    #[test]
    fn endpoint_path_is_included_verbatim() {
        let url = build_fdv2_url("http://example.com", "sdk/stream", &None);
        assert_eq!(url, "http://example.com/sdk/stream");
    }

    #[test]
    fn selector_state_is_percent_encoded() {
        let selector = Some("a&b".to_string());
        let url = build_fdv2_url("http://example.com", "sdk/poll", &selector);
        assert_eq!(url, "http://example.com/sdk/poll?basis=a%26b");
    }

    #[test]
    fn selector_with_reserved_chars_is_encoded() {
        let selector = Some("(p:abc:52)".to_string());
        let url = build_fdv2_url("http://example.com", "sdk/poll", &selector);
        assert_eq!(url, "http://example.com/sdk/poll?basis=%28p%3Aabc%3A52%29");
    }
}
