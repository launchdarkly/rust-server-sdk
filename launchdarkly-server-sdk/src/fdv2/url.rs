use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

use super::model::Selector;

/// Percent-encoding set for URL query values: encodes everything except
/// RFC 3986 unreserved characters (A-Za-z0-9 and `- . _ ~`).
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

pub(super) fn is_valid_filter_key(k: &str) -> bool {
    let mut chars = k.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

pub(super) fn build_fdv2_url(
    base_url: &str,
    endpoint_path: &str,
    selector: &Selector,
    filter_key: Option<&str>,
) -> String {
    let trimmed = base_url.trim_end_matches('/');
    let mut url = format!("{trimmed}/{endpoint_path}");

    let mut sep = '?';
    if let Some(state) = selector.as_deref() {
        url.push(sep);
        url.push_str("basis=");
        url.push_str(&utf8_percent_encode(state, QUERY_VALUE).to_string());
        sep = '&';
    }
    if let Some(filter) = filter_key {
        if is_valid_filter_key(filter) {
            url.push(sep);
            url.push_str("filter=");
            url.push_str(&utf8_percent_encode(filter, QUERY_VALUE).to_string());
        } else {
            warn!("data source config: filter key '{filter}' is invalid, requesting full environment instead");
        }
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_with_subpath_trailing_slash_joins_cleanly() {
        let url = build_fdv2_url("http://example.com/relay/", "sdk/poll", &None, None);
        assert_eq!(url, "http://example.com/relay/sdk/poll");
    }

    #[test]
    fn endpoint_path_is_included_verbatim() {
        let url = build_fdv2_url("http://example.com", "sdk/stream", &None, None);
        assert_eq!(url, "http://example.com/sdk/stream");
    }

    #[test]
    fn valid_filter_key_is_included() {
        let url = build_fdv2_url(
            "http://example.com",
            "sdk/poll",
            &None,
            Some("my-filter_1.0"),
        );
        assert_eq!(url, "http://example.com/sdk/poll?filter=my-filter_1.0");
    }

    #[test]
    fn invalid_filter_key_is_dropped() {
        let url = build_fdv2_url("http://example.com", "sdk/poll", &None, Some("has spaces"));
        assert_eq!(url, "http://example.com/sdk/poll");
    }

    #[test]
    fn selector_state_is_percent_encoded() {
        let selector = Some("a&b".to_string());
        let url = build_fdv2_url("http://example.com", "sdk/poll", &selector, None);
        assert_eq!(url, "http://example.com/sdk/poll?basis=a%26b");
    }

    #[test]
    fn selector_with_reserved_chars_is_encoded() {
        let selector = Some("(p:abc:52)".to_string());
        let url = build_fdv2_url("http://example.com", "sdk/poll", &selector, None);
        assert_eq!(url, "http://example.com/sdk/poll?basis=%28p%3Aabc%3A52%29");
    }

    #[test]
    fn selector_and_filter_both_included() {
        let selector = Some("state-1".to_string());
        let url = build_fdv2_url(
            "http://example.com",
            "sdk/poll",
            &selector,
            Some("my-filter"),
        );
        assert_eq!(
            url,
            "http://example.com/sdk/poll?basis=state-1&filter=my-filter"
        );
    }
}
