//! Compression support for polling requests.
//!
//! The SDK decodes polling responses above the transport. This keeps custom transports working,
//! because a transport returns the raw body and does not decode it.

use http::HeaderMap;

/// The value of the Accept-Encoding header on polling requests.
///
/// The value is `None` when the SDK cannot decode a compressed response. The SDK then does not
/// send the header.
#[cfg(feature = "polling-compression")]
pub(crate) const ACCEPT_ENCODING: Option<&str> = Some("gzip");
#[cfg(not(feature = "polling-compression"))]
pub(crate) const ACCEPT_ENCODING: Option<&str> = None;

/// Decodes a polling response body as its Content-Encoding header specifies.
///
/// The function returns the body unchanged when the response does not use gzip.
#[cfg(feature = "polling-compression")]
pub(crate) fn decode_body(headers: &HeaderMap, body: Vec<u8>) -> std::io::Result<Vec<u8>> {
    use std::io::Read;

    let is_gzip = headers
        .get(http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("gzip"));
    if !is_gzip {
        return Ok(body);
    }

    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(body.as_slice()).read_to_end(&mut decoded)?;
    Ok(decoded)
}

/// Returns the body unchanged, because the SDK does not request a compressed response.
#[cfg(not(feature = "polling-compression"))]
pub(crate) fn decode_body(_headers: &HeaderMap, body: Vec<u8>) -> std::io::Result<Vec<u8>> {
    Ok(body)
}

#[cfg(all(test, feature = "polling-compression"))]
pub(crate) fn gzip(data: &[u8]) -> Vec<u8> {
    use std::io::Write;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

#[cfg(all(test, feature = "polling-compression"))]
mod tests {
    use super::*;
    use test_case::test_case;

    fn headers(content_encoding: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(value) = content_encoding {
            headers.insert(http::header::CONTENT_ENCODING, value.parse().unwrap());
        }
        headers
    }

    #[test_case("gzip")]
    #[test_case("GZIP")]
    #[test_case("Gzip")]
    #[test_case(" gzip ")]
    fn decodes_gzip_body(content_encoding: &str) {
        let body = gzip(b"{\"flags\":{}}");
        let decoded = decode_body(&headers(Some(content_encoding)), body).unwrap();
        assert_eq!(decoded, b"{\"flags\":{}}");
    }

    #[test_case(None)]
    #[test_case(Some("identity"))]
    fn passes_through_uncompressed_body(content_encoding: Option<&str>) {
        let decoded = decode_body(&headers(content_encoding), b"plain".to_vec()).unwrap();
        assert_eq!(decoded, b"plain");
    }

    #[test]
    fn corrupt_gzip_body_returns_error() {
        assert!(decode_body(&headers(Some("gzip")), b"not gzip".to_vec()).is_err());
    }
}
