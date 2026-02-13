use crate::reqwest::is_http_error_recoverable;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use launchdarkly_sdk_transport::HttpTransport;
use std::collections::HashMap;

use super::stores::store_types::AllData;
use launchdarkly_server_sdk_evaluation::{Flag, Segment};

#[derive(Debug, PartialEq, Eq)]
pub enum FeatureRequesterError {
    Temporary,
    Permanent,
}

#[derive(Clone, Debug)]
struct CachedEntry(AllData<Flag, Segment>, String);

pub trait FeatureRequester: Send {
    fn get_all(&mut self) -> BoxFuture<'_, Result<AllData<Flag, Segment>, FeatureRequesterError>>;
}

pub struct HttpFeatureRequester<T: HttpTransport> {
    transport: T,
    url: http::Uri,
    sdk_key: String,
    cache: Option<CachedEntry>,
    default_headers: HashMap<&'static str, String>,
}

impl<T: HttpTransport> HttpFeatureRequester<T> {
    pub fn new(
        transport: T,
        url: http::Uri,
        sdk_key: String,
        default_headers: HashMap<&'static str, String>,
    ) -> Self {
        Self {
            transport,
            url,
            sdk_key,
            cache: None,
            default_headers,
        }
    }
}

impl<T: HttpTransport> FeatureRequester for HttpFeatureRequester<T> {
    fn get_all(&mut self) -> BoxFuture<'_, Result<AllData<Flag, Segment>, FeatureRequesterError>> {
        Box::pin(async {
            let uri = self.url.clone();
            let key = self.sdk_key.clone();

            let transport = self.transport.clone();
            let cache = self.cache.clone();

            let mut request_builder = http::Request::builder()
                .uri(uri)
                .method("GET")
                .header("Content-Type", "application/json")
                .header("Authorization", key)
                .header("User-Agent", &*crate::USER_AGENT);

            for default_header in &self.default_headers {
                request_builder =
                    request_builder.header(*default_header.0, default_header.1.as_str());
            }

            if let Some(cache) = &self.cache {
                request_builder = request_builder.header("If-None-Match", cache.1.clone());
            }

            // Create empty body for GET request
            let request = request_builder.body(Some(Bytes::new())).unwrap();

            let result = transport.request(request).await;

            let response = match result {
                Ok(response) => response,
                Err(e) => {
                    // It appears this type of error will not be an HTTP error.
                    // It will be a closed connection, aborted write, timeout, etc.
                    error!("An error occurred while retrieving flag information {e}",);
                    return Err(FeatureRequesterError::Temporary);
                }
            };

            // 304 NOT MODIFIED
            if response.status() == 304 && cache.is_some() {
                if let Some(entry) = cache {
                    return Ok(entry.0);
                }
            }

            let etag: String = response
                .headers()
                .get("etag")
                .unwrap_or(&crate::EMPTY_HEADER)
                .to_str()
                .map_or_else(|_| "".into(), |s| s.into());

            if response.status().is_success() {
                // Collect streaming body
                let mut body_bytes = Vec::new();
                let mut stream = response.into_body();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|e| {
                        error!("An error occurred while reading the polling response body: {e}");
                        FeatureRequesterError::Temporary
                    })?;
                    body_bytes.extend_from_slice(&chunk);
                }
                let json = serde_json::from_slice::<AllData<Flag, Segment>>(&body_bytes);

                return match json {
                    Ok(all_data) => {
                        if !etag.is_empty() {
                            debug!("Caching data for future use with etag: {etag}");
                            self.cache = Some(CachedEntry(all_data.clone(), etag));
                        }
                        Ok(all_data)
                    }
                    Err(e) => {
                        error!("An error occurred while parsing the json response: {e}");
                        Err(FeatureRequesterError::Temporary)
                    }
                };
            }

            error!(
                "An error occurred while retrieving flag information. (status: {})",
                response.status().as_str()
            );

            if !is_http_error_recoverable(response.status().as_u16()) {
                return Err(FeatureRequesterError::Permanent);
            }

            Err(FeatureRequesterError::Temporary)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use test_case::test_case;

    #[tokio::test]
    async fn updates_etag_as_appropriate() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/")
            .with_status(200)
            .with_header("etag", "INITIAL-TAG")
            .with_body(r#"{"flags": {}, "segments": {}}"#)
            .expect(1)
            .create_async()
            .await;
        server
            .mock("GET", "/")
            .with_status(304)
            .match_header("If-None-Match", "INITIAL-TAG")
            .expect(1)
            .create_async()
            .await;
        server
            .mock("GET", "/")
            .with_status(200)
            .match_header("If-None-Match", "INITIAL-TAG")
            .with_header("etag", "UPDATED-TAG")
            .with_body(r#"{"flags": {}, "segments": {}}"#)
            .create_async()
            .await;

        let mut requester = build_feature_requester(server.url());
        let result = requester.get_all().await;

        assert!(result.is_ok());
        if let Some(cache) = &requester.cache {
            assert_eq!("INITIAL-TAG", cache.1);
        }

        let result = requester.get_all().await;
        assert!(result.is_ok());
        if let Some(cache) = &requester.cache {
            assert_eq!("INITIAL-TAG", cache.1);
        }

        let result = requester.get_all().await;
        assert!(result.is_ok());
        if let Some(cache) = &requester.cache {
            assert_eq!("UPDATED-TAG", cache.1);
        }
    }

    #[tokio::test]
    async fn can_process_large_body() {
        let payload = std::fs::read("test-data/large-polling-payload.json")
            .expect("Failed to read polling payload file");
        let payload =
            String::from_utf8(payload).expect("Invalid UTF-8 characters in polling payload");

        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/")
            .with_status(200)
            .with_body(payload)
            .expect(1)
            .create_async()
            .await;

        let mut requester = build_feature_requester(server.url());
        let result = requester.get_all().await;

        assert!(result.is_ok());
    }

    #[test_case(400, FeatureRequesterError::Temporary)]
    #[test_case(401, FeatureRequesterError::Permanent)]
    #[test_case(408, FeatureRequesterError::Temporary)]
    #[test_case(409, FeatureRequesterError::Permanent)]
    #[test_case(429, FeatureRequesterError::Temporary)]
    #[test_case(430, FeatureRequesterError::Permanent)]
    #[test_case(500, FeatureRequesterError::Temporary)]
    #[tokio::test]
    async fn correctly_determines_unrecoverable_errors(
        status: usize,
        error: FeatureRequesterError,
    ) {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/")
            .with_status(status)
            .create_async()
            .await;

        let mut requester = build_feature_requester(server.url());
        let result = requester.get_all().await;

        if let Err(err) = result {
            assert_eq!(err, error);
        } else {
            panic!("get_all returned the wrong response");
        }
    }

    fn build_feature_requester(
        url: String,
    ) -> HttpFeatureRequester<launchdarkly_sdk_transport::HyperTransport> {
        let url = http::Uri::from_str(&url).expect("Failed parsing the mock server url");
        let transport = launchdarkly_sdk_transport::HyperTransport::new()
            .expect("Failed to create HyperTransport");

        HttpFeatureRequester::new(
            transport,
            url,
            "sdk-key".to_string(),
            HashMap::<&str, String>::new(),
        )
    }
}
