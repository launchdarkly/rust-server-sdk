use crate::reqwest::is_http_error_recoverable;
use futures::future::BoxFuture;
use hyper::body::HttpBody;
use hyper::Body;
use std::collections::HashMap;
use std::sync::Arc;

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
    fn get_all(&mut self) -> BoxFuture<Result<AllData<Flag, Segment>, FeatureRequesterError>>;
}

pub struct HyperFeatureRequester<C> {
    http: Arc<hyper::Client<C>>,
    url: hyper::Uri,
    sdk_key: String,
    cache: Option<CachedEntry>,
    default_headers: HashMap<&'static str, String>,
}

impl<C> HyperFeatureRequester<C> {
    pub fn new(
        http: hyper::Client<C>,
        url: hyper::Uri,
        sdk_key: String,
        default_headers: HashMap<&'static str, String>,
    ) -> Self {
        Self {
            http: Arc::new(http),
            url,
            sdk_key,
            cache: None,
            default_headers,
        }
    }
}

impl<C> FeatureRequester for HyperFeatureRequester<C>
where
    C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
{
    fn get_all(&mut self) -> BoxFuture<Result<AllData<Flag, Segment>, FeatureRequesterError>> {
        Box::pin(async {
            let uri = self.url.clone();
            let key = self.sdk_key.clone();

            let http = self.http.clone();
            let cache = self.cache.clone();

            let mut request_builder = hyper::http::Request::builder()
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

            let result = http
                .request(request_builder.body(Body::empty()).unwrap())
                .await;

            let mut response = match result {
                Ok(response) => response,
                Err(e) => {
                    // It appears this type of error will not be an HTTP error.
                    // It will be a closed connection, aborted write, timeout, etc.
                    error!("An error occurred while retrieving flag information {}", e,);
                    return Err(FeatureRequesterError::Temporary);
                }
            };

            if response.status() == hyper::StatusCode::NOT_MODIFIED && cache.is_some() {
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
                let bytes = response.body_mut().data().await.unwrap().unwrap();
                let json = serde_json::from_slice::<AllData<Flag, Segment>>(bytes.as_ref());

                return match json {
                    Ok(all_data) => {
                        if !etag.is_empty() {
                            debug!("Caching data for future use with etag: {}", etag);
                            self.cache = Some(CachedEntry(all_data.clone(), etag));
                        }
                        Ok(all_data)
                    }
                    Err(e) => {
                        error!("An error occurred while parsing the json response: {}", e);
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
    use mockito::mock;
    use std::str::FromStr;
    use test_case::test_case;

    #[tokio::test]
    async fn updates_etag_as_appropriate() {
        let _initial_request = mock("GET", "/")
            .with_status(200)
            .with_header("etag", "INITIAL-TAG")
            .with_body(r#"{"flags": {}, "segments": {}}"#)
            .expect(1)
            .create();
        let _second_request = mock("GET", "/")
            .with_status(304)
            .match_header("If-None-Match", "INITIAL-TAG")
            .expect(1)
            .create();
        let _third_request = mock("GET", "/")
            .with_status(200)
            .match_header("If-None-Match", "INITIAL-TAG")
            .with_header("etag", "UPDATED-TAG")
            .with_body(r#"{"flags": {}, "segments": {}}"#)
            .create();

        let mut requester = build_feature_requester();
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
        let _initial_request = mock("GET", "/").with_status(status).create();

        let mut requester = build_feature_requester();
        let result = requester.get_all().await;

        if let Err(err) = result {
            assert_eq!(err, error);
        } else {
            panic!("get_all returned the wrong response");
        }
    }

    fn build_feature_requester() -> HyperFeatureRequester<hyper::client::HttpConnector> {
        let http = hyper::Client::builder().build(hyper::client::HttpConnector::new());
        let url = hyper::Uri::from_str(&mockito::server_url())
            .expect("Failed parsing the mock server url");

        HyperFeatureRequester::new(
            http,
            url,
            "sdk-key".to_string(),
            HashMap::<&str, String>::new(),
        )
    }
}
