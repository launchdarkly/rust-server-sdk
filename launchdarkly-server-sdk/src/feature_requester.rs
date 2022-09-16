use crate::reqwest::is_http_error_recoverable;

use super::stores::store_types::AllData;
use launchdarkly_server_sdk_evaluation::{Flag, Segment};
use r::{
    header::{HeaderValue, ETAG},
    StatusCode,
};
use reqwest as r;

#[derive(Debug, PartialEq)]
pub enum FeatureRequesterError {
    Temporary,
    Permanent,
}

#[derive(Clone, Debug)]
struct CachedEntry(AllData<Flag, Segment>, String);

pub trait FeatureRequester: Send {
    fn get_all(&mut self) -> Result<AllData<Flag, Segment>, FeatureRequesterError>;
}

pub struct ReqwestFeatureRequester {
    http: r::Client,
    url: r::Url,
    sdk_key: String,
    cache: Option<CachedEntry>,
}

impl ReqwestFeatureRequester {
    pub fn new(http: r::Client, url: r::Url, sdk_key: String) -> Self {
        Self {
            http,
            url,
            sdk_key,
            cache: None,
        }
    }
}

impl FeatureRequester for ReqwestFeatureRequester {
    fn get_all(&mut self) -> Result<AllData<Flag, Segment>, FeatureRequesterError> {
        let mut request_builder = self
            .http
            .get(self.url.clone())
            .header("Content-Type", "application/json")
            .header("Authorization", self.sdk_key.clone())
            .header("User-Agent", &*crate::USER_AGENT);

        if let Some(cache) = &self.cache {
            request_builder = request_builder.header("If-None-Match", cache.1.clone());
        }

        let resp = request_builder.send();

        let mut response = match resp {
            Ok(response) => response,
            Err(e) => {
                error!(
                    "An error occurred while retrieving flag information {} (status: {})",
                    e,
                    e.status()
                        .map_or(String::from("none"), |s| s.as_str().to_string())
                );
                return Err(match e.status() {
                    // If there is no status code, then the error is recoverable.
                    // If there is a status code, and the code is for a non-recoverable error,
                    // then the failure is permanent.
                    Some(status_code) if !is_http_error_recoverable(status_code) => {
                        FeatureRequesterError::Permanent
                    }
                    _ => FeatureRequesterError::Temporary,
                });
            }
        };

        if response.status() == StatusCode::NOT_MODIFIED && self.cache.is_some() {
            let cache = self.cache.clone().unwrap();
            debug!("Returning cached data. Etag: {}", cache.1);
            return Ok(cache.0);
        }

        let etag: String = response
            .headers()
            .get(ETAG)
            .unwrap_or(&HeaderValue::from_static(""))
            .to_str()
            .map_or_else(|_| "".into(), |s| s.into());

        if response.status().is_success() {
            return match response.json::<AllData<Flag, Segment>>() {
                Ok(all_data) => {
                    if !etag.is_empty() {
                        debug!("Caching data for future use with etag: {}", etag);
                        self.cache = Some(CachedEntry(all_data.clone(), etag));
                    }
                    Ok(all_data)
                }
                Err(e) => {
                    error!("An error occurred while parsing the json response: {}", e);
                    Err(FeatureRequesterError::Permanent)
                }
            };
        }

        error!(
            "An error occurred while retrieving flag information. (status: {})",
            response.status().as_str()
        );

        if !is_http_error_recoverable(response.status()) {
            return Err(FeatureRequesterError::Permanent);
        }

        Err(FeatureRequesterError::Temporary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::mock;
    use test_case::test_case;

    #[test]
    fn updates_etag_as_appropriate() {
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
        let result = requester.get_all();

        assert!(result.is_ok());
        if let Some(cache) = &requester.cache {
            assert_eq!("INITIAL-TAG", cache.1);
        }

        let result = requester.get_all();
        assert!(result.is_ok());
        if let Some(cache) = &requester.cache {
            assert_eq!("INITIAL-TAG", cache.1);
        }

        let result = requester.get_all();
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
    fn correctly_determines_unrecoverable_errors(status: usize, error: FeatureRequesterError) {
        let _initial_request = mock("GET", "/").with_status(status).create();

        let mut requester = build_feature_requester();
        let result = requester.get_all();

        if let Err(err) = result {
            assert_eq!(err, error);
        } else {
            panic!("get_all returned the wrong response");
        }
    }

    fn build_feature_requester() -> ReqwestFeatureRequester {
        let http = r::Client::builder()
            .build()
            .expect("Failed building the client");
        let url = reqwest::Url::parse(&mockito::server_url())
            .expect("Failed parsing the mock server url");

        ReqwestFeatureRequester::new(http, url, "sdk-key".to_string())
    }
}
