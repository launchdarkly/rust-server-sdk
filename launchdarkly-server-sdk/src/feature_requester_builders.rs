use crate::feature_requester::{FeatureRequester, HyperFeatureRequester};
use crate::LAUNCHDARKLY_TAGS_HEADER;
use hyper_rustls::HttpsConnectorBuilder;
use std::collections::HashMap;
use std::str::FromStr;
use thiserror::Error;

/// Error type used to represent failures when building a [FeatureRequesterFactory] instance.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    /// Error used when a configuration setting is invalid.
    #[error("feature requester factory failed to build: {0}")]
    InvalidConfig(String),
}

/// Trait which allows creation of feature requesters.
///
/// Feature requesters are used by the polling data source (see [crate::PollingDataSourceBuilder])
/// to retrieve state information from an external resource such as the LaunchDarkly API.
pub trait FeatureRequesterFactory: Send {
    /// Create an instance of FeatureRequester.
    fn build(&self, tags: Option<String>) -> Result<Box<dyn FeatureRequester>, BuildError>;
}

pub struct HyperFeatureRequesterBuilder {
    url: String,
    sdk_key: String,
}

impl HyperFeatureRequesterBuilder {
    pub fn new(url: &str, sdk_key: &str) -> Self {
        Self {
            url: url.into(),
            sdk_key: sdk_key.into(),
        }
    }
}

impl FeatureRequesterFactory for HyperFeatureRequesterBuilder {
    fn build(&self, tags: Option<String>) -> Result<Box<dyn FeatureRequester>, BuildError> {
        let url = format!("{}/sdk/latest-all", self.url);

        let builder = hyper::Client::builder();

        let connector = HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build();

        let http = builder.build(connector);

        let mut default_headers = HashMap::<&str, String>::new();

        if let Some(tags) = tags {
            default_headers.insert(LAUNCHDARKLY_TAGS_HEADER, tags);
        }

        let url = hyper::Uri::from_str(url.as_str())
            .map_err(|_| BuildError::InvalidConfig("Invalid base url provided".into()))?;

        Ok(Box::new(HyperFeatureRequester::new(
            http,
            url,
            self.sdk_key.clone(),
            default_headers,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_handles_url_parsing_failure() {
        let builder =
            HyperFeatureRequesterBuilder::new("This is clearly not a valid URL", "sdk-key");
        let result = builder.build(None);

        match result {
            Err(BuildError::InvalidConfig(_)) => (),
            _ => panic!("Build did not return the right type of error"),
        };
    }
}
