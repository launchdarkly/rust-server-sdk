use crate::feature_requester::{FeatureRequester, HyperFeatureRequester};
use crate::LAUNCHDARKLY_TAGS_HEADER;
use hyper::client::connect::Connection;
use hyper::service::Service;
use hyper::Uri;
use std::collections::HashMap;
use std::str::FromStr;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

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

pub struct HyperFeatureRequesterBuilder<C> {
    url: String,
    sdk_key: String,
    http: hyper::Client<C>,
}

impl<C> HyperFeatureRequesterBuilder<C>
where
    C: Service<Uri> + Clone + Send + Sync + 'static,
    C::Response: Connection + AsyncRead + AsyncWrite + Send + Unpin,
    C::Future: Send + Unpin + 'static,
    C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    pub fn new(url: &str, sdk_key: &str, connector: C) -> Self {
        Self {
            http: hyper::Client::builder().build(connector),
            url: url.into(),
            sdk_key: sdk_key.into(),
        }
    }
}

impl<C> FeatureRequesterFactory for HyperFeatureRequesterBuilder<C>
where
    C: Service<Uri> + Clone + Send + Sync + 'static,
    C::Response: Connection + AsyncRead + AsyncWrite + Send + Unpin,
    C::Future: Send + Unpin + 'static,
    C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn build(&self, tags: Option<String>) -> Result<Box<dyn FeatureRequester>, BuildError> {
        let url = format!("{}/sdk/latest-all", self.url);

        let mut default_headers = HashMap::<&str, String>::new();

        if let Some(tags) = tags {
            default_headers.insert(LAUNCHDARKLY_TAGS_HEADER, tags);
        }

        let url = hyper::Uri::from_str(url.as_str())
            .map_err(|_| BuildError::InvalidConfig("Invalid base url provided".into()))?;

        Ok(Box::new(HyperFeatureRequester::new(
            self.http.clone(),
            url,
            self.sdk_key.clone(),
            default_headers,
        )))
    }
}

#[cfg(test)]
mod tests {
    use hyper::client::HttpConnector;

    use super::*;

    #[test]
    fn factory_handles_url_parsing_failure() {
        let builder = HyperFeatureRequesterBuilder::new(
            "This is clearly not a valid URL",
            "sdk-key",
            HttpConnector::new(),
        );
        let result = builder.build(None);

        match result {
            Err(BuildError::InvalidConfig(_)) => (),
            _ => panic!("Build did not return the right type of error"),
        };
    }
}
