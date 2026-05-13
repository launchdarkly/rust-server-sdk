use crate::feature_requester::{FeatureRequester, HttpFeatureRequester};
use crate::{LAUNCHDARKLY_INSTANCE_ID_HEADER, LAUNCHDARKLY_TAGS_HEADER};
use http::Uri;
use launchdarkly_sdk_transport::HttpTransport;
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

pub struct HttpFeatureRequesterBuilder<T: HttpTransport> {
    url: String,
    sdk_key: String,
    instance_id: Option<String>,
    transport: T,
}

impl<T: HttpTransport> HttpFeatureRequesterBuilder<T> {
    pub fn new(url: &str, sdk_key: &str, transport: T) -> Self {
        Self {
            transport,
            url: url.into(),
            sdk_key: sdk_key.into(),
            instance_id: None,
        }
    }

    /// Sets the per-SDK-instance identifier that will be sent on outbound polling requests as
    /// the `X-LaunchDarkly-Instance-Id` header. This is set by the SDK at client construction
    /// time and is not part of the public API surface of the polling data source.
    pub fn with_instance_id(mut self, instance_id: &str) -> Self {
        self.instance_id = Some(instance_id.into());
        self
    }
}

impl<T: HttpTransport> FeatureRequesterFactory for HttpFeatureRequesterBuilder<T> {
    fn build(&self, tags: Option<String>) -> Result<Box<dyn FeatureRequester>, BuildError> {
        let url = format!("{}/sdk/latest-all", self.url);

        let mut default_headers = HashMap::<&str, String>::new();

        if let Some(tags) = tags {
            default_headers.insert(LAUNCHDARKLY_TAGS_HEADER, tags);
        }
        if let Some(instance_id) = &self.instance_id {
            default_headers.insert(LAUNCHDARKLY_INSTANCE_ID_HEADER, instance_id.clone());
        }

        let url = Uri::from_str(url.as_str())
            .map_err(|_| BuildError::InvalidConfig("Invalid base url provided".into()))?;

        Ok(Box::new(HttpFeatureRequester::new(
            self.transport.clone(),
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
        let transport =
            launchdarkly_sdk_transport::HyperTransport::new().expect("Failed to create transport");
        let builder = HttpFeatureRequesterBuilder::new(
            "This is clearly not a valid URL",
            "sdk-key",
            transport,
        );
        let result = builder.build(None);

        match result {
            Err(BuildError::InvalidConfig(_)) => (),
            _ => panic!("Build did not return the right type of error"),
        };
    }
}
