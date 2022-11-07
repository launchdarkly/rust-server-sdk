use crate::feature_requester::FeatureRequester;
use crate::feature_requester::ReqwestFeatureRequester;
use crate::LAUNCHDARKLY_TAGS_HEADER;
use reqwest as r;
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

pub struct ReqwestFeatureRequesterBuilder {
    url: String,
    sdk_key: String,
}

impl ReqwestFeatureRequesterBuilder {
    pub fn new(url: &str, sdk_key: &str) -> Self {
        Self {
            url: url.into(),
            sdk_key: sdk_key.into(),
        }
    }
}

impl FeatureRequesterFactory for ReqwestFeatureRequesterBuilder {
    fn build(&self, tags: Option<String>) -> Result<Box<dyn FeatureRequester>, BuildError> {
        let url = format!("{}/sdk/latest-all", self.url);
        let url = r::Url::parse(&url)
            .map_err(|_| BuildError::InvalidConfig("Invalid base url provided".into()))?;

        let mut builder = r::Client::builder();

        if let Some(tags) = tags {
            let mut headers = r::header::HeaderMap::new();
            headers.append(
                LAUNCHDARKLY_TAGS_HEADER,
                r::header::HeaderValue::from_str(&tags)
                    .map_err(|e| BuildError::InvalidConfig(e.to_string()))?,
            );
            builder = builder.default_headers(headers);
        }

        let http = builder
            .build()
            .map_err(|e| BuildError::InvalidConfig(e.to_string()))?;

        Ok(Box::new(ReqwestFeatureRequester::new(
            http,
            url,
            self.sdk_key.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_handles_url_parsing_failure() {
        let builder =
            ReqwestFeatureRequesterBuilder::new("This is clearly not a valid URL", "sdk-key");
        let result = builder.build(None);

        match result {
            Err(BuildError::InvalidConfig(_)) => (),
            _ => panic!("Build did not return the right type of error"),
        };
    }
}
