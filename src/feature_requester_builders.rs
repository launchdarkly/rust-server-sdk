use crate::feature_requester::FeatureRequester;
use crate::feature_requester::ReqwestFeatureRequester;
use reqwest as r;
use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("feature requester factory failed to build: {0}")]
    InvalidConfig(String),
}

/// Trait which allows creation of feature requesters.
///
/// Feature requesters are used by the polling data source (see [crate::PollingDataSourceBuilder])
/// to retrieve state information from an external resource such as the LaunchDarkly API.
pub trait FeatureRequesterFactory: Send {
    fn build(&self) -> Result<Box<dyn FeatureRequester>, BuildError>;
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
    fn build(&self) -> Result<Box<dyn FeatureRequester>, BuildError> {
        let mut url = reqwest::Url::parse(&self.url)
            .map_err(|_| BuildError::InvalidConfig("Invalid base url provided".into()))?;
        url.set_path("/sdk/latest-all");

        let http = r::Client::builder()
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
        let result = builder.build();

        match result {
            Err(BuildError::InvalidConfig(_)) => (),
            _ => panic!("Build did not return the right type of error"),
        };
    }
}
