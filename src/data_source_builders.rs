use super::service_endpoints;
use crate::data_source::{DataSource, NullDataSource, StreamingDataSource};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

#[cfg(test)]
use super::data_source;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("data source factory failed to build: {0}")]
    InvalidConfig(String),
}

const DEFAULT_INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// Trait which allows creation of data sources. Should be implemented by data source builder types.
pub trait DataSourceFactory {
    fn build(
        &self,
        endpoints: &service_endpoints::ServiceEndpoints,
        sdk_key: &str,
    ) -> Result<Arc<Mutex<dyn DataSource>>, BuildError>;
    fn to_owned(&self) -> Box<dyn DataSourceFactory>;
}

/// Contains methods for configuring the streaming data source.
///
/// By default, the SDK uses a streaming connection to receive feature flag data from LaunchDarkly. If you want
/// to customize the behavior of the connection, create a builder [StreamingDataSourceBuilder::new],
/// change its properties with the methods of this class, and pass it to
/// [crate::ConfigBuilder::data_source].
///
/// # Examples
///
/// Adjust the initial reconnect delay.
/// ```
/// # use launchdarkly_server_sdk::{StreamingDataSourceBuilder, ConfigBuilder};
/// # use std::time::Duration;
/// # fn main() {
///     ConfigBuilder::new("sdk-key").data_source(StreamingDataSourceBuilder::new()
///         .initial_reconnect_delay(Duration::from_secs(10)));
/// # }
/// ```
#[derive(Clone)]
pub struct StreamingDataSourceBuilder {
    initial_reconnect_delay: Duration,
}

impl StreamingDataSourceBuilder {
    pub fn new() -> Self {
        Self {
            initial_reconnect_delay: DEFAULT_INITIAL_RECONNECT_DELAY,
        }
    }

    /// Sets the initial reconnect delay for the streaming connection.
    pub fn initial_reconnect_delay(&mut self, duration: Duration) -> &mut Self {
        self.initial_reconnect_delay = duration;
        self
    }
}

impl DataSourceFactory for StreamingDataSourceBuilder {
    fn build(
        &self,
        endpoints: &service_endpoints::ServiceEndpoints,
        sdk_key: &str,
    ) -> Result<Arc<Mutex<dyn DataSource>>, BuildError> {
        let data_source = StreamingDataSource::new(
            endpoints.streaming_base_url(),
            sdk_key,
            self.initial_reconnect_delay,
        )
        .map_err(|e| BuildError::InvalidConfig(format!("invalid stream_base_url: {:?}", e)))?;
        Ok(Arc::new(Mutex::new(data_source)))
    }

    fn to_owned(&self) -> Box<dyn DataSourceFactory> {
        Box::new(self.clone())
    }
}

impl Default for StreamingDataSourceBuilder {
    fn default() -> Self {
        StreamingDataSourceBuilder::new()
    }
}

#[derive(Clone)]
pub struct NullDataSourceBuilder {}

impl NullDataSourceBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl DataSourceFactory for NullDataSourceBuilder {
    fn build(
        &self,
        _: &service_endpoints::ServiceEndpoints,
        _: &str,
    ) -> Result<Arc<Mutex<dyn DataSource>>, BuildError> {
        Ok(Arc::new(Mutex::new(NullDataSource::new())))
    }

    fn to_owned(&self) -> Box<dyn DataSourceFactory> {
        Box::new(self.clone())
    }
}

impl Default for NullDataSourceBuilder {
    fn default() -> Self {
        NullDataSourceBuilder::new()
    }
}

/// For testing you can use this builder to inject the MockDataSource.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct MockDataSourceBuilder {
    data_source: Option<Arc<Mutex<data_source::MockDataSource>>>,
}

#[cfg(test)]
impl MockDataSourceBuilder {
    pub fn new() -> MockDataSourceBuilder {
        MockDataSourceBuilder { data_source: None }
    }

    pub fn data_source(
        &mut self,
        data_source: Arc<Mutex<data_source::MockDataSource>>,
    ) -> &mut MockDataSourceBuilder {
        self.data_source = Some(data_source);
        self
    }
}

#[cfg(test)]
impl DataSourceFactory for MockDataSourceBuilder {
    // TODO: Implement re-connect with jitter.
    fn build(
        &self,
        _endpoints: &service_endpoints::ServiceEndpoints,
        _sdk_key: &str,
    ) -> Result<Arc<Mutex<dyn DataSource>>, BuildError> {
        return Ok(self.data_source.as_ref().unwrap().clone());
    }

    fn to_owned(&self) -> Box<dyn DataSourceFactory> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_has_correct_defaults() {
        let builder = StreamingDataSourceBuilder::new();
        assert_eq!(
            builder.initial_reconnect_delay,
            DEFAULT_INITIAL_RECONNECT_DELAY
        );
    }

    #[test]
    fn initial_reconnect_delay_can_be_adjusted() {
        let mut builder = StreamingDataSourceBuilder::new();
        builder.initial_reconnect_delay(Duration::from_secs(1234));
        assert_eq!(builder.initial_reconnect_delay, Duration::from_secs(1234));
    }
}
