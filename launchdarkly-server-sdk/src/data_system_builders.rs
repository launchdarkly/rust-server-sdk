use std::sync::Arc;
use std::time::Duration;

use launchdarkly_sdk_transport::{HttpTransport, HyperTransport};
use thiserror::Error;

use crate::data_source_builders::{DataSourceFactory, StreamingDataSourceBuilder};
use crate::data_system::DataSystem;
use crate::fdv2::data_system::{FDv2DataSystem, InitializerFactory, SynchronizerFactory};
use crate::fdv2::fdv1_adapter::FDv1AdapterFactory;
use crate::fdv2::polling::{PollingInitializerFactory, PollingSynchronizerFactory};
use crate::fdv2::request_headers::RequestHeaders;
use crate::fdv2::streaming::StreamingSynchronizerFactory;
use crate::service_endpoints::ServiceEndpoints;

const DEFAULT_INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_FALLBACK_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);

/// Error returned when a data system configuration cannot be built.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    /// The data system configuration was invalid.
    #[error("data system config failed to build: {0}")]
    InvalidConfig(String),
}

/// A configured FDv2 source usable as a synchronizer.
pub(crate) trait FDv2SynchronizerConfig {
    fn build_synchronizer(
        &self,
        endpoints: &ServiceEndpoints,
        headers: &RequestHeaders,
    ) -> Result<Box<dyn SynchronizerFactory>, BuildError>;

    fn to_owned(&self) -> Box<dyn FDv2SynchronizerConfig>;
}

/// A configured FDv2 source usable as an initializer.
pub(crate) trait FDv2InitializerConfig {
    fn build_initializer(
        &self,
        endpoints: &ServiceEndpoints,
        headers: &RequestHeaders,
    ) -> Result<Box<dyn InitializerFactory>, BuildError>;

    fn to_owned(&self) -> Box<dyn FDv2InitializerConfig>;
}

/// Builds the default HTTPS transport, or errors if no TLS feature is enabled.
fn default_https_transport() -> Result<impl HttpTransport + 'static, BuildError> {
    #[cfg(any(
        feature = "hyper-rustls-native-roots",
        feature = "hyper-rustls-webpki-roots",
        feature = "native-tls"
    ))]
    {
        HyperTransport::new_https().map_err(|e| {
            BuildError::InvalidConfig(format!("failed to create default https transport: {e:?}"))
        })
    }
    #[cfg(not(any(
        feature = "hyper-rustls-native-roots",
        feature = "hyper-rustls-webpki-roots",
        feature = "native-tls"
    )))]
    {
        Err::<HyperTransport, _>(BuildError::InvalidConfig(
            "https connector required when hyper-rustls-native-roots, hyper-rustls-webpki-roots, or native-tls features are disabled".into(),
        ))
    }
}

/// Configures an FDv2 streaming source, which can only act as a synchronizer.
#[derive(Clone)]
pub struct FDv2StreamingBuilder<T: HttpTransport = HyperTransport> {
    initial_reconnect_delay: Duration,
    base_url: Option<String>,
    transport: Option<T>,
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> FDv2StreamingBuilder<T> {
    /// Creates a builder with default values.
    pub fn new() -> Self {
        Self {
            initial_reconnect_delay: DEFAULT_INITIAL_RECONNECT_DELAY,
            base_url: None,
            transport: None,
        }
    }

    /// Sets the initial reconnect delay for the streaming connection.
    pub fn initial_reconnect_delay(&mut self, duration: Duration) -> &mut Self {
        self.initial_reconnect_delay = duration;
        self
    }

    /// Sets the streaming base URL, overriding the configured service endpoints.
    pub fn base_url(&mut self, url: &str) -> &mut Self {
        self.base_url = Some(url.to_string());
        self
    }

    /// Sets the transport to use, instead of the default HTTPS transport.
    pub fn transport(&mut self, transport: T) -> &mut Self {
        self.transport = Some(transport);
        self
    }
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> FDv2SynchronizerConfig
    for FDv2StreamingBuilder<T>
{
    fn build_synchronizer(
        &self,
        endpoints: &ServiceEndpoints,
        headers: &RequestHeaders,
    ) -> Result<Box<dyn SynchronizerFactory>, BuildError> {
        let base_url = self
            .base_url
            .clone()
            .unwrap_or_else(|| endpoints.streaming_base_url().to_string());
        let factory: Box<dyn SynchronizerFactory> = match &self.transport {
            Some(transport) => Box::new(StreamingSynchronizerFactory::new(
                transport.clone(),
                base_url,
                headers.clone(),
                self.initial_reconnect_delay,
            )),
            None => Box::new(StreamingSynchronizerFactory::new(
                default_https_transport()?,
                base_url,
                headers.clone(),
                self.initial_reconnect_delay,
            )),
        };
        Ok(factory)
    }

    fn to_owned(&self) -> Box<dyn FDv2SynchronizerConfig> {
        Box::new(self.clone())
    }
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> Default for FDv2StreamingBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Configures an FDv2 polling source, which can act as an initializer or a synchronizer.
#[derive(Clone)]
pub struct FDv2PollingBuilder<T: HttpTransport = HyperTransport> {
    poll_interval: Duration,
    base_url: Option<String>,
    transport: Option<T>,
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> FDv2PollingBuilder<T> {
    /// Creates a builder with default values.
    pub fn new() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            base_url: None,
            transport: None,
        }
    }

    /// Sets the interval between polling requests, with an effective minimum of 30 seconds.
    pub fn poll_interval(&mut self, poll_interval: Duration) -> &mut Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Sets the polling base URL, overriding the configured service endpoints.
    pub fn base_url(&mut self, url: &str) -> &mut Self {
        self.base_url = Some(url.to_string());
        self
    }

    /// Sets the transport to use, instead of the default HTTPS transport.
    pub fn transport(&mut self, transport: T) -> &mut Self {
        self.transport = Some(transport);
        self
    }
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> FDv2SynchronizerConfig
    for FDv2PollingBuilder<T>
{
    fn build_synchronizer(
        &self,
        endpoints: &ServiceEndpoints,
        headers: &RequestHeaders,
    ) -> Result<Box<dyn SynchronizerFactory>, BuildError> {
        let base_url = self
            .base_url
            .clone()
            .unwrap_or_else(|| endpoints.polling_base_url().to_string());
        let factory: Box<dyn SynchronizerFactory> = match &self.transport {
            Some(transport) => Box::new(PollingSynchronizerFactory::new(
                transport.clone(),
                base_url,
                headers.clone(),
                self.poll_interval,
            )),
            None => Box::new(PollingSynchronizerFactory::new(
                default_https_transport()?,
                base_url,
                headers.clone(),
                self.poll_interval,
            )),
        };
        Ok(factory)
    }

    fn to_owned(&self) -> Box<dyn FDv2SynchronizerConfig> {
        Box::new(self.clone())
    }
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> FDv2InitializerConfig
    for FDv2PollingBuilder<T>
{
    fn build_initializer(
        &self,
        endpoints: &ServiceEndpoints,
        headers: &RequestHeaders,
    ) -> Result<Box<dyn InitializerFactory>, BuildError> {
        let base_url = self
            .base_url
            .clone()
            .unwrap_or_else(|| endpoints.polling_base_url().to_string());
        let factory: Box<dyn InitializerFactory> = match &self.transport {
            Some(transport) => Box::new(PollingInitializerFactory::new(
                transport.clone(),
                base_url,
                headers.clone(),
            )),
            None => Box::new(PollingInitializerFactory::new(
                default_https_transport()?,
                base_url,
                headers.clone(),
            )),
        };
        Ok(factory)
    }

    fn to_owned(&self) -> Box<dyn FDv2InitializerConfig> {
        Box::new(self.clone())
    }
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> Default for FDv2PollingBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Configures the FDv2 data system.
pub struct DataSystemBuilder {
    initializers: Vec<Box<dyn FDv2InitializerConfig>>,
    synchronizers: Vec<Box<dyn FDv2SynchronizerConfig>>,
    fdv1_fallback: Option<Box<dyn DataSourceFactory>>,
}

impl Clone for DataSystemBuilder {
    fn clone(&self) -> Self {
        Self {
            initializers: self.initializers.iter().map(|c| (**c).to_owned()).collect(),
            synchronizers: self
                .synchronizers
                .iter()
                .map(|c| (**c).to_owned())
                .collect(),
            fdv1_fallback: self.fdv1_fallback.as_ref().map(|f| (**f).to_owned()),
        }
    }
}

impl DataSystemBuilder {
    /// Creates an empty builder; the caller adds sources explicitly.
    pub fn custom() -> Self {
        Self {
            initializers: Vec::new(),
            synchronizers: Vec::new(),
            fdv1_fallback: None,
        }
    }

    /// Appends a polling initializer.
    pub fn initializer<T: HttpTransport + Clone + Send + Sync + 'static>(
        &mut self,
        source: FDv2PollingBuilder<T>,
    ) -> &mut Self {
        self.initializers.push(Box::new(source));
        self
    }

    /// Appends a streaming synchronizer, ordered after any already added.
    pub fn streaming_synchronizer<T: HttpTransport + Clone + Send + Sync + 'static>(
        &mut self,
        source: FDv2StreamingBuilder<T>,
    ) -> &mut Self {
        self.synchronizers.push(Box::new(source));
        self
    }

    /// Appends a polling synchronizer, ordered after any already added.
    pub fn polling_synchronizer<T: HttpTransport + Clone + Send + Sync + 'static>(
        &mut self,
        source: FDv2PollingBuilder<T>,
    ) -> &mut Self {
        self.synchronizers.push(Box::new(source));
        self
    }

    /// Sets the FDv1 source used as a last-resort fallback.
    pub fn fdv1_fallback(&mut self, factory: &dyn DataSourceFactory) -> &mut Self {
        self.fdv1_fallback = Some(factory.to_owned());
        self
    }

    /// Disables the FDv1 fallback.
    pub fn disable_fdv1_fallback(&mut self) -> &mut Self {
        self.fdv1_fallback = None;
        self
    }
}

impl Default for DataSystemBuilder {
    /// The recommended data system setup.
    fn default() -> Self {
        let mut builder = Self::custom();
        builder.initializer(FDv2PollingBuilder::<HyperTransport>::new());
        builder.streaming_synchronizer(FDv2StreamingBuilder::<HyperTransport>::new());
        builder.polling_synchronizer(FDv2PollingBuilder::<HyperTransport>::new());
        builder.fdv1_fallback(&StreamingDataSourceBuilder::<HyperTransport>::new());
        builder
    }
}

/// Builds the internal FDv2 data system from a configured source set.
pub(crate) trait DataSystemFactory {
    fn build(
        &self,
        endpoints: &ServiceEndpoints,
        sdk_key: &str,
        tags: Option<&str>,
        instance_id: &str,
    ) -> Result<Arc<dyn DataSystem>, BuildError>;
}

impl DataSystemFactory for DataSystemBuilder {
    fn build(
        &self,
        endpoints: &ServiceEndpoints,
        sdk_key: &str,
        tags: Option<&str>,
        instance_id: &str,
    ) -> Result<Arc<dyn DataSystem>, BuildError> {
        let headers = RequestHeaders::new(sdk_key, tags, instance_id);

        let initializer_factories: Vec<Arc<dyn InitializerFactory>> = self
            .initializers
            .iter()
            .map(|c| c.build_initializer(endpoints, &headers).map(Arc::from))
            .collect::<Result<_, _>>()?;

        let mut synchronizer_factories: Vec<Arc<dyn SynchronizerFactory>> = self
            .synchronizers
            .iter()
            .map(|c| c.build_synchronizer(endpoints, &headers).map(Arc::from))
            .collect::<Result<_, _>>()?;

        // Build the FDv1 fallback source once and wrap it as a synchronizer; the
        // adapter re-subscribes it whenever the fallback activates.
        if let Some(fdv1_factory) = &self.fdv1_fallback {
            let mut fdv1_factory = (**fdv1_factory).to_owned();
            fdv1_factory.set_instance_id(instance_id.to_string());
            let source = fdv1_factory
                .build(endpoints, sdk_key, tags.map(|t| t.to_string()))
                .map_err(|e| {
                    BuildError::InvalidConfig(format!("failed to build FDv1 fallback source: {e}"))
                })?;
            let adapter = FDv1AdapterFactory::new(Box::new(move || source.clone()));
            synchronizer_factories.push(Arc::new(adapter));
        }

        let system: Arc<dyn DataSystem> = Arc::new(FDv2DataSystem::new(
            initializer_factories,
            synchronizer_factories,
            DEFAULT_FALLBACK_TIMEOUT,
            DEFAULT_RECOVERY_TIMEOUT,
        ));
        Ok(system)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use launchdarkly_sdk_transport::{Request, ResponseFuture};

    use super::*;

    #[test]
    fn custom_starts_empty() {
        let builder = DataSystemBuilder::custom();

        assert!(builder.initializers.is_empty());
        assert!(builder.synchronizers.is_empty());
        assert!(builder.fdv1_fallback.is_none());
    }

    #[test]
    fn default_has_recommended_sources() {
        let builder = DataSystemBuilder::default();

        assert_eq!(builder.initializers.len(), 1);
        assert_eq!(builder.synchronizers.len(), 2);
        assert!(builder.fdv1_fallback.is_some());
    }

    #[test]
    fn disable_fdv1_fallback_clears_it() {
        let mut builder = DataSystemBuilder::default();
        assert!(builder.fdv1_fallback.is_some());

        builder.disable_fdv1_fallback();

        assert!(builder.fdv1_fallback.is_none());
    }

    #[derive(Debug, Clone)]
    struct TestTransport;

    impl HttpTransport for TestTransport {
        fn request(&self, _request: Request<Option<Bytes>>) -> ResponseFuture {
            unreachable!();
        }
    }

    #[test]
    fn builders_build_factories_with_injected_transport() {
        let endpoints = crate::ServiceEndpointsBuilder::new().build().unwrap();
        let headers = RequestHeaders::new("sdk-key", None, "test-instance");

        // Each source builds a factory from a configured transport.
        assert!(FDv2StreamingBuilder::<TestTransport>::new()
            .transport(TestTransport)
            .build_synchronizer(&endpoints, &headers)
            .is_ok());
        assert!(FDv2PollingBuilder::<TestTransport>::new()
            .transport(TestTransport)
            .build_synchronizer(&endpoints, &headers)
            .is_ok());
        assert!(FDv2PollingBuilder::<TestTransport>::new()
            .transport(TestTransport)
            .build_initializer(&endpoints, &headers)
            .is_ok());
    }

    // The default path builds a real HTTPS transport, which needs a TLS backend,
    // so this only runs where one of those features is enabled.
    #[test]
    #[cfg(any(
        feature = "hyper-rustls-native-roots",
        feature = "hyper-rustls-webpki-roots",
        feature = "native-tls"
    ))]
    fn builders_build_factories_with_default_transport() {
        let endpoints = crate::ServiceEndpointsBuilder::new().build().unwrap();
        let headers = RequestHeaders::new("sdk-key", None, "test-instance");

        // Each source builds a factory off the default HTTPS transport.
        assert!(FDv2StreamingBuilder::<HyperTransport>::new()
            .build_synchronizer(&endpoints, &headers)
            .is_ok());
        assert!(FDv2PollingBuilder::<HyperTransport>::new()
            .build_synchronizer(&endpoints, &headers)
            .is_ok());
        assert!(FDv2PollingBuilder::<HyperTransport>::new()
            .build_initializer(&endpoints, &headers)
            .is_ok());
    }
}
