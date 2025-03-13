use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use hyper::client::connect::Connection;
use hyper::service::Service;
use hyper::Uri;
#[cfg(feature = "rustls")]
use hyper_rustls::HttpsConnectorBuilder;
#[cfg(feature = "native-certs")]
use hyper::client::HttpConnector;
#[cfg(feature = "native-certs")]
use hyper_rustls::HttpsConnector;
#[cfg(feature = "native-certs")]
use rustls::ClientConfig;
use launchdarkly_server_sdk_evaluation::Reference;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::events::sender::HyperEventSender;
use crate::{service_endpoints, LAUNCHDARKLY_TAGS_HEADER};

use super::processor::{
    EventProcessor, EventProcessorError, EventProcessorImpl, NullEventProcessor,
};
use super::sender::EventSender;
use super::EventsConfiguration;

const DEFAULT_FLUSH_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_EVENT_CAPACITY: usize = 500;
// The capacity will be set to max(DEFAULT_CONTEXT_KEY_CAPACITY, 1), meaning
// caching cannot be entirely disabled.
const DEFAULT_CONTEXT_KEY_CAPACITY: Option<NonZeroUsize> = NonZeroUsize::new(1000);
const DEFAULT_CONTEXT_KEYS_FLUSH_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Error type used to represent failures when building an [EventProcessor] instance.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BuildError {
    /// Error used when a configuration setting is invalid.
    #[error("event processor factory failed to build: {0}")]
    InvalidConfig(String),

    /// Error used when the event processor's thread fails to start
    #[error(transparent)]
    FailedToStart(EventProcessorError),
}

/// Trait which allows creation of event processors. Should be implemented by event processor
/// builder types.
pub trait EventProcessorFactory {
    fn build(
        &self,
        endpoints: &service_endpoints::ServiceEndpoints,
        sdk_key: &str,
        tags: Option<String>,
    ) -> Result<Arc<dyn EventProcessor>, BuildError>;
    fn to_owned(&self) -> Box<dyn EventProcessorFactory>;
}

/// Contains methods for configuring delivery of analytics events.
///
/// The SDK normally buffers analytics events and sends them to LaunchDarkly at intervals. If you want
/// to customize this behavior, create a builder with [crate::EventProcessorBuilder::new], change its
/// properties with the methods of this struct, and pass it to [crate::ConfigBuilder::event_processor].
///
/// # Examples
///
/// Adjust the flush interval
/// ```
/// # use launchdarkly_server_sdk::{EventProcessorBuilder, ConfigBuilder};
/// # use hyper_rustls::HttpsConnector;
/// # use hyper::client::HttpConnector;
/// # use std::time::Duration;
/// # fn main() {
///     ConfigBuilder::new("sdk-key").event_processor(EventProcessorBuilder::<HttpsConnector<HttpConnector>>::new()
///         .flush_interval(Duration::from_secs(10)));
/// # }
/// ```
#[derive(Clone)]
pub struct EventProcessorBuilder<C> {
    capacity: usize,
    flush_interval: Duration,
    context_keys_capacity: NonZeroUsize,
    context_keys_flush_interval: Duration,
    event_sender: Option<Arc<dyn EventSender>>,
    all_attributes_private: bool,
    private_attributes: HashSet<Reference>,
    connector: Option<C>,
    omit_anonymous_contexts: bool,
    compress_events: bool,
    // diagnostic_recording_interval: Duration
}

#[cfg(feature = "native-certs")]
fn create_https_connector_with_native_certs() -> HttpsConnector<HttpConnector> {
    // Load native certs
    let mut root_store = rustls::RootCertStore::empty();
    match rustls_native_certs::load_native_certs() {
        Ok(certs) => {
            for cert in certs {
                root_store
                    .add(&rustls::Certificate(cert.0))
                    .unwrap_or_else(|e| {
                        eprintln!("Failed to add certificate: {}", e);
                    });
            }
            println!("Added {} certificates", root_store.len());
        }
        Err(e) => {
            eprintln!("Failed to load native certificates: {}", e);
            // Fall back to an empty store, which will likely fail connections
        }
    }

    // Create TLS config
    let tls_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    // Build the HTTPS connector
    HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_only()
        .enable_http1()
        .enable_http2()
        .build()
}

impl<C> EventProcessorFactory for EventProcessorBuilder<C>
where
    C: Service<Uri> + Clone + Send + Sync + 'static,
    C::Response: Connection + AsyncRead + AsyncWrite + Send + Unpin,
    C::Future: Send + Unpin + 'static,
    C::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    fn build(
        &self,
        endpoints: &service_endpoints::ServiceEndpoints,
        sdk_key: &str,
        tags: Option<String>,
    ) -> Result<Arc<dyn EventProcessor>, BuildError> {
        let url_string = format!("{}/bulk", endpoints.events_base_url());

        let mut default_headers = HashMap::<&str, String>::new();

        if let Some(tags) = tags {
            default_headers.insert(LAUNCHDARKLY_TAGS_HEADER, tags);
        }

        let event_sender_result: Result<Arc<dyn EventSender>, BuildError> =
            // NOTE: This would only be possible under unit testing conditions.
            if let Some(event_sender) = &self.event_sender {
                Ok(event_sender.clone())
            } else if let Some(connector) = &self.connector {
                Ok(Arc::new(HyperEventSender::new(
                    connector.clone(),
                    hyper::Uri::from_str(url_string.as_str()).unwrap(),
                    sdk_key,
                    default_headers,
                    self.compress_events,
                )))
            } else {
                #[cfg(feature = "rustls")]
                {
                    #[cfg(feature = "native-certs")]
                    {
                        let connector = create_https_connector_with_native_certs();
                        Ok(Arc::new(HyperEventSender::new(
                            connector,
                            hyper::Uri::from_str(url_string.as_str()).unwrap(),
                            sdk_key,
                            default_headers,
                            self.compress_events,
                        )))
                    }

                    #[cfg(not(feature = "native-certs"))]
                    {

                        let connector = HttpsConnectorBuilder::new()
                            .with_native_roots()
                            .https_or_http()
                            .enable_http1()
                            .enable_http2()
                            .build();
    
                        Ok(Arc::new(HyperEventSender::new(
                            connector,
                            hyper::Uri::from_str(url_string.as_str()).unwrap(),
                            sdk_key,
                            default_headers,
                            self.compress_events,
                        )))
                    }
                }
                #[cfg(not(feature = "rustls"))]
                Err(BuildError::InvalidConfig(
                    "https connector is required when rustls is disabled".into(),
                ))
            };
        let event_sender = event_sender_result?;

        let events_configuration = EventsConfiguration {
            event_sender,
            capacity: self.capacity,
            flush_interval: self.flush_interval,
            context_keys_capacity: self.context_keys_capacity,
            context_keys_flush_interval: self.context_keys_flush_interval,
            all_attributes_private: self.all_attributes_private,
            private_attributes: self.private_attributes.clone(),
            omit_anonymous_contexts: self.omit_anonymous_contexts,
        };

        let events_processor =
            EventProcessorImpl::new(events_configuration).map_err(BuildError::FailedToStart)?;

        Ok(Arc::new(events_processor))
    }

    fn to_owned(&self) -> Box<dyn EventProcessorFactory> {
        Box::new(self.clone())
    }
}

impl<C> EventProcessorBuilder<C> {
    /// Create a new [EventProcessorBuilder] with all default values.
    pub fn new() -> Self {
        Self {
            capacity: DEFAULT_EVENT_CAPACITY,
            flush_interval: DEFAULT_FLUSH_POLL_INTERVAL,
            context_keys_capacity: DEFAULT_CONTEXT_KEY_CAPACITY
                .unwrap_or_else(|| NonZeroUsize::new(1).unwrap()),
            context_keys_flush_interval: DEFAULT_CONTEXT_KEYS_FLUSH_INTERVAL,
            event_sender: None,
            all_attributes_private: false,
            private_attributes: HashSet::new(),
            omit_anonymous_contexts: false,
            connector: None,
            compress_events: false,
        }
    }

    /// Set the capacity of the events buffer.
    ///
    /// The client buffers up to this many events in memory before flushing. If the capacity is exceeded before
    /// the buffer is flushed [crate::EventProcessor::flush], events will be discarded. Increasing the
    /// capacity means that events are less likely to be discarded, at the cost of consuming more memory.
    ///
    pub fn capacity(&mut self, capacity: usize) -> &mut Self {
        self.capacity = capacity;
        self
    }

    /// Sets the interval between flushes of the event buffer.
    ///
    /// Decreasing the flush interval means that the event buffer is less likely to reach capacity.
    pub fn flush_interval(&mut self, flush_interval: Duration) -> &mut Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Sets the number of context keys that the event processor can remember at any one time.
    ///
    /// To avoid sending duplicate context details in analytics events, the SDK maintains a cache of
    /// recently seen context keys.
    pub fn context_keys_capacity(&mut self, context_keys_capacity: NonZeroUsize) -> &mut Self {
        self.context_keys_capacity = context_keys_capacity;
        self
    }

    /// Sets the interval at which the event processor will reset its cache of known context keys.
    pub fn context_keys_flush_interval(
        &mut self,
        context_keys_flush_interval: Duration,
    ) -> &mut Self {
        self.context_keys_flush_interval = context_keys_flush_interval;
        self
    }

    /// Sets whether or not all optional user attributes should be hidden from LaunchDarkly.
    ///
    /// If this is true, all user attribute values (other than the key) will be private, not just the attributes
    /// specified with private_attributes or on a per-user basis with UserBuilder methods. By default, it is false.
    pub fn all_attributes_private(&mut self, all_attributes_private: bool) -> &mut Self {
        self.all_attributes_private = all_attributes_private;
        self
    }

    /// Marks a set of attribute names as always private.
    ///
    /// Any users sent to LaunchDarkly with this configuration active will have attributes with these
    /// names removed. This is in addition to any attributes that were marked as private for an
    /// individual user with UserBuilder methods. Setting all_attribute_private to true overrides this.
    pub fn private_attributes<R>(&mut self, attributes: HashSet<R>) -> &mut Self
    where
        R: Into<Reference>,
    {
        self.private_attributes = attributes.into_iter().map(|a| a.into()).collect();
        self
    }

    /// Sets the connector for the event sender to use. This allows for re-use of a connector
    /// between multiple client instances. This is especially useful for the `sdk-test-harness`
    /// where many client instances are created throughout the test and reading the native
    /// certificates is a substantial portion of the runtime.
    pub fn https_connector(&mut self, connector: C) -> &mut Self {
        self.connector = Some(connector);
        self
    }

    /// Sets whether anonymous contexts should be omitted from index and identify events.
    ///
    /// The default is false, meaning that anonymous contexts will be included in index and
    /// identify events.
    pub fn omit_anonymous_contexts(&mut self, omit: bool) -> &mut Self {
        self.omit_anonymous_contexts = omit;
        self
    }

    #[cfg(feature = "event-compression")]
    /// Should the event payload sent to LaunchDarkly use gzip compression. By
    /// default this is false to prevent backward breaking compatibility issues with
    /// older versions of the relay proxy.
    //
    /// Customers not using the relay proxy are strongly encouraged to enable this
    /// feature to reduce egress bandwidth cost.
    pub fn compress_events(&mut self, enabled: bool) -> &mut Self {
        self.compress_events = enabled;
        self
    }

    #[cfg(test)]
    /// Test only functionality that allows us to override the event sender.
    pub fn event_sender(&mut self, event_sender: Arc<dyn EventSender>) -> &mut Self {
        self.event_sender = Some(event_sender);
        self
    }
}

impl<C> Default for EventProcessorBuilder<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// An implementation of EventProcessorFactory that will discard all events received. This should
/// only be used for unit tests.
#[derive(Clone)]
pub struct NullEventProcessorBuilder {}

impl EventProcessorFactory for NullEventProcessorBuilder {
    fn build(
        &self,
        _: &service_endpoints::ServiceEndpoints,
        _: &str,
        _: Option<String>,
    ) -> Result<Arc<dyn EventProcessor>, BuildError> {
        Ok(Arc::new(NullEventProcessor::new()))
    }

    fn to_owned(&self) -> Box<dyn EventProcessorFactory> {
        Box::new(self.clone())
    }
}

impl NullEventProcessorBuilder {
    /// Create a new [NullEventProcessorBuilder] with all default values.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NullEventProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use hyper::client::HttpConnector;
    use launchdarkly_server_sdk_evaluation::ContextBuilder;
    use maplit::hashset;
    use mockito::Matcher;
    use test_case::test_case;

    use crate::{events::event::EventFactory, ServiceEndpointsBuilder};

    use super::*;

    #[test]
    fn default_builder_has_correct_defaults() {
        let builder = EventProcessorBuilder::<HttpConnector>::new();
        assert_eq!(builder.capacity, DEFAULT_EVENT_CAPACITY);
        assert_eq!(builder.flush_interval, DEFAULT_FLUSH_POLL_INTERVAL);
    }

    #[test]
    fn capacity_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::<HttpConnector>::new();
        builder.capacity(1234);
        assert_eq!(builder.capacity, 1234);
    }

    #[test]
    fn flush_interval_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::<HttpConnector>::new();
        builder.flush_interval(Duration::from_secs(1234));
        assert_eq!(builder.flush_interval, Duration::from_secs(1234));
    }

    #[test]
    fn context_keys_capacity_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::<HttpConnector>::new();
        let cap = NonZeroUsize::new(1234).expect("1234 > 0");
        builder.context_keys_capacity(cap);
        assert_eq!(builder.context_keys_capacity, cap);
    }

    #[test]
    fn context_keys_flush_interval_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::<HttpConnector>::new();
        builder.context_keys_flush_interval(Duration::from_secs(1000));
        assert_eq!(
            builder.context_keys_flush_interval,
            Duration::from_secs(1000)
        );
    }

    #[test]
    fn all_attribute_private_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::<HttpConnector>::new();

        assert!(!builder.all_attributes_private);
        builder.all_attributes_private(true);
        assert!(builder.all_attributes_private);
    }

    #[test]
    fn attribte_names_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::<HttpConnector>::new();

        assert!(builder.private_attributes.is_empty());
        builder.private_attributes(hashset!["name"]);
        assert!(builder.private_attributes.contains(&"name".into()));
    }

    #[test_case(Some("application-id/abc:application-sha/xyz".into()), "application-id/abc:application-sha/xyz")]
    #[test_case(None, Matcher::Missing)]
    fn processor_sends_correct_headers(tag: Option<String>, matcher: impl Into<Matcher>) {
        let mut server = mockito::Server::new();
        let mock = server
            .mock("POST", "/bulk")
            .with_status(200)
            .expect_at_least(1)
            .match_header(LAUNCHDARKLY_TAGS_HEADER, matcher)
            .create();

        let service_endpoints = ServiceEndpointsBuilder::new()
            .events_base_url(&server.url())
            .polling_base_url(&server.url())
            .streaming_base_url(&server.url())
            .build()
            .expect("Service endpoints failed to be created");

        let builder = EventProcessorBuilder::<HttpConnector>::new();
        let processor = builder
            .build(&service_endpoints, "sdk-key", tag)
            .expect("Processor failed to build");

        let event_factory = EventFactory::new(false);

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let identify_event = event_factory.new_identify(context);

        processor.send(identify_event);
        processor.close();

        mock.assert()
    }
}
