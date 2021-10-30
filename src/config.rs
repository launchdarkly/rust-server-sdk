use crate::data_source_builders::DataSourceFactory;
use crate::event_processor_builders::{EventProcessorBuilder, EventProcessorFactory};
use crate::{ServiceEndpointsBuilder, StreamingDataSourceBuilder};

use std::borrow::Borrow;

/// Immutable configuration object for [crate::Client].
///
/// [Config] instances can be created using a [ConfigBuilder].
pub struct Config {
    sdk_key: String,
    service_endpoints_builder: ServiceEndpointsBuilder,
    data_source_builder: Box<dyn DataSourceFactory + Send>,
    event_processor_builder: Box<dyn EventProcessorFactory + Send>,
    inline_users_in_events: bool,
}

impl Config {
    pub fn sdk_key(&self) -> &str {
        &self.sdk_key
    }

    pub fn service_endpoints_builder(&self) -> &ServiceEndpointsBuilder {
        &self.service_endpoints_builder
    }

    pub fn data_source_builder(&self) -> &(dyn DataSourceFactory + Send) {
        self.data_source_builder.borrow()
    }

    pub fn event_processor_builder(&self) -> &(dyn EventProcessorFactory + Send) {
        self.event_processor_builder.borrow()
    }

    pub fn inline_users_in_events(&self) -> bool {
        self.inline_users_in_events
    }
}

/// Used to create a [Config] struct for creating [crate::Client] instances.
///
/// For usage examples see:
/// - [crate::ServiceEndpointsBuilder]
/// - [crate::StreamingDataSourceBuilder]
/// - [crate::EventProcessorBuilder]
pub struct ConfigBuilder {
    service_endpoints_builder: Option<ServiceEndpointsBuilder>,
    data_source_builder: Option<Box<dyn DataSourceFactory + Send>>,
    event_processor_builder: Option<Box<dyn EventProcessorFactory + Send>>,
    inline_users_in_events: bool,
    sdk_key: String,
}

impl ConfigBuilder {
    pub fn new(sdk_key: &str) -> Self {
        Self {
            service_endpoints_builder: None,
            data_source_builder: None,
            event_processor_builder: None,
            inline_users_in_events: false,
            sdk_key: sdk_key.to_string(),
        }
    }
    /// Set the URLs to use for this client. For usage see [ServiceEndpointsBuilder]
    pub fn service_endpoints(mut self, builder: &ServiceEndpointsBuilder) -> Self {
        self.service_endpoints_builder = Some(builder.clone());
        self
    }

    /// Set the data source to use for this client.
    /// For usage see [crate::data_source_builders::StreamingDataSourceBuilder]
    pub fn data_source(mut self, builder: &dyn DataSourceFactory) -> Self {
        self.data_source_builder = Some(builder.to_owned());
        self
    }

    /// Set the data source to use for this client.
    /// For usage see [crate::event_processor_builders::EventProcessorBuilder]
    pub fn event_processor(mut self, builder: &dyn EventProcessorFactory) -> Self {
        self.event_processor_builder = Some(builder.to_owned());
        self
    }

    /// Sets whether to include full user details in every analytics event.
    ///
    /// The default is `false`: events will only include the user key, except for one "index" event
    /// that provides the full details for the user).
    pub fn inline_users_in_events(mut self, inline: bool) -> Self {
        self.inline_users_in_events = inline;
        self
    }

    pub fn build(self) -> Config {
        let service_endpoints_builder = match &self.service_endpoints_builder {
            None => ServiceEndpointsBuilder::new(),
            Some(service_endpoints_builder) => service_endpoints_builder.clone(),
        };

        let data_source_builder = match &self.data_source_builder {
            None => Box::new(StreamingDataSourceBuilder::new()),
            Some(_data_source_builder) => self.data_source_builder.unwrap(),
        };
        let event_processor_builder = match &self.event_processor_builder {
            None => Box::new(EventProcessorBuilder::new()),
            Some(_event_processor_builder) => self.event_processor_builder.unwrap(),
        };

        Config {
            sdk_key: self.sdk_key,
            service_endpoints_builder,
            data_source_builder,
            event_processor_builder,
            inline_users_in_events: self.inline_users_in_events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_configured_with_custom_endpoints() {
        let builder = ConfigBuilder::new("sdk-key").service_endpoints(
            ServiceEndpointsBuilder::new().relay_proxy("http://my-relay-hostname:8080"),
        );

        let endpoints = builder.service_endpoints_builder.unwrap().build().unwrap();
        assert_eq!(
            endpoints.streaming_base_url(),
            "http://my-relay-hostname:8080"
        );
        assert_eq!(
            endpoints.polling_base_url(),
            "http://my-relay-hostname:8080"
        );
        assert_eq!(endpoints.events_base_url(), "http://my-relay-hostname:8080");
    }
}
