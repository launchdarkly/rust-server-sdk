use crate::data_source_builders::{DataSourceFactory, NullDataSourceBuilder};
use crate::data_store_builders::{DataStoreFactory, InMemoryDataStoreBuilder};
use crate::event_processor_builders::{
    EventProcessorBuilder, EventProcessorFactory, NullEventProcessorBuilder,
};
use crate::{ServiceEndpointsBuilder, StreamingDataSourceBuilder};

use std::borrow::Borrow;

/// Immutable configuration object for [crate::Client].
///
/// [Config] instances can be created using a [ConfigBuilder].
pub struct Config {
    sdk_key: String,
    service_endpoints_builder: ServiceEndpointsBuilder,
    data_store_builder: Box<dyn DataStoreFactory>,
    data_source_builder: Box<dyn DataSourceFactory>,
    event_processor_builder: Box<dyn EventProcessorFactory>,
    inline_users_in_events: bool,
    offline: bool,
}

impl Config {
    pub fn sdk_key(&self) -> &str {
        &self.sdk_key
    }

    pub fn service_endpoints_builder(&self) -> &ServiceEndpointsBuilder {
        &self.service_endpoints_builder
    }

    pub fn data_store_builder(&self) -> &(dyn DataStoreFactory) {
        self.data_store_builder.borrow()
    }

    pub fn data_source_builder(&self) -> &(dyn DataSourceFactory) {
        self.data_source_builder.borrow()
    }

    pub fn event_processor_builder(&self) -> &(dyn EventProcessorFactory) {
        self.event_processor_builder.borrow()
    }

    pub fn inline_users_in_events(&self) -> bool {
        self.inline_users_in_events
    }

    pub fn offline(&self) -> bool {
        self.offline
    }
}

/// Used to create a [Config] struct for creating [crate::Client] instances.
///
/// For usage examples see:
// TODO(doc) Include the data store builder example once we have something that can be customized
/// - [crate::ServiceEndpointsBuilder]
/// - [crate::StreamingDataSourceBuilder]
/// - [crate::EventProcessorBuilder]
pub struct ConfigBuilder {
    service_endpoints_builder: Option<ServiceEndpointsBuilder>,
    data_store_builder: Option<Box<dyn DataStoreFactory>>,
    data_source_builder: Option<Box<dyn DataSourceFactory>>,
    event_processor_builder: Option<Box<dyn EventProcessorFactory>>,
    inline_users_in_events: bool,
    offline: bool,
    sdk_key: String,
}

impl ConfigBuilder {
    pub fn new(sdk_key: &str) -> Self {
        Self {
            service_endpoints_builder: None,
            data_store_builder: None,
            data_source_builder: None,
            event_processor_builder: None,
            inline_users_in_events: false,
            offline: false,
            sdk_key: sdk_key.to_string(),
        }
    }
    /// Set the URLs to use for this client. For usage see [ServiceEndpointsBuilder]
    pub fn service_endpoints(mut self, builder: &ServiceEndpointsBuilder) -> Self {
        self.service_endpoints_builder = Some(builder.clone());
        self
    }

    /// Set the data store to use for this client.
    pub fn data_store(mut self, builder: &dyn DataStoreFactory) -> Self {
        self.data_store_builder = Some(builder.to_owned());
        self
    }

    /// Set the data source to use for this client.
    /// For usage see [crate::data_source_builders::StreamingDataSourceBuilder]
    ///
    /// If offline mode is enabled, this data source will be ignored.
    pub fn data_source(mut self, builder: &dyn DataSourceFactory) -> Self {
        self.data_source_builder = Some(builder.to_owned());
        self
    }

    /// Set the event processor to use for this client.
    /// For usage see [crate::event_processor_builders::EventProcessorBuilder]
    ///
    /// If offline mode is enabled, this event processor will be ignored.
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

    /// Whether the client should be initialized in offline mode.
    ///
    /// In offline mode, default values are returned for all flags and no remote network requests
    /// are made. By default, this is false.
    pub fn offline(mut self, offline: bool) -> Self {
        self.offline = offline;
        self
    }

    pub fn build(self) -> Config {
        let service_endpoints_builder = match &self.service_endpoints_builder {
            None => ServiceEndpointsBuilder::new(),
            Some(service_endpoints_builder) => service_endpoints_builder.clone(),
        };

        let data_store_builder = match &self.data_store_builder {
            None => Box::new(InMemoryDataStoreBuilder::new()),
            Some(_data_store_builder) => self.data_store_builder.unwrap(),
        };

        let data_source_builder: Box<dyn DataSourceFactory> = match &self.data_source_builder {
            None if self.offline => Box::new(NullDataSourceBuilder::new()),
            Some(_) if self.offline => {
                warn!("Custom data source builders will be ignored when in offline mode");
                Box::new(NullDataSourceBuilder::new())
            }
            None => Box::new(StreamingDataSourceBuilder::new()),
            Some(_data_source_builder) => self.data_source_builder.unwrap(),
        };

        let event_processor_builder: Box<dyn EventProcessorFactory> =
            match &self.event_processor_builder {
                None if self.offline => Box::new(NullEventProcessorBuilder::new()),
                Some(_) if self.offline => {
                    warn!("Custom event processor builders will be ignored when in offline mode");
                    Box::new(NullEventProcessorBuilder::new())
                }
                None => Box::new(EventProcessorBuilder::new()),
                Some(_event_processor_builder) => self.event_processor_builder.unwrap(),
            };

        Config {
            sdk_key: self.sdk_key,
            service_endpoints_builder,
            data_store_builder,
            data_source_builder,
            event_processor_builder,
            inline_users_in_events: self.inline_users_in_events,
            offline: self.offline,
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
