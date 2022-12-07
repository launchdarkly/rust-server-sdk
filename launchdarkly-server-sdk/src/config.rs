use crate::data_source_builders::{DataSourceFactory, NullDataSourceBuilder};
use crate::events::processor_builders::{
    EventProcessorBuilder, EventProcessorFactory, NullEventProcessorBuilder,
};
use crate::stores::store_builders::{DataStoreFactory, InMemoryDataStoreBuilder};
use crate::{ServiceEndpointsBuilder, StreamingDataSourceBuilder};

use std::borrow::Borrow;

#[derive(Debug)]
struct Tag {
    key: String,
    value: String,
}

impl Tag {
    fn is_valid(&self) -> Result<(), &str> {
        if self.value.chars().count() > 64 {
            return Err("Value was longer than 64 characters and was discarded");
        }

        if self.key.is_empty() || !self.key.chars().all(Tag::valid_characters) {
            return Err("Key was empty or contained invalid characters");
        }

        if self.value.is_empty() || !self.value.chars().all(Tag::valid_characters) {
            return Err("Value was empty or contained invalid characters");
        }

        Ok(())
    }

    fn valid_characters(c: char) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_')
    }
}

impl ToString for &Tag {
    fn to_string(&self) -> String {
        format!("{}/{}", self.key, self.value)
    }
}

/// ApplicationInfo allows configuration of application metadata.
///
/// If you want to set non-default values for any of these fields, create a new instance with
/// [ApplicationInfo::new] and pass it to [ConfigBuilder::application_info].
pub struct ApplicationInfo {
    tags: Vec<Tag>,
}

impl ApplicationInfo {
    /// Create a new default instance of [ApplicationInfo].
    pub fn new() -> Self {
        Self { tags: Vec::new() }
    }

    /// A unique identifier representing the application where the LaunchDarkly SDK is running.
    ///
    /// This can be specified as any string value as long as it only uses the following characters:
    /// ASCII letters, ASCII digits, period, hyphen, underscore. A string containing any other
    /// characters will be ignored.
    pub fn application_identifier(&mut self, application_id: impl Into<String>) -> &mut Self {
        self.add_tag("application-id", application_id)
    }

    /// A unique identifier representing the version of the application where the LaunchDarkly SDK
    /// is running.
    ///
    /// This can be specified as any string value as long as it only uses the following characters:
    /// ASCII letters, ASCII digits, period, hyphen, underscore. A string containing any other
    /// characters will be ignored.
    pub fn application_version(&mut self, application_version: impl Into<String>) -> &mut Self {
        self.add_tag("application-version", application_version)
    }

    fn add_tag(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        let tag = Tag {
            key: key.into(),
            value: value.into(),
        };

        match tag.is_valid() {
            Ok(_) => self.tags.push(tag),
            Err(e) => {
                warn!("{}", e)
            }
        }

        self
    }

    pub(crate) fn build(&self) -> Option<String> {
        if self.tags.is_empty() {
            return None;
        }

        let mut tags = self
            .tags
            .iter()
            .map(|tag| tag.to_string())
            .collect::<Vec<String>>();

        tags.sort();
        tags.dedup();

        Some(tags.join(" "))
    }
}

impl Default for ApplicationInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable configuration object for [crate::Client].
///
/// [Config] instances can be created using a [ConfigBuilder].
pub struct Config {
    sdk_key: String,
    service_endpoints_builder: ServiceEndpointsBuilder,
    data_store_builder: Box<dyn DataStoreFactory>,
    data_source_builder: Box<dyn DataSourceFactory>,
    event_processor_builder: Box<dyn EventProcessorFactory>,
    application_tag: Option<String>,
    offline: bool,
}

impl Config {
    /// Returns the sdk key.
    pub fn sdk_key(&self) -> &str {
        &self.sdk_key
    }

    /// Returns the [ServiceEndpointsBuilder]
    pub fn service_endpoints_builder(&self) -> &ServiceEndpointsBuilder {
        &self.service_endpoints_builder
    }

    /// Returns the DataStoreFactory
    pub fn data_store_builder(&self) -> &(dyn DataStoreFactory) {
        self.data_store_builder.borrow()
    }

    /// Returns the DataSourceFactory
    pub fn data_source_builder(&self) -> &(dyn DataSourceFactory) {
        self.data_source_builder.borrow()
    }

    /// Returns the EventProcessorFactory
    pub fn event_processor_builder(&self) -> &(dyn EventProcessorFactory) {
        self.event_processor_builder.borrow()
    }

    /// Returns the offline status
    pub fn offline(&self) -> bool {
        self.offline
    }

    /// Returns the tag builder if provided
    pub fn application_tag(&self) -> &Option<String> {
        &self.application_tag
    }
}

/// Used to create a [Config] struct for creating [crate::Client] instances.
///
/// For usage examples see:
/// - [Creating service endpoints](crate::ServiceEndpointsBuilder)
/// - [Configuring a persistent data store](crate::PersistentDataStoreBuilder)
/// - [Configuring the streaming data source](crate::StreamingDataSourceBuilder)
/// - [Configuring events sent to LaunchDarkly](crate::EventProcessorBuilder)
pub struct ConfigBuilder {
    service_endpoints_builder: Option<ServiceEndpointsBuilder>,
    data_store_builder: Option<Box<dyn DataStoreFactory>>,
    data_source_builder: Option<Box<dyn DataSourceFactory>>,
    event_processor_builder: Option<Box<dyn EventProcessorFactory>>,
    application_info: Option<ApplicationInfo>,
    offline: bool,
    sdk_key: String,
}

impl ConfigBuilder {
    /// Create a new instance of the [ConfigBuilder] with the provided `sdk_key`.
    pub fn new(sdk_key: &str) -> Self {
        Self {
            service_endpoints_builder: None,
            data_store_builder: None,
            data_source_builder: None,
            event_processor_builder: None,
            offline: false,
            application_info: None,
            sdk_key: sdk_key.to_string(),
        }
    }

    /// Set the URLs to use for this client. For usage see [ServiceEndpointsBuilder]
    pub fn service_endpoints(mut self, builder: &ServiceEndpointsBuilder) -> Self {
        self.service_endpoints_builder = Some(builder.clone());
        self
    }

    /// Set the data store to use for this client.
    ///
    /// By default, the SDK uses an in-memory data store.
    /// For a persistent store, see [PersistentDataStoreBuilder](crate::stores::persistent_store_builders::PersistentDataStoreBuilder).
    pub fn data_store(mut self, builder: &dyn DataStoreFactory) -> Self {
        self.data_store_builder = Some(builder.to_owned());
        self
    }

    /// Set the data source to use for this client.
    /// For the streaming data source, see [StreamingDataSourceBuilder](crate::data_source_builders::StreamingDataSourceBuilder).
    ///
    /// If offline mode is enabled, this data source will be ignored.
    pub fn data_source(mut self, builder: &dyn DataSourceFactory) -> Self {
        self.data_source_builder = Some(builder.to_owned());
        self
    }

    /// Set the event processor to use for this client.
    /// For usage see [EventProcessorBuilder](crate::EventProcessorBuilder).
    ///
    /// If offline mode is enabled, this event processor will be ignored.
    pub fn event_processor(mut self, builder: &dyn EventProcessorFactory) -> Self {
        self.event_processor_builder = Some(builder.to_owned());
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

    /// Provides configuration of application metadata.
    ///
    /// These properties are optional and informational. They may be used in LaunchDarkly analytics
    /// or other product features, but they do not affect feature flag evaluations.
    pub fn application_info(mut self, application_info: ApplicationInfo) -> Self {
        self.application_info = Some(application_info);
        self
    }

    /// Create a new instance of [Config] based on the [ConfigBuilder] configuration.
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

        let application_tag = match self.application_info {
            Some(tb) => tb.build(),
            _ => None,
        };

        Config {
            sdk_key: self.sdk_key,
            service_endpoints_builder,
            data_store_builder,
            data_source_builder,
            event_processor_builder,
            application_tag,
            offline: self.offline,
        }
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

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

    #[test]
    fn unconfigured_config_builder_handles_application_tags_correctly() {
        let builder = ConfigBuilder::new("sdk-key");
        let config = builder.build();

        assert_eq!(None, config.application_tag);
    }

    #[test_case("id", "version", Some("application-id/id application-version/version".to_string()))]
    #[test_case("Invalid id", "version", Some("application-version/version".to_string()))]
    #[test_case("id", "Invalid version", Some("application-id/id".to_string()))]
    #[test_case("Invalid id", "Invalid version", None)]
    fn config_builder_handles_application_tags_appropriately(
        id: impl Into<String>,
        version: impl Into<String>,
        expected: Option<String>,
    ) {
        let mut application_info = ApplicationInfo::new();
        application_info
            .application_identifier(id)
            .application_version(version);
        let builder = ConfigBuilder::new("sdk-key");
        let config = builder.application_info(application_info).build();

        assert_eq!(expected, config.application_tag);
    }

    #[test_case("", "abc", Err("Key was empty or contained invalid characters"); "Empty key")]
    #[test_case(" ", "abc", Err("Key was empty or contained invalid characters"); "Key with whitespace")]
    #[test_case("/", "abc", Err("Key was empty or contained invalid characters"); "Key with slash")]
    #[test_case(":", "abc", Err("Key was empty or contained invalid characters"); "Key with colon")]
    #[test_case("🦀", "abc", Err("Key was empty or contained invalid characters"); "Key with emoji")]
    #[test_case("abcABC123.-_", "abc", Ok(()); "Valid key")]
    #[test_case("abc", "", Err("Value was empty or contained invalid characters"); "Empty value")]
    #[test_case("abc", " ", Err("Value was empty or contained invalid characters"); "Value with whitespace")]
    #[test_case("abc", "/", Err("Value was empty or contained invalid characters"); "Value with slash")]
    #[test_case("abc", ":", Err("Value was empty or contained invalid characters"); "Value with colon")]
    #[test_case("abc", "🦀", Err("Value was empty or contained invalid characters"); "Value with emoji")]
    #[test_case("abc", "abcABC123.-_", Ok(()); "Valid value")]
    #[test_case("abc", "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl", Ok(()); "64 is the max length")]
    #[test_case("abc", "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm", Err("Value was longer than 64 characters and was discarded"); "65 is too far")]
    fn tag_can_determine_valid_values(key: &str, value: &str, expected_result: Result<(), &str>) {
        let tag = Tag {
            key: key.to_string(),
            value: value.to_string(),
        };
        assert_eq!(expected_result, tag.is_valid());
    }

    #[test_case(vec![], None; "No tags returns None")]
    #[test_case(vec![("application-id".into(), "gonfalon-be".into()), ("application-sha".into(), "abcdef".into())], Some("application-id/gonfalon-be application-sha/abcdef".into()); "Tags are formatted correctly")]
    #[test_case(vec![("key".into(), "xyz".into()), ("key".into(), "abc".into())], Some("key/abc key/xyz".into()); "Keys are ordered correctly")]
    #[test_case(vec![("key".into(), "abc".into()), ("key".into(), "abc".into())], Some("key/abc".into()); "Tags are deduped")]
    #[test_case(vec![("XYZ".into(), "xyz".into()), ("abc".into(), "abc".into())], Some("XYZ/xyz abc/abc".into()); "Keys are ascii sorted correctly")]
    #[test_case(vec![("abc".into(), "XYZ".into()), ("abc".into(), "abc".into())], Some("abc/XYZ abc/abc".into()); "Values are ascii sorted correctly")]
    #[test_case(vec![("".into(), "XYZ".into()), ("abc".into(), "xyz".into())], Some("abc/xyz".into()); "Invalid tags are filtered")]
    #[test_case(Vec::new(), None; "Empty tags returns None")]
    fn application_tag_builder_can_create_tag_string_correctly(
        tags: Vec<(String, String)>,
        expected_value: Option<String>,
    ) {
        let mut application_info = ApplicationInfo::new();

        tags.into_iter().for_each(|(key, value)| {
            application_info.add_tag(key, value);
        });

        assert_eq!(expected_value, application_info.build());
    }
}
