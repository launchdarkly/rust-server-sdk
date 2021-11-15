use super::client::Error;
use super::event_sink as sink;
use crate::event_processor::{EventProcessor, EventProcessorImpl};
use crate::service_endpoints;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

const DEFAULT_FLUSH_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_EVENT_CAPACITY: usize = 500;
const DEFAULT_USER_KEY_SIZE: usize = 1000;

/// Trait which allows creation of event processors. Should be implemented by event processor
/// builder types.
pub trait EventProcessorFactory {
    fn build(
        &self,
        endpoints: &service_endpoints::ServiceEndpoints,
        sdk_key: &str,
    ) -> Result<Arc<Mutex<dyn EventProcessor>>, Error>;
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
/// # use std::time::Duration;
/// # fn main() {
///     ConfigBuilder::new("sdk-key").event_processor(EventProcessorBuilder::new()
///         .flush_interval(Duration::from_secs(10)));
/// # }
/// ```
#[derive(Clone)]
pub struct EventProcessorBuilder {
    capacity: usize,
    flush_interval: Duration,
    event_sink: Option<Arc<RwLock<dyn sink::EventSink>>>,
    user_keys_capacity: usize,
    // inline_user_in_events: bool,
    // all_attributes_private: bool,
    // private_attributes: Vec<String>,
    // user_keys_capacity: usize,
    // user_keys_flush_interval: Duration,
    // diagnostic_recording_interval: Duration,
}

impl EventProcessorFactory for EventProcessorBuilder {
    fn build(
        &self,
        endpoints: &service_endpoints::ServiceEndpoints,
        sdk_key: &str,
    ) -> Result<Arc<Mutex<dyn EventProcessor>>, Error> {
        Ok(Arc::new(Mutex::new(
            reqwest::Url::parse(endpoints.events_base_url())
                .map_err(|e| Error::InvalidConfig(format!("couldn't parse events_base_url: {}", e)))
                .and_then(|base_url| match &self.event_sink {
                    None => EventProcessorImpl::new(
                        base_url,
                        sdk_key,
                        self.flush_interval,
                        self.capacity,
                        self.user_keys_capacity,
                    )
                    .map_err(|e| Error::InvalidConfig(format!("invalid events_base_url: {}", e))),
                    Some(event_sink) => EventProcessorImpl::new_with_sink(
                        event_sink.clone(),
                        self.flush_interval,
                        self.capacity,
                        self.user_keys_capacity,
                    )
                    .map_err(|e| Error::InvalidConfig(format!("invalid events_base_url: {}", e))),
                })?,
        )))
    }

    fn to_owned(&self) -> Box<dyn EventProcessorFactory> {
        Box::new(self.clone())
    }
}

impl EventProcessorBuilder {
    pub fn new() -> Self {
        Self {
            capacity: DEFAULT_EVENT_CAPACITY,
            flush_interval: DEFAULT_FLUSH_POLL_INTERVAL,
            user_keys_capacity: DEFAULT_USER_KEY_SIZE,
            event_sink: None,
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

    /// Sets the number of user keys that the event processor can remember at any one time.
    ///
    /// To avoid sending duplicate user details in analytics events, the SDK maintains a cache of
    /// recently seen user keys.
    pub fn user_keys_capacity(&mut self, user_keys_capacity: usize) -> &mut Self {
        self.user_keys_capacity = user_keys_capacity;
        self
    }

    #[cfg(test)] //TODO: Should we make this not just for test?
    pub fn event_sink(&mut self, event_sink: Arc<RwLock<dyn sink::EventSink>>) -> &mut Self {
        self.event_sink = Some(event_sink);
        self
    }
}

impl Default for EventProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_builder_has_correct_defaults() {
        let builder = EventProcessorBuilder::new();
        assert_eq!(builder.capacity, DEFAULT_EVENT_CAPACITY);
        assert_eq!(builder.flush_interval, DEFAULT_FLUSH_POLL_INTERVAL);
        assert!(builder.event_sink.is_none());
    }

    #[test]
    fn capacity_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::new();
        builder.capacity(1234);
        assert_eq!(builder.capacity, 1234);
    }

    #[test]
    fn flush_interval_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::new();
        builder.flush_interval(Duration::from_secs(1234));
        assert_eq!(builder.flush_interval, Duration::from_secs(1234));
    }

    #[test]
    fn user_keys_capacity_can_be_adjusted() {
        let mut builder = EventProcessorBuilder::new();
        builder.user_keys_capacity(1234);
        assert_eq!(builder.user_keys_capacity, 1234);
    }
}
