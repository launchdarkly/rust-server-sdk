use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use self::sender::EventSender;

pub mod dispatcher;
pub mod event;
pub mod processor;
pub mod processor_builders;
pub mod sender;

pub struct EventsConfiguration {
    capacity: usize,
    event_sender: Arc<dyn EventSender>,
    flush_interval: Duration,
    inline_users_in_events: bool,
    user_keys_capacity: usize,
    user_keys_flush_interval: Duration,
    all_attributes_private: bool,
    private_attributes: HashSet<String>,
}

#[cfg(test)]
fn create_events_configuration(
    event_sender: self::sender::InMemoryEventSender,
    inline_users_in_events: bool,
    flush_interval: Duration,
) -> EventsConfiguration {
    EventsConfiguration {
        capacity: 5,
        event_sender: Arc::new(event_sender),
        flush_interval,
        inline_users_in_events,
        user_keys_capacity: 5,
        user_keys_flush_interval: Duration::from_secs(100),
        all_attributes_private: false,
        private_attributes: HashSet::new(),
    }
}

#[cfg(test)]
pub(super) fn create_event_sender() -> (
    self::sender::InMemoryEventSender,
    crossbeam_channel::Receiver<self::event::OutputEvent>,
) {
    let (event_tx, event_rx) = crossbeam_channel::unbounded();
    (self::sender::InMemoryEventSender::new(event_tx), event_rx)
}
