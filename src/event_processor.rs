use super::event_sink as sink;
use super::events::Event;

use std::sync::{Arc, RwLock};

use reqwest as r;
use reqwest::r#async as ra;

type Error = String; // TODO

pub struct EventProcessor {
    sink: Arc<RwLock<dyn sink::EventSink>>,
}

impl EventProcessor {
    pub fn new(base_url: r::Url, sdk_key: &str) -> Self {
        Self::new_with_sink(Arc::new(RwLock::new(sink::OneShotTokioSink {
            http: ra::Client::new(),
            base_url,
            sdk_key: sdk_key.to_string(),
        })))
    }

    pub fn new_with_sink(sink: Arc<RwLock<dyn sink::EventSink>>) -> Self {
        EventProcessor { sink }
    }

    pub fn send(&self, event: Event) {
        debug!("Okay, sending event: {}", event);

        self._send(event).expect("failed to send event");
    }

    fn _send(&self, event: Event) -> Result<(), Error> {
        let result = self.sink.write().unwrap().send(&event);

        if let Some(index) = event.make_index_event() {
            self._send(index)?;
        }

        // TODO make real summaries once we have batching
        if let Some(summary) = event.make_singleton_summary() {
            self._send(summary)?;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use spectral::prelude::*;

    use super::*;
    use crate::events::BaseEvent;
    use crate::users::User;

    #[test]
    fn sends_index_event() {
        let events = Arc::new(RwLock::new(sink::MockSink::new()));
        let ep = EventProcessor::new_with_sink(events.clone());

        let user =
            crate::events::MaybeInlinedUser::Inlined(User::with_key("foo".to_string()).build());
        let index = Event::Index {
            base: BaseEvent {
                creation_date: 42,
                user,
            },
        };
        ep.send(index.clone());

        let events = events.read().unwrap();
        assert_that!(*events).has_length(1);
        let event = &events[0];
        assert_that!(event).is_equal_to(&index);
    }
}
