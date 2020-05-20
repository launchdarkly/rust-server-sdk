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

    pub fn new_with_sink(sink: Arc<RwLock<(impl sink::EventSink + 'static)>>) -> Self {
        EventProcessor { sink }
    }

    pub fn send(&self, event: Event) {
        debug!("Okay, sending event: {}", event);

        self._send(event).expect("failed to send event");
    }

    fn _send(&self, event: Event) -> Result<(), Error> {
        let json = serde_json::to_vec(&vec![&event]).map_err(|e| e.to_string())?;
        debug!(
            "Sending: {}",
            serde_json::to_string_pretty(&vec![&event]).map_err(|e| e.to_string())?,
        );

        let result = self.sink.write().unwrap().send(json);

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
    use crate::users::UserBuilder;

    #[test]
    fn serializes_index_event() {
        let jsons = Arc::new(RwLock::new(sink::TestSink::new()));
        let ep = EventProcessor::new_with_sink(jsons.clone());

        let user =
            crate::events::MaybeInlinedUser::Inlined(UserBuilder::new("foo".to_string()).build());
        ep.send(Event::Index {
            base: BaseEvent {
                creation_date: 42,
                user,
            },
        });

        let jsons = jsons.read().unwrap();
        assert_that!(*jsons).has_length(1);
        let json = &jsons[0];
        assert_that!(utf8(json)).is_equal_to(
            r#"[{"kind":"index","creationDate":42,"user":{"key":"foo","custom":{}}}]"#,
        );
    }

    fn utf8(bytes: &[u8]) -> &str {
        std::str::from_utf8(bytes).expect("not utf-8")
    }
}
