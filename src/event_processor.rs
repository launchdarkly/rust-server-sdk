use super::event_sink as sink;
use super::events::Event;

use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use reqwest as r;

type Error = String; // TODO

pub struct EventProcessor {
    batcher: Batcher<Event>,
}

impl EventProcessor {
    pub fn new(base_url: r::Url, sdk_key: &str) -> Result<Self, Error> {
        let sink = sink::ReqwestSink::new(&base_url, sdk_key)?;
        Self::new_with_sink(Arc::new(RwLock::new(sink)))
    }

    pub fn new_with_sink(sink: Arc<RwLock<dyn sink::EventSink>>) -> Result<Self, Error> {
        // TODO don't hardcode batcher params
        let batcher = Batcher::start(5, Duration::from_secs(5), move |events: Vec<Event>| {
            if let Err(e) = sink.write().unwrap().send(events) {
                warn!("failed to send event: {}", e);
            }
        })?;
        Ok(EventProcessor { batcher })
    }

    pub fn send(&self, event: Event) {
        debug!("Okay, sending event: {}", event);

        self._send(event).expect("failed to send event");
    }

    fn _send(&self, event: Event) -> Result<(), Error> {
        let result = self.batcher.add(event.clone());

        if let Some(index) = event.make_index_event() {
            self._send(index)?;
        }

        // TODO make real summaries now we have batching
        if let Some(summary) = event.make_singleton_summary() {
            self._send(summary)?;
        }

        result
    }
}

struct Batcher<T> {
    sender: mpsc::SyncSender<T>,
}

impl<T: Send + 'static> Batcher<T> {
    pub fn start(
        batch_size: usize,
        batch_timeout: Duration,
        on_batch: impl Fn(Vec<T>) + Send + 'static,
    ) -> Result<Batcher<T>, Error> {
        let (sender, receiver) = mpsc::sync_channel(500); // TODO hardcode

        let _worker_handle = thread::Builder::new()
            // TODO make this an object?
            .spawn(move || 'batch: loop {
                debug!("waiting for a batch to send");

                let batch_deadline: Instant = Instant::now() + batch_timeout;

                let mut batch = Vec::new();

                'event: loop {
                    debug!("waiting for an event to add to the batch");

                    let now = Instant::now();

                    let batch_ready = match receiver
                        .recv_timeout(batch_deadline.saturating_duration_since(now))
                    {
                        Ok(item) => {
                            batch.push(item);
                            batch.len() > batch_size
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            debug!("batch timer expired");
                            true
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            debug!("batch sender disconnected");
                            // TODO flush and terminate
                            true
                        }
                    };

                    if batch_ready {
                        break 'event;
                    }

                    debug!("Not ready to send batch");
                }

                if batch.is_empty() {
                    debug!("Nothing to send");
                } else {
                    on_batch(batch);
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(Batcher { sender })
    }

    pub fn add(&self, item: T) -> Result<(), Error> {
        self.sender.send(item).map_err(|e| e.to_string())
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
