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
        let batcher = Batcher::start(1000, Duration::from_secs(30), move |events: Vec<Event>| {
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

    pub fn flush(&self) -> Result<(), Error> {
        self.batcher.flush()
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
    flusher: mpsc::SyncSender<mpsc::SyncSender<()>>,
}

impl<T: Send + 'static> Batcher<T> {
    pub fn start(
        batch_size: usize,
        batch_timeout: Duration,
        on_batch: impl Fn(Vec<T>) + Send + 'static,
    ) -> Result<Batcher<T>, Error> {
        let (sender, receiver) = mpsc::sync_channel(500); // TODO hardcode

        let flush_poll_interval = Duration::from_millis(100); // TODO hardcode
        let (flusher, flush_receiver) = mpsc::sync_channel::<mpsc::SyncSender<()>>(10); // TODO hardcode

        let _worker_handle = thread::Builder::new()
            // TODO make this an object with methods, this is confusing
            .spawn(move || 'batch: loop {
                debug!("waiting for a batch to send");

                let batch_deadline: Instant = Instant::now() + batch_timeout;

                let mut batch = Vec::new();
                let mut flushes = Vec::new();

                'event: loop {
                    // Rust doesn't have a (stable) select on multiple channels, so we have to
                    // contort a bit to support flush without blocking flush requesters until the
                    // batch deadline
                    let mut batch_ready = match receiver.recv_timeout(flush_poll_interval) {
                        Ok(item) => {
                            batch.push(item);
                            batch.len() > batch_size
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => Instant::now() >= batch_deadline,
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            debug!("batch sender disconnected");
                            // TODO flush and terminate
                            true
                        }
                    };

                    // check if anyone asked us to flush
                    while let Ok(flush) = flush_receiver.try_recv() {
                        debug!("flush requested");
                        flushes.push(flush);
                        batch_ready = true;
                    }

                    if batch_ready {
                        break 'event;
                    }
                }

                if !batch.is_empty() {
                    if !flushes.is_empty() {
                        // flush requested, just send everything left in the channel (even if the
                        // current batch was "full")
                        let mut extra = 0;
                        while let Ok(item) = receiver.try_recv() {
                            batch.push(item);
                            extra += 1;
                        }
                        if extra > 0 {
                            debug!("Sending {} extra items early due to flush", extra);
                        }
                    }

                    debug!("Sending batch of {} items", batch.len());

                    on_batch(batch);
                }

                for flush in flushes {
                    debug!("flush completed");
                    let _ = flush.try_send(());
                }
            })
            .map_err(|e| e.to_string())?;

        Ok(Batcher { sender, flusher })
    }

    pub fn add(&self, item: T) -> Result<(), Error> {
        self.sender.send(item).map_err(|e| e.to_string())
    }

    pub fn flush(&self) -> Result<(), Error> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.flusher
            .send(sender)
            .map_err(|e| format!("failed to request flush: {}", e))?;
        receiver
            .recv()
            .map_err(|e| format!("failed to wait for flush: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use spectral::prelude::*;

    use super::*;
    use crate::eval::{Detail, Reason};
    use crate::store::{FeatureFlag, FlagValue};
    use crate::users::User;

    #[test]
    fn feature_event_emits_index_and_summary() {
        let events = Arc::new(RwLock::new(sink::MockSink::new()));
        let ep = EventProcessor::new_with_sink(events.clone())
            .expect("event processor should initialize");

        let flag = FeatureFlag::basic_flag("flag");
        let user =
            crate::events::MaybeInlinedUser::Inlined(User::with_key("foo".to_string()).build());
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough,
        };

        let feature_request = Event::new_feature_request(
            &flag.key.clone(),
            user.clone(),
            Some(flag.clone()),
            detail.clone(),
            FlagValue::from(false),
            true,
        );
        ep.send(feature_request.clone());

        ep.flush().expect("flush should not fail");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(3);

        // TODO test that it only does this under certain conditions (trackEvents = true, etc)
        asserting!("emits original feature event")
            .that(&*events)
            .matching_contains(|event| event == &feature_request);

        asserting!("emits index event")
            .that(&*events)
            .matching_contains(|event| match event {
                Event::Index { base: b } => b.user == user,
                _ => false,
            });

        asserting!("emits summary event")
            .that(&*events)
            .matching_contains(|event| match event {
                Event::Summary { features, .. } => {
                    let counter = &features[&flag.key].counters[0];
                    &counter.value == detail.value.as_ref().unwrap() && counter.count == 1
                }
                _ => false,
            });
    }
}
