use super::event_sink as sink;
use super::events::{Event, EventSummary, MaybeInlinedUser};

use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use lru::LruCache;
use reqwest as r;

type Error = String; // TODO

const FLUSH_POLL_INTERVAL: Duration = Duration::from_millis(100); // TODO make this configurable instead of hardcoding

pub struct EventProcessor {
    batcher: EventBatcher,
}

impl EventProcessor {
    pub fn new(base_url: r::Url, sdk_key: &str) -> Result<Self, Error> {
        let sink = sink::ReqwestSink::new(&base_url, sdk_key)?;
        Self::new_with_sink(Arc::new(RwLock::new(sink)))
    }

    pub fn new_with_sink(sink: Arc<RwLock<dyn sink::EventSink>>) -> Result<Self, Error> {
        // TODO don't hardcode batcher params
        let batcher = EventBatcher::start(sink, 1000, Duration::from_secs(30))?;
        Ok(EventProcessor { batcher })
    }

    pub fn send(&self, event: Event) -> Result<(), Error> {
        debug!("Sending event: {}", event);

        let index_result = match event.to_index_event() {
            Some(index) => self.batcher.add(Event::Index(index)),
            None => Ok(()),
        };
        // if failed to send index, still try to send event, but return an error

        self.batcher.add(event).and(index_result)
    }

    pub fn flush(&self) -> Result<(), Error> {
        self.batcher.flush()
    }
}

struct EventBatcher {
    sender: mpsc::SyncSender<Event>,
    flusher: mpsc::SyncSender<mpsc::SyncSender<()>>,
}

impl EventBatcher {
    pub fn start(
        sink: Arc<RwLock<dyn sink::EventSink>>,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::sync_channel(500); // TODO hardcode

        let (flusher, flush_receiver) = mpsc::sync_channel::<mpsc::SyncSender<()>>(10); // TODO hardcode

        let _worker_handle = thread::Builder::new()
            // TODO make this an object with methods, this is confusing
            .spawn(move || loop {
                debug!("waiting for a batch to send");

                let batch_deadline: Instant = Instant::now() + batch_timeout;

                let mut batch = Vec::new();
                let mut summary = EventSummary::new();
                let mut flushes = Vec::new();

                // Dedupe users per batch.
                // This restricts us to only deduping users within the batch_timeout.
                // TODO support configuring the deduping period separately (probably after redoing
                // this using crossbeam so we can select on multiple channels)
                let mut user_cache = LruCache::<String, ()>::new(1000); // TODO hardcode

                'event: loop {
                    // Rust doesn't have a (stable) select on multiple channels, so we have to
                    // contort a bit to support flush without blocking flush requesters until the
                    // batch deadline
                    // TODO redo this using https://crates.io/crates/crossbeam-channel
                    let mut batch_ready = match receiver.recv_timeout(FLUSH_POLL_INTERVAL) {
                        Ok(event) => {
                            process_event(event, &mut user_cache, &mut batch, &mut summary);

                            batch.len() > batch_size
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => Instant::now() >= batch_deadline,
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            debug!("batch sender disconnected");
                            // TODO terminate
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
                        while let Ok(event) = receiver.try_recv() {
                            if process_event(event, &mut user_cache, &mut batch, &mut summary) {
                                extra += 1;
                            }
                        }
                        if extra > 0 {
                            debug!("Sending {} extra events early due to flush", extra);
                        }
                    }

                    if !summary.is_empty() {
                        batch.push(Event::Summary(summary));
                    }

                    debug!("Sending batch of {} events", batch.len());

                    if let Err(e) = sink.write().unwrap().send(batch) {
                        warn!("failed to send event: {}", e);
                    }
                }

                for flush in flushes {
                    debug!("flush completed");
                    let _ = flush.try_send(());
                }
            })
            .map_err(|e| e.to_string())?;

        Ok(EventBatcher { sender, flusher })
    }

    pub fn add(&self, event: Event) -> Result<(), Error> {
        self.sender.send(event).map_err(|e| e.to_string())
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

fn process_event(
    event: Event,
    user_cache: &mut LruCache<String, ()>,
    batch: &mut Vec<Event>,
    summary: &mut EventSummary,
) -> bool {
    match event {
        Event::Index(index) => {
            if notice_user(user_cache, &index.user) {
                batch.push(Event::Index(index));
                true
            } else {
                false
            }
        }
        _ => {
            summary.add(&event);
            batch.push(event);
            true
        }
    }
}

fn notice_user(user_cache: &mut LruCache<String, ()>, user: &MaybeInlinedUser) -> bool {
    let key = match user.key() {
        Some(k) => k,
        None => {
            return true;
        }
    };

    if user_cache.get(key).is_none() {
        trace!("noticing new user {:?}", key);
        user_cache.put(key.clone(), ());
        true
    } else {
        trace!("ignoring already-seen user {:?}", key);
        false
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
        let user = MaybeInlinedUser::Inlined(User::with_key("foo".to_string()).build());
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
        ep.send(feature_request.clone())
            .expect("send should succeed");

        ep.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(3);

        // TODO test that it only does this under certain conditions (trackEvents = true, etc)
        asserting!("emits original feature event")
            .that(&*events)
            .matching_contains(|event| event == &feature_request);

        asserting!("emits index event")
            .that(&*events)
            .matching_contains(|event| event.kind() == "index");

        asserting!("emits summary event")
            .that(&*events)
            .matching_contains(|event| event.kind() == "summary");
    }

    #[test]
    fn feature_events_dedupe_index_events() {
        let events = Arc::new(RwLock::new(sink::MockSink::new()));
        let ep = EventProcessor::new_with_sink(events.clone())
            .expect("event processor should initialize");

        let flag = FeatureFlag::basic_flag("flag");
        let user = MaybeInlinedUser::Inlined(User::with_key("bar".to_string()).build());
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
        ep.send(feature_request.clone())
            .expect("send should succeed");
        ep.send(feature_request.clone())
            .expect("send should succeed");

        // ensure batcher processes both events before the flush, otherwise
        // it may (nondeterministically) be a failypants TODO
        //std::thread::sleep(2 * FLUSH_POLL_INTERVAL);

        ep.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(4);

        asserting!("emits feature event each time")
            .that(
                &events
                    .iter()
                    .filter(|event| event.kind() == "feature")
                    .count(),
            )
            .is_equal_to(2);

        asserting!("emits index event only once")
            .that(
                &events
                    .iter()
                    .filter(|event| event.kind() == "index")
                    .count(),
            )
            .is_equal_to(1);

        asserting!("emits summary event only once")
            .that(
                &events
                    .iter()
                    .filter(|event| event.kind() == "summary")
                    .count(),
            )
            .is_equal_to(1);
    }
}
