use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use lru::LruCache;
use reqwest as r;

use super::event_sink as sink;
use super::events::{Event, EventSummary, MaybeInlinedUser};

type Error = String; // TODO(ch108607) use an error enum

pub trait EventProcessor: Send {
    fn send(&self, event: Event) -> Result<(), Error>;
    fn flush(&self) -> Result<(), Error>;
}

pub struct EventProcessorImpl {
    batcher: EventBatcher,
}

impl EventProcessorImpl {
    pub fn new(
        base_url: r::Url,
        sdk_key: &str,
        flush_interval: Duration,
        capacity: usize,
        user_keys_capacity: usize,
    ) -> Result<Self, Error> {
        let sink = sink::ReqwestSink::new(&base_url, sdk_key)?;
        Self::new_with_sink(
            Arc::new(RwLock::new(sink)),
            flush_interval,
            capacity,
            user_keys_capacity,
        )
    }

    pub fn new_with_sink(
        sink: Arc<RwLock<dyn sink::EventSink>>,
        flush_interval: Duration,
        capacity: usize,
        user_keys_capacity: usize,
    ) -> Result<Self, Error> {
        // TODO(ch108609) make batcher params configurable
        let batcher = EventBatcher::start(
            sink,
            capacity,
            user_keys_capacity,
            1000,
            Duration::from_secs(30),
            flush_interval,
        )?;
        Ok(EventProcessorImpl { batcher })
    }
}

impl EventProcessor for EventProcessorImpl {
    fn send(&self, event: Event) -> Result<(), Error> {
        debug!("Sending event: {}", event);

        let index_result = match event.to_index_event() {
            Some(index) => self.batcher.add(Event::Index(index)),
            None => Ok(()),
        };
        // if failed to send index, still try to send event, but return an error

        self.batcher.add(event).and(index_result)
    }

    fn flush(&self) -> Result<(), Error> {
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
        capacity: usize,
        user_keys_capacity: usize,
        batch_size: usize,
        batch_timeout: Duration,
        flush_interval: Duration,
    ) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::sync_channel(capacity);

        let (flusher, flush_receiver) = mpsc::sync_channel::<mpsc::SyncSender<()>>(10); // TODO(ch108609) make flush buffer size configurable

        let _worker_handle = thread::Builder::new()
            // TODO(ch108616) make this an object with methods, this is confusing
            .spawn(move || loop {
                debug!("waiting for a batch to send");

                let batch_deadline: Instant = Instant::now() + batch_timeout;

                let mut batch = Vec::new();
                let mut summary = EventSummary::new();
                let mut flushes = Vec::new();

                // Dedupe users per batch.
                // This restricts us to only deduping users within the batch_timeout.
                // TODO(ch108616) support configuring the deduping period separately (probably after redoing
                // this using crossbeam so we can select on multiple channels)
                let mut user_cache = LruCache::<String, ()>::new(user_keys_capacity);

                'event: loop {
                    // This was written without access to a (stable) select on multiple channels, so we have to
                    // contort a bit to support flush without blocking flush requesters until the
                    // batch deadline.
                    // TODO(ch108616) redo this using crossbeam-channel, or futures::channel and futures::select
                    let mut batch_ready = match receiver.recv_timeout(flush_interval) {
                        Ok(event) => {
                            process_event(event, &mut user_cache, &mut batch, &mut summary);

                            batch.len() > batch_size
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => Instant::now() >= batch_deadline,
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            debug!("batch sender disconnected");
                            // TODO(ch108616) terminate this thread
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
        Event::FeatureRequest(fre) => {
            summary.add(&fre);

            if fre.track_events {
                batch.push(Event::FeatureRequest(fre));
                true
            } else {
                false
            }
        }
        _ => {
            batch.push(event);
            true
        }
    }
}

fn notice_user(user_cache: &mut LruCache<String, ()>, user: &MaybeInlinedUser) -> bool {
    let key = user.key().to_string();

    if user_cache.get(&key).is_none() {
        trace!("noticing new user {:?}", key);
        user_cache.put(key, ());
        true
    } else {
        trace!("ignoring already-seen user {:?}", key);
        false
    }
}

#[cfg(test)]
mod tests {
    use rust_server_sdk_evaluation::{Detail, Flag, FlagValue, Reason, User};
    use spectral::prelude::*;

    use super::*;
    use crate::{events::EventFactory, test_common::basic_flag};

    fn make_test_sink(events: &Arc<RwLock<sink::MockSink>>) -> EventProcessorImpl {
        EventProcessorImpl::new_with_sink(events.clone(), Duration::from_millis(100), 1000, 1000)
            .expect("event processor should initialize")
    }

    #[test]
    fn sending_feature_event_emits_only_index_and_summary() {
        let events = Arc::new(RwLock::new(sink::MockSink::new()));
        let ep = make_test_sink(&events);

        let flag = basic_flag("flag");
        let user = User::with_key("foo".to_string()).build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true, true);
        let feature_request = event_factory.new_eval_event(
            &flag.key,
            user,
            &flag,
            detail.clone(),
            FlagValue::from(false),
            None,
        );
        ep.send(feature_request.clone())
            .expect("send should succeed");

        ep.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(2);

        asserting!("emits index event")
            .that(&*events)
            .matching_contains(|event| event.kind() == "index");

        asserting!("emits summary event")
            .that(&*events)
            .matching_contains(|event| event.kind() == "summary");
    }

    #[test]
    fn sending_feature_event_with_track_events_emits_index_and_summary() {
        let events = Arc::new(RwLock::new(sink::MockSink::new()));
        let ep = make_test_sink(&events);

        let mut flag = basic_flag("flag");
        flag.track_events = true;
        let user = User::with_key("foo".to_string()).build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true, true);
        let feature_request = event_factory.new_eval_event(
            &flag.key,
            user,
            &flag,
            detail.clone(),
            FlagValue::from(false),
            None,
        );
        ep.send(feature_request.clone())
            .expect("send should succeed");

        ep.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(3);

        asserting!("emits original feature event")
            .that(&*events)
            .contains(&feature_request);

        asserting!("emits index event")
            .that(&*events)
            .matching_contains(|event| event.kind() == "index");

        asserting!("emits summary event")
            .that(&*events)
            .matching_contains(|event| event.kind() == "summary");
    }

    #[test]
    fn sending_feature_event_with_rule_track_events_emits_index_and_summary() {
        let events = Arc::new(RwLock::new(sink::MockSink::new()));
        let ep = make_test_sink(&events);

        let flag: Flag = serde_json::from_str(
            r#"{
                 "key": "with_rule",
                 "on": true,
                 "targets": [],
                 "prerequisites": [],
                 "rules": [
                   {
                     "id": "rule-0",
                     "clauses": [{
                       "attribute": "key",
                       "negate": false,
                       "op": "matches",
                       "values": ["do-track"]
                     }],
                     "trackEvents": true,
                     "variation": 1
                   },
                   {
                     "id": "rule-1",
                     "clauses": [{
                       "attribute": "key",
                       "negate": false,
                       "op": "matches",
                       "values": ["no-track"]
                     }],
                     "trackEvents": false,
                     "variation": 1
                   }
                 ],
                 "fallthrough": {"variation": 0},
                 "trackEventsFallthrough": true,
                 "offVariation": 0,
                 "clientSideAvailability": {
                   "usingMobileKey": false,
                   "usingEnvironmentId": false
                 },
                 "salt": "kosher",
                 "version": 2,
                 "variations": [false, true]
               }"#,
        )
        .expect("flag should parse");

        let user_track_rule = User::with_key("do-track".to_string()).build();
        let user_notrack_rule = User::with_key("no-track".to_string()).build();
        let user_fallthrough = User::with_key("foo".to_string()).build();

        let detail_track_rule = Detail {
            value: Some(FlagValue::from(true)),
            variation_index: Some(1),
            reason: Reason::RuleMatch {
                rule_index: 0,
                rule_id: "rule-0".into(),
                in_experiment: false,
            },
        };
        let detail_notrack_rule = Detail {
            value: Some(FlagValue::from(true)),
            variation_index: Some(1),
            reason: Reason::RuleMatch {
                rule_index: 1,
                rule_id: "rule-1".into(),
                in_experiment: false,
            },
        };
        let detail_fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(0),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true, true);
        let fre_track_rule = event_factory.new_eval_event(
            &flag.key,
            user_track_rule,
            &flag,
            detail_track_rule,
            FlagValue::from(false),
            None,
        );
        let fre_notrack_rule = event_factory.new_eval_event(
            &flag.key,
            user_notrack_rule,
            &flag,
            detail_notrack_rule,
            FlagValue::from(false),
            None,
        );
        let fre_fallthrough = event_factory.new_eval_event(
            &flag.key,
            user_fallthrough,
            &flag,
            detail_fallthrough,
            FlagValue::from(false),
            None,
        );

        for fre in vec![&fre_track_rule, &fre_notrack_rule, &fre_fallthrough] {
            ep.send(fre.clone()).expect("send should succeed");
        }

        ep.flush().expect("flush should succeed");

        let events = events.read().unwrap();

        assert_that!(*events).has_length(2 + 3 + 1);

        asserting!("emits tracked rule feature event")
            .that(&*events)
            .contains(&fre_track_rule);
        asserting!("does not emit untracked rule feature event")
            .that(&*events)
            .does_not_contain(&fre_notrack_rule);
        asserting!("emits fallthrough feature event (with trackEventsFallthrough)")
            .that(&*events)
            .contains(&fre_fallthrough);

        asserting!("emits an index event for each")
            .that(
                &events
                    .iter()
                    .filter(|event| event.kind() == "index")
                    .count(),
            )
            .is_equal_to(3);

        asserting!("emits summary event")
            .that(&*events)
            .matching_contains(|event| event.kind() == "summary");
    }

    #[test]
    fn feature_events_dedupe_index_events() {
        let events = Arc::new(RwLock::new(sink::MockSink::new()));
        let ep = make_test_sink(&events);

        let flag = basic_flag("flag");
        let user = User::with_key("bar".to_string()).build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true, true);
        let feature_request = event_factory.new_eval_event(
            &flag.key,
            user,
            &flag,
            detail.clone(),
            FlagValue::from(false),
            None,
        );
        ep.send(feature_request.clone())
            .expect("send should succeed");
        ep.send(feature_request.clone())
            .expect("send should succeed");

        ep.flush().expect("flush should succeed");

        let events = events.read().unwrap();
        assert_that!(*events).has_length(2);

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
