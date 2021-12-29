use crossbeam_channel::{bounded, select, tick, Sender};
use std::sync::{Arc, Once, RwLock};
use std::thread;
use std::time::{Duration, SystemTime};

use launchdarkly_server_sdk_evaluation::User;
use lru::LruCache;
use reqwest as r;
use thiserror::Error;

use crate::events::OutputEvent;

use super::event_sink as sink;
use super::events::{EventSummary, InputEvent};

enum EventDispatcherMessage {
    EventMessage(InputEvent),
    Flush,
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EventProcessorError {
    #[error(transparent)]
    DispatcherFailed(#[from] EventBatcherError),

    #[error("unable to build event processor: {0}")]
    BuildFailure(String),
}

/// Trait for the component that buffers analytics events and sends them to LaunchDarkly.
/// This component can be replaced for testing purposes.
pub trait EventProcessor: Send {
    fn send(&self, event: InputEvent);
    fn flush(&self);
}

pub struct NullEventProcessor {}

impl NullEventProcessor {
    pub fn new() -> Self {
        Self {}
    }
}

impl EventProcessor for NullEventProcessor {
    fn send(&self, _: InputEvent) {}
    fn flush(&self) {}
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
        user_keys_flush_interval: Duration,
        inline_users_in_events: bool,
    ) -> Result<Self, EventProcessorError> {
        let sink = sink::ReqwestSink::new(&base_url, sdk_key)
            .map_err(|e| EventProcessorError::BuildFailure(e.to_string()))?;
        Self::new_with_sink(
            Arc::new(RwLock::new(sink)),
            flush_interval,
            capacity,
            user_keys_capacity,
            user_keys_flush_interval,
            inline_users_in_events,
        )
    }

    pub fn new_with_sink(
        sink: Arc<RwLock<dyn sink::EventSink>>,
        flush_interval: Duration,
        capacity: usize,
        user_keys_capacity: usize,
        user_keys_flush_interval: Duration,
        inline_users_in_events: bool,
    ) -> Result<Self, EventProcessorError> {
        let batcher = EventBatcher::start(
            sink,
            capacity,
            user_keys_capacity,
            user_keys_flush_interval,
            inline_users_in_events,
            flush_interval,
        )?;
        Ok(EventProcessorImpl { batcher })
    }
}

impl EventProcessor for EventProcessorImpl {
    fn send(&self, event: InputEvent) {
        debug!("Sending event: {}", event);
        self.batcher.add(event);
    }

    fn flush(&self) {
        self.batcher.flush();
    }
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EventBatcherError {
    #[error("dispatcher failed to start")]
    StartFailure,
}

struct EventBatcher {
    sender: Sender<EventDispatcherMessage>,
    inbox_full_once: Once,
}

impl EventBatcher {
    pub fn start(
        sink: Arc<RwLock<dyn sink::EventSink>>,
        capacity: usize,
        user_keys_capacity: usize,
        user_keys_flush_interval: Duration,
        inline_users_in_events: bool,
        flush_interval: Duration,
    ) -> Result<Self, EventBatcherError> {
        let (sender, receiver) = bounded(capacity);

        let mut user_cache = LruCache::<String, ()>::new(user_keys_capacity);
        let reset_user_cache_ticker = tick(user_keys_flush_interval);
        let flush_ticker = tick(flush_interval);

        let _worker_handle = thread::Builder::new()
            // TODO(ch108616) make this an object with methods, this is confusing
            .spawn(move || loop {
                debug!("waiting for a batch to send");

                let mut flush_requested = false;
                let mut batch = Vec::new();
                let mut summary = EventSummary::new();

                loop {
                    select! {
                        recv(reset_user_cache_ticker) -> _ => user_cache.clear(),
                        recv(flush_ticker) -> _ => break,
                        recv(receiver) -> result => match result {
                            Ok(EventDispatcherMessage::EventMessage(event)) => {
                                process_event(
                                    event,
                                    &mut user_cache,
                                    &mut batch,
                                    &mut summary,
                                    inline_users_in_events,
                                    sink.read().unwrap().last_known_time(),
                                );
                            }
                            Ok(EventDispatcherMessage::Flush) => {
                                flush_requested = true;
                                break;
                            },
                            Err(_) => todo!(),
                        }
                    }
                }

                if flush_requested {
                    let mut extra = 0;
                    receiver
                        .try_iter()
                        .collect::<Vec<EventDispatcherMessage>>()
                        .into_iter()
                        .for_each(|message| {
                            if let EventDispatcherMessage::EventMessage(event) = message {
                                process_event(
                                    event,
                                    &mut user_cache,
                                    &mut batch,
                                    &mut summary,
                                    inline_users_in_events,
                                    sink.read().unwrap().last_known_time(),
                                );
                                extra += 1;
                            }
                        });

                    if extra > 0 {
                        debug!("Sending {} extra events early due to flush", extra);
                    }
                }

                if !summary.is_empty() {
                    batch.push(OutputEvent::Summary(summary));
                }

                debug!("Sending batch of {} events", batch.len());

                if !batch.is_empty() {
                    if let Err(e) = sink.write().unwrap().send(batch) {
                        warn!("failed to send event: {}", e);
                    }
                }
            })
            .map_err(|_| EventBatcherError::StartFailure)?;

        Ok(EventBatcher {
            sender,
            inbox_full_once: Once::new(),
        })
    }

    pub fn add(&self, event: InputEvent) {
        if self
            .sender
            .try_send(EventDispatcherMessage::EventMessage(event))
            .is_err()
        {
            self.inbox_full_once.call_once(|| {
                warn!("Events are being produced faster than they can be processed; some events will be dropped")
            });
        }
    }

    pub fn flush(&self) {
        let _ = self.sender.try_send(EventDispatcherMessage::Flush);
    }
}

fn process_event(
    event: InputEvent,
    user_cache: &mut LruCache<String, ()>,
    batch: &mut Vec<OutputEvent>,
    summary: &mut EventSummary,
    inline_users_in_events: bool,
    last_known_time: u128,
) {
    match event {
        InputEvent::FeatureRequest(fre) => {
            summary.add(&fre);

            if notice_user(user_cache, &fre.base.user) && !inline_users_in_events {
                batch.push(OutputEvent::Index(fre.to_index_event()));
            }

            let now = match SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                Ok(time) => time.as_millis(),
                _ => 0,
            };

            if let Some(debug_events_until_date) = fre.debug_events_until_date {
                let time = u128::from(debug_events_until_date);
                if time > now && time > last_known_time {
                    let event = fre.clone().into_inline();
                    batch.push(OutputEvent::Debug(event));
                }
            }

            if fre.track_events {
                let event = match inline_users_in_events {
                    true => fre.into_inline(),
                    false => fre,
                };

                batch.push(OutputEvent::FeatureRequest(event));
            }
        }
        InputEvent::Identify(identify) => {
            notice_user(user_cache, &identify.base.user);
            batch.push(OutputEvent::Identify(identify.into_inline()));
        }
        InputEvent::Alias(alias) => {
            batch.push(OutputEvent::Alias(alias));
        }
        InputEvent::Custom(custom) => {
            if notice_user(user_cache, &custom.base.user) && !inline_users_in_events {
                batch.push(OutputEvent::Index(custom.to_index_event()));
            }

            let event = match inline_users_in_events {
                true => custom.into_inline(),
                false => custom,
            };

            batch.push(OutputEvent::Custom(event));
        }
    }
}

fn notice_user(user_cache: &mut LruCache<String, ()>, user: &User) -> bool {
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
    use launchdarkly_server_sdk_evaluation::{Detail, Flag, FlagValue, Reason, User};
    use spectral::prelude::*;
    use std::thread::sleep;
    use test_case::test_case;

    use super::*;
    use crate::{events::EventFactory, test_common::basic_flag};

    fn make_test_sink(
        events: &Arc<RwLock<sink::MockSink>>,
        inline_users_in_events: bool,
    ) -> EventProcessorImpl {
        EventProcessorImpl::new_with_sink(
            events.clone(),
            Duration::from_millis(100),
            1000,
            1000,
            Duration::from_millis(100),
            inline_users_in_events,
        )
        .expect("event processor should initialize")
    }

    #[test_case(true, false, vec!["summary"])]
    #[test_case(false, false, vec!["index", "summary"])]
    #[test_case(true, true, vec!["feature", "summary"])]
    #[test_case(false, true, vec!["index", "feature", "summary"])]
    fn sending_feature_event_emits_correct_events(
        inline_users_in_events: bool,
        flag_track_events: bool,
        expected_event_types: Vec<&str>,
    ) {
        let (sink, sink_receiver) = sink::MockSink::new(0);
        let events = Arc::new(RwLock::new(sink));
        let ep = make_test_sink(&events, inline_users_in_events);

        let mut flag = basic_flag("flag");
        flag.track_events = flag_track_events;
        let user = User::with_key("foo".to_string()).build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let feature_request = event_factory.new_eval_event(
            &flag.key,
            user,
            &flag,
            detail.clone(),
            FlagValue::from(false),
            None,
        );
        ep.send(feature_request);
        ep.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(expected_event_types.len());

        for event_type in expected_event_types {
            assert_that(events).matching_contains(|event| event.kind() == event_type);
        }
    }

    #[test_case(0, 64_060_606_800_000, vec!["debug", "summary"])]
    #[test_case(64_060_606_800_000, 64_060_606_800_000, vec!["summary"])]
    #[test_case(64_060_606_800_001, 64_060_606_800_000, vec!["summary"])]
    fn sending_feature_event_emits_debug_event_correctly(
        last_known_time: u128,
        debug_events_until_date: u64,
        expected_event_types: Vec<&str>,
    ) {
        let (sink, sink_receiver) = sink::MockSink::new(last_known_time);
        let events = Arc::new(RwLock::new(sink));
        let ep = make_test_sink(&events, true);

        let mut flag = basic_flag("flag");
        flag.debug_events_until_date = Some(debug_events_until_date);
        let user = User::with_key("foo".to_string()).build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let feature_request = event_factory.new_eval_event(
            &flag.key,
            user,
            &flag,
            detail.clone(),
            FlagValue::from(false),
            None,
        );
        ep.send(feature_request);
        ep.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;
        assert_that!(*events).has_length(expected_event_types.len());

        for event_type in expected_event_types {
            assert_that(events).matching_contains(|event| event.kind() == event_type);
        }
    }

    #[test]
    fn sending_feature_event_with_rule_track_events_emits_feature_and_summary() {
        let (sink, sink_receiver) = sink::MockSink::new(0);
        let events = Arc::new(RwLock::new(sink));
        let ep = make_test_sink(&events, true);

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

        let event_factory = EventFactory::new(true);
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
            ep.send(fre.clone());
        }

        ep.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;

        // detail_track_rule -> feature, detail_fallthrough -> feature, 1 summary
        assert_that!(*events).has_length(1 + 1 + 1);

        asserting!("emits feature events for rules that have track events enabled")
            .that(
                &events
                    .iter()
                    .filter(|event| event.kind() == "feature")
                    .count(),
            )
            .is_equal_to(2);

        asserting!("emits summary event")
            .that(events)
            .matching_contains(|event| event.kind() == "summary");
    }

    #[test]
    fn feature_events_dedupe_index_events() {
        let (sink, sink_receiver) = sink::MockSink::new(0);
        let events = Arc::new(RwLock::new(sink));
        let ep = make_test_sink(&events, false);

        let flag = basic_flag("flag");
        let user = User::with_key("bar".to_string()).build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let feature_request = event_factory.new_eval_event(
            &flag.key,
            user,
            &flag,
            detail.clone(),
            FlagValue::from(false),
            None,
        );
        ep.send(feature_request.clone());
        ep.send(feature_request);
        ep.flush();

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &events.read().unwrap().events;

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

    #[test]
    fn flush_interval_does_not_include_pending_events() {
        let user = User::with_key("user".to_string()).build();
        let previous = User::with_key("previous".to_string()).build();
        let event_factory = EventFactory::new(true);
        let alias_event = event_factory.new_alias(user, previous);

        let (sink, sink_receiver) = sink::MockSink::new(0);
        let sink = Arc::new(RwLock::new(sink));

        let batcher = EventBatcher::start(
            sink.clone(),
            10,
            10,
            Duration::from_secs(100),
            false,
            Duration::from_millis(100),
        )
        .expect("Batcher failed to start");

        batcher.add(alias_event.clone());

        // Wait long enough for the flush interval to kick off
        sleep(Duration::from_millis(110));

        // These events shouldn't be included in the sink yet
        batcher.add(alias_event.clone());
        batcher.add(alias_event.clone());
        batcher.add(alias_event);

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &sink.read().unwrap().events;

        assert_that!(*events).has_length(1);
    }

    #[test]
    fn batcher_flushes_periodically() {
        let user = User::with_key("user".to_string()).build();
        let previous = User::with_key("previous".to_string()).build();
        let event_factory = EventFactory::new(true);
        let alias_event = event_factory.new_alias(user, previous);

        let (sink, sink_receiver) = sink::MockSink::new(0);
        let sink = Arc::new(RwLock::new(sink));

        let batcher = EventBatcher::start(
            sink.clone(),
            10,
            10,
            Duration::from_secs(100),
            false,
            Duration::from_millis(100),
        )
        .expect("Batcher failed to start");

        batcher.add(alias_event.clone());
        assert!(sink_receiver.try_recv().is_err());

        let _ = sink_receiver.recv_timeout(Duration::from_secs(1));
        let events = &sink.read().unwrap().events;

        assert_that!(*events).has_length(1);
    }
}
