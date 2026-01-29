use crossbeam_channel::{bounded, RecvTimeoutError, Sender};
use std::sync::Once;
use std::thread;
use std::time::Duration;
use thiserror::Error;

use super::dispatcher::{EventDispatcher, EventDispatcherMessage};
use super::event::InputEvent;
use super::EventsConfiguration;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum EventProcessorError {
    #[error(transparent)]
    SpawnFailed(#[from] std::io::Error),
}

/// Trait for the component that buffers analytics events and sends them to LaunchDarkly.
/// This component can be replaced for testing purposes.
pub trait EventProcessor: Send + Sync {
    /// Records an InputEvent asynchronously. Depending on the feature flag properties and event
    /// properties, this may be transmitted to the events service as an individual event, or may
    /// only be added into summary data.
    fn send(&self, event: InputEvent);

    /// Specifies that any buffered events should be sent as soon as possible, rather than waiting
    /// for the next flush interval. This method is asynchronous, so events still may not be sent
    /// until a later time.
    fn flush(&self);

    /// Shuts down all event processor activity, after first ensuring that all events have been
    /// delivered. Subsequent calls to [EventProcessor::send] or [EventProcessor::flush] will be
    /// ignored.
    fn close(&self);

    /// Tells the event processor that all pending analytics events should be delivered as soon as
    /// possible, and blocks until delivery is complete or the timeout expires.
    ///
    /// This method triggers a flush of events currently in the outbox and waits for that specific
    /// flush to complete. Note that if periodic flushes or other flush operations are in-flight
    /// when this is called, those may still be completing after this method returns.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum time to wait for flush to complete. Use `Duration::ZERO` to wait indefinitely.
    ///
    /// # Returns
    ///
    /// Returns `true` if flush completed successfully, `false` if timeout occurred or the event
    /// processor has been shut down.
    fn flush_blocking(&self, timeout: std::time::Duration) -> bool;
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
    fn close(&self) {}
    fn flush_blocking(&self, _timeout: std::time::Duration) -> bool {
        true
    }
}

pub struct EventProcessorImpl {
    inbox_tx: Sender<EventDispatcherMessage>,
    inbox_full_once: Once,
}

impl EventProcessorImpl {
    pub fn new(events_configuration: EventsConfiguration) -> Result<Self, EventProcessorError> {
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);
        let dispatch_start = move || {
            let mut dispatcher = EventDispatcher::new(events_configuration);
            dispatcher.start(inbox_rx)
        };

        match thread::Builder::new().spawn(dispatch_start) {
            Ok(_) => Ok(Self {
                inbox_tx,
                inbox_full_once: Once::new(),
            }),
            Err(e) => Err(EventProcessorError::SpawnFailed(e)),
        }
    }
}

impl EventProcessor for EventProcessorImpl {
    fn send(&self, event: InputEvent) {
        if self
            .inbox_tx
            .try_send(EventDispatcherMessage::EventMessage(event))
            .is_err()
        {
            self.inbox_full_once.call_once(|| {
                warn!("Events are being produced faster than they can be processed; some events will be dropped")
            });
        }
    }

    fn flush(&self) {
        let _ = self.inbox_tx.try_send(EventDispatcherMessage::Flush);
    }

    fn close(&self) {
        let (sender, receiver) = bounded::<()>(1);

        if self.inbox_tx.send(EventDispatcherMessage::Flush).is_err() {
            error!("Failed to send final flush message. Cannot stop event processor");
            return;
        }

        if self
            .inbox_tx
            .send(EventDispatcherMessage::Close(sender))
            .is_err()
        {
            error!("Failed to send close message. Cannot stop event processor");
            return;
        }

        let _ = receiver.recv();
    }

    fn flush_blocking(&self, timeout: Duration) -> bool {
        let (sender, receiver) = bounded::<()>(1);

        if self
            .inbox_tx
            .send(EventDispatcherMessage::FlushWithReply(sender))
            .is_err()
        {
            error!("Failed to send flush_blocking message");
            return false;
        }

        if timeout == Duration::ZERO {
            // Wait indefinitely
            match receiver.recv() {
                Ok(()) => true,
                Err(_) => {
                    error!("flush_blocking failed: event processor shut down");
                    false
                }
            }
        } else {
            // Wait with timeout
            match receiver.recv_timeout(timeout) {
                Ok(()) => true,
                Err(RecvTimeoutError::Timeout) => {
                    warn!("flush_blocking timed out after {timeout:?}");
                    false
                }
                Err(RecvTimeoutError::Disconnected) => {
                    error!("flush_blocking failed: event processor shut down");
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use launchdarkly_server_sdk_evaluation::{ContextBuilder, Detail, Flag, FlagValue, Reason};
    use test_case::test_case;

    use crate::{
        events::{
            create_event_sender, create_events_configuration,
            event::{EventFactory, OutputEvent},
        },
        test_common::basic_flag,
    };

    use super::*;

    // Helper to create a failing event sender for testing
    struct FailingEventSender {
        should_shutdown: bool,
    }

    impl FailingEventSender {
        fn new(should_shutdown: bool) -> Self {
            Self { should_shutdown }
        }
    }

    impl crate::events::sender::EventSender for FailingEventSender {
        fn send_event_data(
            &self,
            _events: Vec<OutputEvent>,
            result_tx: crossbeam_channel::Sender<crate::events::sender::EventSenderResult>,
            flush_signal: Option<crossbeam_channel::Sender<()>>,
        ) -> futures::future::BoxFuture<'static, ()> {
            let should_shutdown = self.should_shutdown;
            Box::pin(async move {
                // Simulate a failed HTTP send
                let _ = result_tx.send(crate::events::sender::EventSenderResult {
                    time_from_server: 0,
                    success: false,
                    must_shutdown: should_shutdown,
                    flush_signal,
                });
            })
        }
    }

    #[test]
    fn calling_close_on_processor_twice_returns() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");
        event_processor.close();
        event_processor.close();
    }

    #[test_case(true, vec!["index", "feature", "summary"])]
    #[test_case(false, vec!["index", "summary"])]
    fn sending_feature_event_emits_correct_events(
        flag_track_events: bool,
        expected_event_types: Vec<&str>,
    ) {
        let mut flag = basic_flag("flag");
        flag.track_events = flag_track_events;
        let context = ContextBuilder::new("foo")
            .build()
            .expect("Failed to create context");
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
            context,
            &flag,
            detail,
            FlagValue::from(false),
            None,
        );

        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");
        event_processor.send(feature_request);
        event_processor.flush();
        event_processor.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(expected_event_types.len(), events.len());

        for event_type in expected_event_types {
            assert!(events.iter().any(|e| e.kind() == event_type));
        }
    }

    #[test]
    fn sending_feature_event_with_rule_track_events_emits_feature_and_summary() {
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

        let context_track_rule = ContextBuilder::new("do-track")
            .build()
            .expect("Failed to create context");
        let context_notrack_rule = ContextBuilder::new("no-track")
            .build()
            .expect("Failed to create context");
        let context_fallthrough = ContextBuilder::new("foo")
            .build()
            .expect("Failed to create context");

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
            context_track_rule,
            &flag,
            detail_track_rule,
            FlagValue::from(false),
            None,
        );
        let fre_notrack_rule = event_factory.new_eval_event(
            &flag.key,
            context_notrack_rule,
            &flag,
            detail_notrack_rule,
            FlagValue::from(false),
            None,
        );
        let fre_fallthrough = event_factory.new_eval_event(
            &flag.key,
            context_fallthrough,
            &flag,
            detail_fallthrough,
            FlagValue::from(false),
            None,
        );

        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        for fre in [fre_track_rule, fre_notrack_rule, fre_fallthrough] {
            event_processor.send(fre);
        }

        event_processor.flush();
        event_processor.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();

        // detail_track_rule -> feature + index, detail_notrack_rule -> index, detail_fallthrough -> feature + index, 1 summary
        assert_eq!(events.len(), 2 + 1 + 2 + 1);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind() == "feature")
                .count(),
            2
        );
        assert!(events.iter().any(|e| e.kind() == "index"));
        assert!(events.iter().any(|e| e.kind() == "summary"));
    }

    #[test]
    fn feature_events_dedupe_index_events() {
        let flag = basic_flag("flag");
        let context = ContextBuilder::new("bar")
            .build()
            .expect("Failed to create context");
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
            context,
            &flag,
            detail,
            FlagValue::from(false),
            None,
        );

        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");
        event_processor.send(feature_request.clone());
        event_processor.send(feature_request);
        event_processor.flush();
        event_processor.close();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();

        assert_eq!(events.len(), 2);

        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind() == "index")
                .count(),
            1
        );

        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind() == "summary")
                .count(),
            1
        );
    }

    #[test]
    fn flush_blocking_completes_successfully() {
        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        let context = ContextBuilder::new("foo")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        event_processor.send(event_factory.new_identify(context));

        let result = event_processor.flush_blocking(Duration::from_secs(5));
        assert!(result, "flush_blocking should complete successfully");

        event_processor.close();
        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn flush_blocking_with_very_short_timeout() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        let event_factory = EventFactory::new(true);

        // Send many events to increase the chance of timeout
        for i in 0..100 {
            let ctx = ContextBuilder::new(format!("user-{i}"))
                .build()
                .expect("Failed to create context");
            event_processor.send(event_factory.new_identify(ctx));
        }

        // Very short timeout may or may not complete - just verify it doesn't panic
        let _result = event_processor.flush_blocking(Duration::from_nanos(1));

        event_processor.close();
    }

    #[test]
    fn flush_blocking_with_zero_timeout_waits() {
        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        let context = ContextBuilder::new("foo")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        event_processor.send(event_factory.new_identify(context));

        let result = event_processor.flush_blocking(Duration::ZERO);
        assert!(
            result,
            "flush_blocking with zero timeout should complete successfully"
        );

        event_processor.close();
        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn flush_blocking_with_no_events_completes_immediately() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        let result = event_processor.flush_blocking(Duration::from_secs(1));
        assert!(
            result,
            "flush_blocking with no events should complete immediately"
        );

        event_processor.close();
    }

    #[test]
    fn null_processor_flush_blocking_returns_true() {
        let processor = NullEventProcessor::new();
        assert!(processor.flush_blocking(Duration::from_secs(1)));
        assert!(processor.flush_blocking(Duration::ZERO));
    }

    #[test]
    fn flush_blocking_fails_when_processor_closed() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        event_processor.close();

        let result = event_processor.flush_blocking(Duration::from_secs(1));
        assert!(
            !result,
            "flush_blocking should fail when processor is closed"
        );
    }

    #[test]
    fn flush_blocking_completes_on_recoverable_http_failure() {
        use std::collections::HashSet;
        use std::num::NonZeroUsize;
        use std::sync::Arc;

        let event_sender = FailingEventSender::new(false);
        let events_configuration = crate::events::EventsConfiguration {
            capacity: 5,
            event_sender: Arc::new(event_sender),
            flush_interval: Duration::from_secs(100),
            context_keys_capacity: NonZeroUsize::new(5).expect("5 > 0"),
            context_keys_flush_interval: Duration::from_secs(100),
            all_attributes_private: false,
            private_attributes: HashSet::new(),
            omit_anonymous_contexts: false,
        };
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        let context = ContextBuilder::new("foo")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        event_processor.send(event_factory.new_identify(context));

        // Even though HTTP fails, flush_blocking should complete (not hang)
        let result = event_processor.flush_blocking(Duration::from_secs(5));
        assert!(
            result,
            "flush_blocking should complete even when HTTP send fails (recoverable)"
        );

        event_processor.close();
    }

    #[test]
    fn flush_blocking_completes_on_unrecoverable_http_failure() {
        use std::collections::HashSet;
        use std::num::NonZeroUsize;
        use std::sync::Arc;

        let event_sender = FailingEventSender::new(true);
        let events_configuration = crate::events::EventsConfiguration {
            capacity: 5,
            event_sender: Arc::new(event_sender),
            flush_interval: Duration::from_secs(100),
            context_keys_capacity: NonZeroUsize::new(5).expect("5 > 0"),
            context_keys_flush_interval: Duration::from_secs(100),
            all_attributes_private: false,
            private_attributes: HashSet::new(),
            omit_anonymous_contexts: false,
        };
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        let context = ContextBuilder::new("foo")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        event_processor.send(event_factory.new_identify(context));

        // Even with must_shutdown=true, flush_blocking should complete (not hang)
        let result = event_processor.flush_blocking(Duration::from_secs(5));
        assert!(
            result,
            "flush_blocking should complete even when HTTP send fails (unrecoverable)"
        );

        event_processor.close();
    }

    #[test]
    fn flush_blocking_with_multiple_events_and_http_failures() {
        use std::collections::HashSet;
        use std::num::NonZeroUsize;
        use std::sync::Arc;

        let event_sender = FailingEventSender::new(false);
        let events_configuration = crate::events::EventsConfiguration {
            capacity: 5,
            event_sender: Arc::new(event_sender),
            flush_interval: Duration::from_secs(100),
            context_keys_capacity: NonZeroUsize::new(5).expect("5 > 0"),
            context_keys_flush_interval: Duration::from_secs(100),
            all_attributes_private: false,
            private_attributes: HashSet::new(),
            omit_anonymous_contexts: false,
        };
        let event_processor =
            EventProcessorImpl::new(events_configuration).expect("failed to start ep");

        let event_factory = EventFactory::new(true);

        // Send multiple events
        for i in 0..10 {
            let ctx = ContextBuilder::new(format!("user-{i}"))
                .build()
                .expect("Failed to create context");
            event_processor.send(event_factory.new_identify(ctx));
        }

        // flush_blocking should complete even with multiple events and HTTP failures
        let result = event_processor.flush_blocking(Duration::from_secs(5));
        assert!(
            result,
            "flush_blocking should complete with multiple events despite HTTP failures"
        );

        event_processor.close();
    }
}
