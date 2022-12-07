use crossbeam_channel::{bounded, Sender};
use std::sync::Once;
use std::thread;
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
}

pub struct EventProcessorImpl {
    inbox_tx: Sender<EventDispatcherMessage>,
    inbox_full_once: Once,
}

impl EventProcessorImpl {
    pub fn new(events_configuration: EventsConfiguration) -> Result<Self, EventProcessorError> {
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);
        let mut dispatcher = EventDispatcher::new(events_configuration);

        match thread::Builder::new().spawn(move || dispatcher.start(inbox_rx)) {
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
}
