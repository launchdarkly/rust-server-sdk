use crossbeam_channel::{bounded, select, tick, Receiver, Sender};
use std::collections::HashSet;
use std::iter::FromIterator;
use std::time::SystemTime;
use threadpool::ThreadPool;

use launchdarkly_server_sdk_evaluation::User;
use lru::LruCache;

use super::event::FeatureRequestEvent;
use super::sender::EventSenderResult;
use super::{
    event::{EventSummary, InputEvent, OutputEvent},
    EventsConfiguration,
};

struct Outbox {
    events: Vec<OutputEvent>,
    summary: EventSummary,
    capacity_exceeded: bool,
    capacity: usize,
}

impl Outbox {
    fn new(capacity: usize) -> Self {
        Self {
            events: Vec::with_capacity(capacity),
            summary: EventSummary::new(),
            capacity_exceeded: false,
            capacity,
        }
    }

    fn add_event(&mut self, output_event: OutputEvent) {
        if self.events.len() == self.capacity {
            if !self.capacity_exceeded {
                self.capacity_exceeded = true;
                warn!("Exceeded event queue capacity. Increase capacity to avoid dropping events.");
            }
            return;
        }

        self.capacity_exceeded = false;
        self.events.push(output_event);
    }

    fn add_to_summary(&mut self, event: &FeatureRequestEvent) {
        self.summary.add(event);
    }

    fn get_payload(&mut self) -> Vec<OutputEvent> {
        let mut payload = Vec::with_capacity(self.capacity + 1);
        payload.append(&mut self.events);

        if !self.summary.is_empty() {
            payload.push(OutputEvent::Summary(self.summary.clone()));
            self.summary.reset();
        }

        payload
    }

    fn is_empty(&self) -> bool {
        self.events.is_empty() && self.summary.is_empty()
    }

    fn reset(&mut self) {
        self.events.clear();
        self.summary.reset();
    }
}

pub(super) struct EventDispatcher {
    flush_pool: ThreadPool,
    outbox: Outbox,
    user_keys: LruCache<String, ()>,
    events_configuration: EventsConfiguration,
    last_known_time: u128,
    disabled: bool,
}

impl EventDispatcher {
    pub(super) fn new(events_configuration: EventsConfiguration) -> Self {
        Self {
            flush_pool: ThreadPool::new(5),
            outbox: Outbox::new(events_configuration.capacity),
            user_keys: LruCache::<String, ()>::new(events_configuration.user_keys_capacity),
            events_configuration,
            last_known_time: 0,
            disabled: false,
        }
    }

    pub(super) fn start(&mut self, inbox_rx: Receiver<EventDispatcherMessage>) {
        let reset_user_cache_ticker = tick(self.events_configuration.user_keys_flush_interval);
        let flush_ticker = tick(self.events_configuration.flush_interval);
        let (event_result_tx, event_result_rx) =
            bounded::<EventSenderResult>(self.flush_pool.max_count());

        loop {
            debug!("waiting for a batch to send");

            loop {
                select! {
                    recv(event_result_rx) -> result => match result {
                        Ok(result) if result.success => self.last_known_time = std::cmp::max(result.time_from_server, self.last_known_time),
                        Ok(result) if result.must_shutdown => {
                            self.disabled = true;
                            self.outbox.reset();
                        },
                        Ok(_) => continue,
                        Err(e) => {
                            error!("event_result_rx is disconnected. Shutting down dispatcher: {}", e);
                            return;
                        }
                    },
                    recv(reset_user_cache_ticker) -> _ => self.user_keys.clear(),
                    recv(flush_ticker) -> _ => break,
                    recv(inbox_rx) -> result => match result {
                        Ok(EventDispatcherMessage::Flush) => break,
                        Ok(EventDispatcherMessage::EventMessage(event)) => {
                            if !self.disabled {
                                self.process_event(event);
                            }
                        }
                        Ok(EventDispatcherMessage::Close(sender)) => {
                            self.flush_pool.join();

                            // We call drop here to make sure this receiver is completely
                            // disconnected. This ensures the event processor cannot send another
                            // message. We could rely on Rust to drop this during the normal course
                            // of operation, but there is a small chance for a deadlock issue if we
                            // call EventProcessor::close twice in rapid succession.
                            drop(inbox_rx);

                            let _ = sender.send(());

                            return;
                        }
                        Err(e) => {
                            error!("inbox_rx is disconnected. Shutting down dispatcher: {}", e);
                            return;
                        }
                    }
                }
            }

            if self.disabled {
                continue;
            }

            if !self.outbox.is_empty()
                && self.flush_pool.max_count() != self.flush_pool.active_count()
            {
                let payload = self.outbox.get_payload();

                debug!("Sending batch of {} events", payload.len());

                let sender = self.events_configuration.event_sender.clone();
                let results = event_result_tx.clone();
                self.flush_pool.execute(move || {
                    let _ = sender.send_event_data(payload, results);
                });
            }
        }
    }

    fn process_event(&mut self, event: InputEvent) {
        match event {
            InputEvent::FeatureRequest(fre) => {
                self.outbox.add_to_summary(&fre);

                let first_time_seeing_user = self.notice_user(&fre.base.user);
                let will_send_full_user_details_in_event =
                    fre.track_events && self.events_configuration.inline_users_in_events;
                if !will_send_full_user_details_in_event && first_time_seeing_user {
                    self.outbox.add_event(OutputEvent::Index(fre.to_index_event(
                        self.events_configuration.all_attributes_private,
                        self.events_configuration.private_attributes.clone(),
                    )));
                }

                let now = match SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                    Ok(time) => time.as_millis(),
                    _ => 0,
                };

                if let Some(debug_events_until_date) = fre.debug_events_until_date {
                    let time = u128::from(debug_events_until_date);
                    if time > now && time > self.last_known_time {
                        let event = fre.clone().into_inline(
                            self.events_configuration.all_attributes_private,
                            self.events_configuration.private_attributes.clone(),
                        );
                        self.outbox.add_event(OutputEvent::Debug(event));
                    }
                }

                if fre.track_events {
                    let event = match self.events_configuration.inline_users_in_events {
                        true => fre.into_inline(
                            self.events_configuration.all_attributes_private,
                            self.events_configuration.private_attributes.clone(),
                        ),
                        false => fre,
                    };

                    self.outbox.add_event(OutputEvent::FeatureRequest(event));
                }
            }
            InputEvent::Identify(identify) => {
                self.notice_user(&identify.base.user);
                self.outbox
                    .add_event(OutputEvent::Identify(identify.into_inline(
                        self.events_configuration.all_attributes_private,
                        HashSet::from_iter(
                            self.events_configuration.private_attributes.iter().cloned(),
                        ),
                    )));
            }
            InputEvent::Alias(alias) => {
                self.outbox.add_event(OutputEvent::Alias(alias));
            }
            InputEvent::Custom(custom) => {
                if self.notice_user(&custom.base.user)
                    && !self.events_configuration.inline_users_in_events
                {
                    self.outbox
                        .add_event(OutputEvent::Index(custom.to_index_event(
                            self.events_configuration.all_attributes_private,
                            self.events_configuration.private_attributes.clone(),
                        )));
                }

                let event = match self.events_configuration.inline_users_in_events {
                    true => custom.into_inline(
                        self.events_configuration.all_attributes_private,
                        HashSet::from_iter(
                            self.events_configuration.private_attributes.iter().cloned(),
                        ),
                    ),
                    false => custom,
                };

                self.outbox.add_event(OutputEvent::Custom(event));
            }
        }
    }

    fn notice_user(&mut self, user: &User) -> bool {
        let key = user.key().to_string();

        if self.user_keys.get(&key).is_none() {
            trace!("noticing new user {:?}", key);
            self.user_keys.put(key, ());
            true
        } else {
            trace!("ignoring already-seen user {:?}", key);
            false
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub(super) enum EventDispatcherMessage {
    EventMessage(InputEvent),
    Flush,
    Close(Sender<()>),
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::events::event::{EventFactory, OutputEvent};
    use crate::events::{create_event_sender, create_events_configuration};
    use crate::test_common::basic_flag;
    use launchdarkly_server_sdk_evaluation::{Detail, FlagValue, Reason};
    use test_case::test_case;

    #[test]
    fn get_payload_from_outbox_empties_outbox() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let user = User::with_key("user".to_string()).build();
        let event_factory = EventFactory::new(true);
        dispatcher.process_event(event_factory.new_identify(user.clone()));
        let _ = dispatcher.outbox.get_payload();

        assert!(dispatcher.outbox.is_empty());
    }

    #[test]
    fn dispatcher_ignores_events_over_capacity() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let user = User::with_key("user".to_string()).build();
        let event_factory = EventFactory::new(true);

        for _ in 0..10 {
            dispatcher.process_event(event_factory.new_identify(user.clone()));
        }

        assert_eq!(5, dispatcher.outbox.events.len());
        assert!(dispatcher
            .outbox
            .events
            .iter()
            .all(|event| event.kind() == "identify"));
        assert_eq!(1, dispatcher.user_keys.len());
    }

    #[test]
    fn dispatcher_handles_feature_request_events_correctly() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let user = User::with_key("user".to_string()).build();
        let mut flag = basic_flag("flag");
        flag.debug_events_until_date = Some(64_060_606_800_000);
        flag.track_events = true;

        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let feature_request_event = event_factory.new_eval_event(
            &flag.key,
            user,
            &flag,
            detail,
            FlagValue::from(false),
            None,
        );

        dispatcher.process_event(feature_request_event);
        assert_eq!(3, dispatcher.outbox.events.len());
        assert_eq!("index", dispatcher.outbox.events[0].kind());
        assert_eq!("debug", dispatcher.outbox.events[1].kind());
        assert_eq!("feature", dispatcher.outbox.events[2].kind());
        assert_eq!(1, dispatcher.user_keys.len());
        assert_eq!(1, dispatcher.outbox.summary.features.len());
    }

    #[test_case(0, 64_060_606_800_000, vec!["debug", "index", "summary"])]
    #[test_case(64_060_606_800_000, 64_060_606_800_000, vec!["index", "summary"])]
    #[test_case(64_060_606_800_001, 64_060_606_800_000, vec!["index", "summary"])]
    fn sending_feature_event_emits_debug_event_correctly(
        last_known_time: u128,
        debug_events_until_date: u64,
        expected_event_types: Vec<&str>,
    ) {
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

        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, true, Duration::from_secs(100));
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);

        let mut dispatcher = create_dispatcher(events_configuration);
        dispatcher.last_known_time = last_known_time;

        let dispatcher_handle = thread::Builder::new()
            .spawn(move || dispatcher.start(inbox_rx))
            .unwrap();

        inbox_tx
            .send(EventDispatcherMessage::EventMessage(feature_request))
            .expect("event send failed");
        inbox_tx
            .send(EventDispatcherMessage::Flush)
            .expect("flush failed");

        let (tx, rx) = bounded(1);
        inbox_tx
            .send(EventDispatcherMessage::Close(tx))
            .expect("failed to close");
        rx.recv().expect("failed to notify on close");
        dispatcher_handle.join().unwrap();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(expected_event_types.len(), events.len());

        let kinds = events.iter().map(|event| event.kind()).collect::<Vec<_>>();

        for event_type in &expected_event_types {
            assert!(kinds.contains(event_type));
        }
    }

    #[test]
    fn dispatcher_does_not_notice_user_from_alias_event() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let user = User::with_key("user".to_string()).build();
        let previous = User::with_key("previous".to_string()).build();
        let event_factory = EventFactory::new(true);

        dispatcher.process_event(event_factory.new_alias(user.clone(), previous));
        assert_eq!(1, dispatcher.outbox.events.len());
        assert_eq!("alias", dispatcher.outbox.events[0].kind());
        assert_eq!(0, dispatcher.user_keys.len());
    }

    #[test]
    fn dispatcher_only_notices_identity_event_once() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let user = User::with_key("user".to_string()).build();
        let event_factory = EventFactory::new(true);

        dispatcher.process_event(event_factory.new_identify(user.clone()));
        dispatcher.process_event(event_factory.new_identify(user));
        assert_eq!(2, dispatcher.outbox.events.len());
        assert_eq!("identify", dispatcher.outbox.events[0].kind());
        assert_eq!("identify", dispatcher.outbox.events[1].kind());
        assert_eq!(1, dispatcher.user_keys.len());
    }

    #[test]
    fn dispatcher_adds_index_on_custom_event() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let user = User::with_key("user".to_string()).build();
        let event_factory = EventFactory::new(true);
        let custom_event = event_factory
            .new_custom(user, "user", None, "")
            .expect("failed to make new custom event");

        dispatcher.process_event(custom_event);
        assert_eq!(2, dispatcher.outbox.events.len());
        assert_eq!("index", dispatcher.outbox.events[0].kind());
        assert_eq!("custom", dispatcher.outbox.events[1].kind());
        assert_eq!(1, dispatcher.user_keys.len());
    }

    #[test]
    fn can_process_events_successfully() {
        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_secs(100));
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);

        let mut dispatcher = create_dispatcher(events_configuration);
        let dispatcher_handle = thread::Builder::new()
            .spawn(move || dispatcher.start(inbox_rx))
            .unwrap();

        let user = User::with_key("user".to_string()).build();
        let event_factory = EventFactory::new(true);

        inbox_tx
            .send(EventDispatcherMessage::EventMessage(
                event_factory.new_identify(user),
            ))
            .expect("event send failed");
        inbox_tx
            .send(EventDispatcherMessage::Flush)
            .expect("flush failed");

        let (tx, rx) = bounded(1);
        inbox_tx
            .send(EventDispatcherMessage::Close(tx))
            .expect("failed to close");
        rx.recv().expect("failed to notify on close");
        dispatcher_handle.join().unwrap();

        let events = event_rx.iter().collect::<Vec<OutputEvent>>();
        assert_eq!(1, events.len());
    }

    #[test]
    fn dispatcher_flushes_periodically() {
        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, false, Duration::from_millis(200));
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);

        let mut dispatcher = create_dispatcher(events_configuration);
        let _ = thread::Builder::new()
            .spawn(move || dispatcher.start(inbox_rx))
            .unwrap();

        let user = User::with_key("user".to_string()).build();
        let event_factory = EventFactory::new(true);

        inbox_tx
            .send(EventDispatcherMessage::EventMessage(
                event_factory.new_identify(user),
            ))
            .expect("event send failed");

        std::thread::sleep(Duration::from_millis(300));
        let events = event_rx.try_iter().collect::<Vec<OutputEvent>>();
        assert_eq!(1, events.len());
    }

    fn create_dispatcher(events_configuration: EventsConfiguration) -> EventDispatcher {
        EventDispatcher::new(events_configuration)
    }
}
