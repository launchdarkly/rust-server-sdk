use crossbeam_channel::{bounded, select, tick, Receiver, Sender};
use rand::rng;
use std::time::SystemTime;

use launchdarkly_server_sdk_evaluation::Context;
use lru::LruCache;

use super::event::{BaseEvent, FeatureRequestEvent, IndexEvent};
use crate::sampler::{Sampler, ThreadRngSampler};

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
    outbox: Outbox,
    context_keys: LruCache<String, ()>,
    events_configuration: EventsConfiguration,
    last_known_time: u128,
    disabled: bool,
    thread_count: usize,
    sampler: Box<dyn Sampler>,
}

impl EventDispatcher {
    pub(super) fn new(events_configuration: EventsConfiguration) -> Self {
        Self {
            outbox: Outbox::new(events_configuration.capacity),
            context_keys: LruCache::<String, ()>::new(events_configuration.context_keys_capacity),
            events_configuration,
            last_known_time: 0,
            disabled: false,
            thread_count: 5,
            sampler: Box::new(ThreadRngSampler::new(rng())),
        }
    }

    pub(super) fn start(&mut self, inbox_rx: Receiver<EventDispatcherMessage>) {
        let reset_context_cache_ticker =
            tick(self.events_configuration.context_keys_flush_interval);
        let flush_ticker = tick(self.events_configuration.flush_interval);
        let (event_result_tx, event_result_rx) = bounded::<EventSenderResult>(self.thread_count);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.thread_count)
            .enable_io()
            .enable_time()
            .build();

        let rt = match rt {
            Ok(rt) => rt,
            Err(e) => {
                error!("Could not start runtime for event sending: {}", e);
                return;
            }
        };

        let (send, recv) = bounded::<()>(1);

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
                    recv(reset_context_cache_ticker) -> _ => self.context_keys.clear(),
                    recv(flush_ticker) -> _ => break,
                    recv(inbox_rx) -> result => match result {
                        Ok(EventDispatcherMessage::Flush) => break,
                        Ok(EventDispatcherMessage::EventMessage(event)) => {
                            if !self.disabled {
                                self.process_event(event);
                            }
                        }
                        Ok(EventDispatcherMessage::Close(sender)) => {
                            drop(send);
                            //Should unblock once all the senders are dropped.
                            let _ = recv.recv();

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

            if !self.outbox.is_empty() {
                let payload = self.outbox.get_payload();

                debug!("Sending batch of {} events", payload.len());

                let sender = self.events_configuration.event_sender.clone();
                let results = event_result_tx.clone();
                let send = send.clone();
                rt.spawn(async move {
                    sender.send_event_data(payload, results).await;
                    drop(send);
                });
            }
        }
    }

    fn process_event(&mut self, event: InputEvent) {
        match event {
            InputEvent::MigrationOp(migration_op) => {
                if self
                    .sampler
                    .sample(migration_op.sampling_ratio.unwrap_or(1))
                {
                    self.outbox
                        .add_event(OutputEvent::MigrationOp(migration_op));
                }
            }
            InputEvent::FeatureRequest(fre) => {
                if !fre.exclude_from_summaries {
                    self.outbox.add_to_summary(&fre);
                }

                let inlined = fre.clone().into_inline_with_anonymous_redaction(
                    self.events_configuration.all_attributes_private,
                    self.events_configuration.private_attributes.clone(),
                );

                if let Some(context) = self.get_indexable_context(&fre.base) {
                    let base = BaseEvent::new(fre.base.creation_date, context).into_inline(
                        self.events_configuration.all_attributes_private,
                        self.events_configuration.private_attributes.clone(),
                    );
                    self.outbox
                        .add_event(OutputEvent::Index(IndexEvent::from(base)));
                }

                let now = match SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                    Ok(time) => time.as_millis(),
                    _ => 0,
                };

                if let Some(debug_events_until_date) = fre.debug_events_until_date {
                    let time = u128::from(debug_events_until_date);
                    if time > now
                        && time > self.last_known_time
                        && self.sampler.sample(fre.sampling_ratio.unwrap_or(1))
                    {
                        self.outbox
                            .add_event(OutputEvent::Debug(fre.clone().into_inline(
                                self.events_configuration.all_attributes_private,
                                self.events_configuration.private_attributes.clone(),
                            )));
                    }
                }

                if fre.track_events && self.sampler.sample(fre.sampling_ratio.unwrap_or(1)) {
                    self.outbox.add_event(OutputEvent::FeatureRequest(inlined));
                }
            }
            InputEvent::Identify(mut identify) => {
                if self.events_configuration.omit_anonymous_contexts {
                    match identify.base.context.without_anonymous_contexts() {
                        Ok(context) => identify.base.context = context,
                        Err(_) => return,
                    }
                }

                self.notice_context(&identify.base.context);
                if self.sampler.sample(identify.sampling_ratio.unwrap_or(1)) {
                    self.outbox
                        .add_event(OutputEvent::Identify(identify.into_inline(
                            self.events_configuration.all_attributes_private,
                            self.events_configuration.private_attributes.clone(),
                        )));
                }
            }
            InputEvent::Custom(custom) => {
                if let Some(context) = self.get_indexable_context(&custom.base) {
                    let base = BaseEvent::new(custom.base.creation_date, context).into_inline(
                        self.events_configuration.all_attributes_private,
                        self.events_configuration.private_attributes.clone(),
                    );
                    self.outbox
                        .add_event(OutputEvent::Index(IndexEvent::from(base)));
                }

                if self.sampler.sample(custom.sampling_ratio.unwrap_or(1)) {
                    self.outbox.add_event(OutputEvent::Custom(custom));
                }
            }
        }
    }

    fn get_indexable_context(&mut self, event: &BaseEvent) -> Option<Context> {
        let context = match self.events_configuration.omit_anonymous_contexts {
            true => event.context.without_anonymous_contexts(),
            false => Ok(event.context.clone()),
        };

        if let Ok(ctx) = context {
            if self.notice_context(&ctx) {
                return Some(ctx);
            }

            return None;
        }

        None
    }

    fn notice_context(&mut self, context: &Context) -> bool {
        let key = context.canonical_key();

        if self.context_keys.get(key).is_none() {
            trace!("noticing new context {:?}", key);
            self.context_keys.put(key.to_owned(), ());
            true
        } else {
            trace!("ignoring already-seen context {:?}", key);
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
    use launchdarkly_server_sdk_evaluation::{
        ContextBuilder, Detail, FlagValue, MultiContextBuilder, Reason,
    };
    use test_case::test_case;

    #[test]
    fn get_payload_from_outbox_empties_outbox() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        dispatcher.process_event(event_factory.new_identify(context));
        let _ = dispatcher.outbox.get_payload();

        assert!(dispatcher.outbox.is_empty());
    }

    #[test]
    fn dispatcher_ignores_events_over_capacity() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);

        for _ in 0..10 {
            dispatcher.process_event(event_factory.new_identify(context.clone()));
        }

        assert_eq!(5, dispatcher.outbox.events.len());
        assert!(dispatcher
            .outbox
            .events
            .iter()
            .all(|event| event.kind() == "identify"));
        assert_eq!(1, dispatcher.context_keys.len());
    }

    #[test]
    fn dispatcher_handles_feature_request_events_correctly() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
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
            context,
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
        assert_eq!(1, dispatcher.context_keys.len());
        assert_eq!(1, dispatcher.outbox.summary.features.len());
    }

    #[test]
    fn dispatcher_strips_anonymous_contexts_from_index_for_feature_request_events() {
        let (event_sender, _) = create_event_sender();
        let mut events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        events_configuration.omit_anonymous_contexts = true;
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
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
            context,
            &flag,
            detail,
            FlagValue::from(false),
            None,
        );

        dispatcher.process_event(feature_request_event);
        assert_eq!(2, dispatcher.outbox.events.len());
        assert_eq!("debug", dispatcher.outbox.events[0].kind());
        assert_eq!("feature", dispatcher.outbox.events[1].kind());
        assert_eq!(0, dispatcher.context_keys.len());
        assert_eq!(1, dispatcher.outbox.summary.features.len());
    }

    #[test]
    fn dispatcher_ignores_feature_events_with_0_sampling_ratio() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let mut flag = basic_flag("flag");
        flag.sampling_ratio = Some(0);
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
            context,
            &flag,
            detail,
            FlagValue::from(false),
            None,
        );

        dispatcher.process_event(feature_request_event);
        assert_eq!(1, dispatcher.outbox.events.len());
        assert_eq!("index", dispatcher.outbox.events[0].kind());
        assert_eq!(1, dispatcher.context_keys.len());
        assert_eq!(1, dispatcher.outbox.summary.features.len());
    }

    #[test]
    fn dispatcher_can_exclude_feature_event_from_summaries() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let mut flag = basic_flag("flag");
        flag.exclude_from_summaries = true;
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
            context,
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
        assert_eq!(1, dispatcher.context_keys.len());
        assert_eq!(0, dispatcher.outbox.summary.features.len());
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
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);

        let dispatcher_handle = thread::Builder::new()
            .spawn(move || {
                let mut dispatcher = create_dispatcher(events_configuration);
                dispatcher.last_known_time = last_known_time;
                dispatcher.start(inbox_rx)
            })
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
    fn dispatcher_only_notices_identity_event_once() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);

        dispatcher.process_event(event_factory.new_identify(context.clone()));
        dispatcher.process_event(event_factory.new_identify(context));
        assert_eq!(2, dispatcher.outbox.events.len());
        assert_eq!("identify", dispatcher.outbox.events[0].kind());
        assert_eq!("identify", dispatcher.outbox.events[1].kind());
        assert_eq!(1, dispatcher.context_keys.len());
    }

    #[test]
    fn dispatcher_can_ignore_identify_if_anonymous() {
        let (event_sender, _) = create_event_sender();
        let mut events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        events_configuration.omit_anonymous_contexts = true;
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);

        dispatcher.process_event(event_factory.new_identify(context));
        assert_eq!(0, dispatcher.outbox.events.len());
        assert_eq!(0, dispatcher.context_keys.len());
    }

    #[test]
    fn dispatcher_strips_anon_contexts_from_multi_kind_identify() {
        let (event_sender, _) = create_event_sender();
        let mut events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        events_configuration.omit_anonymous_contexts = true;
        let mut dispatcher = create_dispatcher(events_configuration);

        let user_context = ContextBuilder::new("user")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let org_context = ContextBuilder::new("org")
            .kind("org")
            .build()
            .expect("Failed to create context");
        let context = MultiContextBuilder::new()
            .add_context(user_context)
            .add_context(org_context)
            .build()
            .expect("Failed to create context");

        let event_factory = EventFactory::new(true);

        dispatcher.process_event(event_factory.new_identify(context));
        assert_eq!(1, dispatcher.outbox.events.len());
        assert_eq!("identify", dispatcher.outbox.events[0].kind());
        assert_eq!(1, dispatcher.context_keys.len());

        if let OutputEvent::Identify(identify) = &dispatcher.outbox.events[0] {
            assert_eq!("org:org", identify.base.context.canonical_key());
        } else {
            panic!("Expected an identify event");
        }
    }

    #[test]
    fn dispatcher_adds_index_on_custom_event() {
        let (event_sender, _) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        let custom_event = event_factory
            .new_custom(context, "context", None, "")
            .expect("failed to make new custom event");

        dispatcher.process_event(custom_event);
        assert_eq!(2, dispatcher.outbox.events.len());
        assert_eq!("index", dispatcher.outbox.events[0].kind());
        assert_eq!("custom", dispatcher.outbox.events[1].kind());
        assert_eq!(1, dispatcher.context_keys.len());
    }

    #[test]
    fn dispatcher_can_strip_anonymous_from_index_events() {
        let (event_sender, _) = create_event_sender();
        let mut events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        events_configuration.omit_anonymous_contexts = true;
        let mut dispatcher = create_dispatcher(events_configuration);

        let context = ContextBuilder::new("context")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        let custom_event = event_factory
            .new_custom(context, "context", None, "")
            .expect("failed to make new custom event");

        dispatcher.process_event(custom_event);
        assert_eq!(1, dispatcher.outbox.events.len());
        assert_eq!("custom", dispatcher.outbox.events[0].kind());
        assert_eq!(0, dispatcher.context_keys.len());
    }

    #[test]
    fn dispatcher_can_strip_anonymous_from_index_events_with_multi_kinds() {
        let (event_sender, _) = create_event_sender();
        let mut events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        events_configuration.omit_anonymous_contexts = true;
        let mut dispatcher = create_dispatcher(events_configuration);

        let user_context = ContextBuilder::new("user")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let org_context = ContextBuilder::new("org")
            .kind("org")
            .build()
            .expect("Failed to create context");
        let context = MultiContextBuilder::new()
            .add_context(user_context)
            .add_context(org_context)
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        let custom_event = event_factory
            .new_custom(context, "context", None, "")
            .expect("failed to make new custom event");

        dispatcher.process_event(custom_event);
        assert_eq!(2, dispatcher.outbox.events.len());
        assert_eq!("index", dispatcher.outbox.events[0].kind());
        assert_eq!("custom", dispatcher.outbox.events[1].kind());
        assert_eq!(1, dispatcher.context_keys.len());
    }

    #[test]
    fn can_process_events_successfully() {
        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_secs(100));
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);

        let dispatcher_handle = thread::Builder::new()
            .spawn(move || {
                let mut dispatcher = create_dispatcher(events_configuration);
                dispatcher.start(inbox_rx)
            })
            .unwrap();

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);

        inbox_tx
            .send(EventDispatcherMessage::EventMessage(
                event_factory.new_identify(context),
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

        assert_eq!(event_rx.iter().count(), 1);
    }

    #[test]
    fn dispatcher_flushes_periodically() {
        let (event_sender, event_rx) = create_event_sender();
        let events_configuration =
            create_events_configuration(event_sender, Duration::from_millis(200));
        let (inbox_tx, inbox_rx) = bounded(events_configuration.capacity);

        let _ = thread::Builder::new()
            .spawn(move || {
                let mut dispatcher = create_dispatcher(events_configuration);
                dispatcher.start(inbox_rx)
            })
            .unwrap();

        let context = ContextBuilder::new("context")
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);

        inbox_tx
            .send(EventDispatcherMessage::EventMessage(
                event_factory.new_identify(context),
            ))
            .expect("event send failed");

        thread::sleep(Duration::from_millis(300));
        assert_eq!(event_rx.try_iter().count(), 1);
    }

    fn create_dispatcher(events_configuration: EventsConfiguration) -> EventDispatcher {
        EventDispatcher::new(events_configuration)
    }
}
