use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};

use launchdarkly_server_sdk_evaluation::{
    Context, ContextAttributes, Detail, Flag, FlagValue, Kind, Reason, Reference, VariationIndex,
};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

#[derive(Clone, Debug, PartialEq)]
pub struct BaseEvent {
    pub creation_date: u64,
    pub context: Context,

    // These attributes will not be serialized. They exist only to help serialize base event into
    // the right structure
    inline: bool,
    all_attribute_private: bool,
    global_private_attributes: HashSet<Reference>,
}

impl Serialize for BaseEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BaseEvent", 2)?;
        state.serialize_field("creationDate", &self.creation_date)?;

        if self.inline {
            let context_attribute = ContextAttributes::from_context(
                self.context.clone(),
                self.all_attribute_private,
                self.global_private_attributes.clone(),
            );
            state.serialize_field("context", &context_attribute)?;
        } else {
            state.serialize_field("contextKeys", &self.context.context_keys())?;
        }

        state.end()
    }
}

impl BaseEvent {
    pub fn new(creation_date: u64, context: Context) -> Self {
        Self {
            creation_date,
            context,
            inline: false,
            all_attribute_private: false,
            global_private_attributes: HashSet::new(),
        }
    }

    pub(crate) fn into_inline(
        self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<Reference>,
    ) -> Self {
        Self {
            inline: true,
            all_attribute_private,
            global_private_attributes,
            ..self
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureRequestEvent {
    #[serde(flatten)]
    pub(crate) base: BaseEvent,
    key: String,
    value: FlagValue,
    variation: Option<VariationIndex>,
    default: FlagValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<Reason>,
    version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prereq_of: Option<String>,

    #[serde(skip)]
    pub(crate) track_events: bool,

    #[serde(skip)]
    pub(crate) debug_events_until_date: Option<u64>,
}

impl FeatureRequestEvent {
    pub fn to_index_event(
        &self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<Reference>,
    ) -> IndexEvent {
        self.base
            .clone()
            .into_inline(all_attribute_private, global_private_attributes)
            .into()
    }

    pub(crate) fn into_inline(
        self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<Reference>,
    ) -> Self {
        Self {
            base: self
                .base
                .into_inline(all_attribute_private, global_private_attributes),
            ..self
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct IndexEvent {
    #[serde(flatten)]
    base: BaseEvent,
}

impl From<BaseEvent> for IndexEvent {
    fn from(base: BaseEvent) -> Self {
        let base = BaseEvent {
            inline: true,
            ..base
        };

        Self { base }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct IdentifyEvent {
    #[serde(flatten)]
    pub(crate) base: BaseEvent,
    key: String,
}

impl IdentifyEvent {
    pub(crate) fn into_inline(
        self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<Reference>,
    ) -> Self {
        Self {
            base: self
                .base
                .into_inline(all_attribute_private, global_private_attributes),
            ..self
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomEvent {
    #[serde(flatten)]
    pub(crate) base: BaseEvent,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metric_value: Option<f64>,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    data: serde_json::Value,
}

impl CustomEvent {
    pub fn to_index_event(
        &self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<Reference>,
    ) -> IndexEvent {
        self.base
            .clone()
            .into_inline(all_attribute_private, global_private_attributes)
            .into()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind")]
#[allow(clippy::large_enum_variant)]
pub enum OutputEvent {
    #[serde(rename = "index")]
    Index(IndexEvent),

    #[serde(rename = "debug")]
    Debug(FeatureRequestEvent),

    #[serde(rename = "feature")]
    FeatureRequest(FeatureRequestEvent),

    #[serde(rename = "identify")]
    Identify(IdentifyEvent),

    #[serde(rename = "custom")]
    Custom(CustomEvent),

    #[serde(rename = "summary")]
    Summary(EventSummary),
}

impl OutputEvent {
    #[cfg(test)]
    pub fn kind(&self) -> &'static str {
        match self {
            OutputEvent::Index { .. } => "index",
            OutputEvent::Debug { .. } => "debug",
            OutputEvent::FeatureRequest { .. } => "feature",
            OutputEvent::Identify { .. } => "identify",
            OutputEvent::Custom { .. } => "custom",
            OutputEvent::Summary { .. } => "summary",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum InputEvent {
    FeatureRequest(FeatureRequestEvent),
    Identify(IdentifyEvent),
    Custom(CustomEvent),
}

impl InputEvent {
    #[cfg(test)]
    pub fn base_mut(&mut self) -> Option<&mut BaseEvent> {
        match self {
            InputEvent::FeatureRequest(FeatureRequestEvent { base, .. }) => Some(base),
            InputEvent::Identify(IdentifyEvent { base, .. }) => Some(base),
            InputEvent::Custom(CustomEvent { base, .. }) => Some(base),
        }
    }
}

impl Display for InputEvent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let json = serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("JSON serialization failed ({}): {:?}", e, self));
        write!(f, "{}", json)
    }
}

pub struct EventFactory {
    send_reason: bool,
}

impl EventFactory {
    pub fn new(send_reason: bool) -> Self {
        Self { send_reason }
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn new_unknown_flag_event(
        &self,
        flag_key: &str,
        context: Context,
        detail: Detail<FlagValue>,
        default: FlagValue,
    ) -> InputEvent {
        self.new_feature_request_event(flag_key, context, None, detail, default, None)
    }

    pub fn new_eval_event(
        &self,
        flag_key: &str,
        context: Context,
        flag: &Flag,
        detail: Detail<FlagValue>,
        default: FlagValue,
        prereq_of: Option<String>,
    ) -> InputEvent {
        self.new_feature_request_event(flag_key, context, Some(flag), detail, default, prereq_of)
    }

    fn new_feature_request_event(
        &self,
        flag_key: &str,
        context: Context,
        flag: Option<&Flag>,
        detail: Detail<FlagValue>,
        default: FlagValue,
        prereq_of: Option<String>,
    ) -> InputEvent {
        let value = detail
            .value
            .unwrap_or(FlagValue::Json(serde_json::Value::Null));

        let flag_track_events;
        let require_experiment_data;
        let debug_events_until_date;

        if let Some(f) = flag {
            flag_track_events = f.track_events;
            require_experiment_data = f.is_experimentation_enabled(&detail.reason);
            debug_events_until_date = f.debug_events_until_date;
        } else {
            flag_track_events = false;
            require_experiment_data = false;
            debug_events_until_date = None
        }

        let reason = if self.send_reason || require_experiment_data {
            Some(detail.reason)
        } else {
            None
        };

        InputEvent::FeatureRequest(FeatureRequestEvent {
            base: BaseEvent::new(Self::now(), context),
            key: flag_key.to_owned(),
            default,
            reason,
            value,
            variation: detail.variation_index,
            version: flag.map(|f| f.version),
            prereq_of,
            track_events: flag_track_events || require_experiment_data,
            debug_events_until_date,
        })
    }

    pub fn new_identify(&self, context: Context) -> InputEvent {
        InputEvent::Identify(IdentifyEvent {
            key: context.key().to_owned(),
            base: BaseEvent::new(Self::now(), context),
        })
    }

    pub fn new_custom(
        &self,
        context: Context,
        key: impl Into<String>,
        metric_value: Option<f64>,
        data: impl Serialize,
    ) -> serde_json::Result<InputEvent> {
        let data = serde_json::to_value(data)?;

        Ok(InputEvent::Custom(CustomEvent {
            base: BaseEvent::new(Self::now(), context),
            key: key.into(),
            metric_value,
            data,
        }))
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(into = "EventSummaryOutput")]
pub struct EventSummary {
    pub(crate) start_date: u64,
    pub(crate) end_date: u64,
    pub(crate) features: HashMap<String, FlagSummary>,
}

impl Default for EventSummary {
    fn default() -> Self {
        EventSummary::new()
    }
}

impl EventSummary {
    pub fn new() -> Self {
        EventSummary {
            start_date: u64::MAX,
            end_date: 0,
            features: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.features.is_empty()
    }

    pub fn add(&mut self, event: &FeatureRequestEvent) {
        let FeatureRequestEvent {
            base:
                BaseEvent {
                    creation_date,
                    context,
                    ..
                },
            key,
            value,
            version,
            variation,
            default,
            ..
        } = event;

        self.start_date = min(self.start_date, *creation_date);
        self.end_date = max(self.end_date, *creation_date);

        let variation_key = VariationKey {
            version: *version,
            variation: *variation,
        };

        let feature = self
            .features
            .entry(key.clone())
            .or_insert_with(|| FlagSummary::new(default.clone()));

        feature.track(variation_key, value, context);
    }

    pub fn reset(&mut self) {
        self.features.clear();
        self.start_date = u64::MAX;
        self.end_date = 0;
    }
}

#[derive(Clone, Debug)]
pub struct FlagSummary {
    pub(crate) counters: HashMap<VariationKey, VariationSummary>,
    pub(crate) default: FlagValue,
    pub(crate) context_kinds: HashSet<Kind>,
}

impl FlagSummary {
    pub fn new(default: FlagValue) -> Self {
        Self {
            counters: HashMap::new(),
            default,
            context_kinds: HashSet::new(),
        }
    }

    pub fn track(
        &mut self,
        variation_key: VariationKey,
        value: &FlagValue,
        context: &Context,
    ) -> &mut Self {
        if let Some(summary) = self.counters.get_mut(&variation_key) {
            summary.count_request();
        } else {
            self.counters
                .insert(variation_key, VariationSummary::new(value.clone()));
        }

        for kind in context.kinds() {
            self.context_kinds.insert(kind.clone());
        }

        self
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct VariationKey {
    pub version: Option<u64>,
    pub variation: Option<VariationIndex>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariationSummary {
    pub count: u64,
    pub value: FlagValue,
}

impl VariationSummary {
    fn new(value: FlagValue) -> Self {
        VariationSummary { count: 1, value }
    }

    fn count_request(&mut self) {
        self.count += 1;
    }
}

// Implement event summarisation a second time because we report it summarised a different way than
// we collected it.
//
// (See #[serde(into)] annotation on EventSummary.)

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventSummaryOutput {
    start_date: u64,
    end_date: u64,
    features: HashMap<String, FeatureSummaryOutput>,
}

impl From<EventSummary> for EventSummaryOutput {
    fn from(summary: EventSummary) -> Self {
        let features = summary
            .features
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect();

        EventSummaryOutput {
            start_date: summary.start_date,
            end_date: summary.end_date,
            features,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FeatureSummaryOutput {
    default: FlagValue,
    context_kinds: HashSet<Kind>,
    counters: Vec<VariationCounterOutput>,
}

impl From<FlagSummary> for FeatureSummaryOutput {
    fn from(flag_summary: FlagSummary) -> Self {
        let counters = flag_summary
            .counters
            .into_iter()
            .map(|(variation_key, variation_summary)| (variation_key, variation_summary).into())
            .collect::<Vec<VariationCounterOutput>>();

        Self {
            default: flag_summary.default,
            context_kinds: flag_summary.context_kinds,
            counters,
        }
    }
}

#[derive(Serialize)]
struct VariationCounterOutput {
    pub value: FlagValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unknown: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    pub count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variation: Option<VariationIndex>,
}

impl From<(VariationKey, VariationSummary)> for VariationCounterOutput {
    fn from((variation_key, variation_summary): (VariationKey, VariationSummary)) -> Self {
        VariationCounterOutput {
            value: variation_summary.value,
            unknown: variation_key.version.map_or(Some(true), |_| None),
            version: variation_key.version,
            count: variation_summary.count,
            variation: variation_key.variation,
        }
    }
}

#[cfg(test)]
mod tests {
    use launchdarkly_server_sdk_evaluation::{
        AttributeValue, ContextBuilder, Kind, MultiContextBuilder,
    };
    use maplit::{hashmap, hashset};

    use super::*;
    use crate::test_common::basic_flag;
    use assert_json_diff::assert_json_eq;
    use serde_json::json;
    use test_case::test_case;

    #[test]
    fn serializes_feature_request_event() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event =
            event_factory.new_eval_event(&flag.key, context, &flag, fallthrough, default, None);
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(
                feature_request_event.into_inline(false, HashSet::new()),
            );
            let event_json = json!({
              "kind": "feature",
              "creationDate": 1234,
              "context": {
                "key": "alice",
                "kind": "user",
                "anonymous": true
              },
              "key": "flag",
              "value": false,
              "variation": 1,
              "default": false,
              "reason": {
                "kind": "FALLTHROUGH"
              },
              "version": 42
            });

            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_feature_request_event_with_global_private_attribute() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .set_value("foo", AttributeValue::Bool(true))
            .build()
            .expect("Failed to create context");
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event =
            event_factory.new_eval_event(&flag.key, context, &flag, fallthrough, default, None);
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(
                feature_request_event.into_inline(false, hashset!["foo".into()]),
            );
            let event_json = json!({
              "kind": "feature",
              "creationDate": 1234,
              "context": {
                "key": "alice",
                "kind": "user",
                "anonymous": true,
                "_meta" : {
                    "redactedAttributes" : ["foo"]
                }
              },
              "key": "flag",
              "value": false,
              "variation": 1,
              "default": false,
              "reason": {
                "kind": "FALLTHROUGH"
              },
              "version": 42
            });

            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_feature_request_event_with_all_private_attributes() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .set_value("foo", AttributeValue::Bool(true))
            .build()
            .expect("Failed to create context");
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event =
            event_factory.new_eval_event(&flag.key, context, &flag, fallthrough, default, None);
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(
                feature_request_event.into_inline(true, HashSet::new()),
            );
            let event_json = json!({
              "kind": "feature",
              "creationDate": 1234,
              "context": {
                "_meta": {
                  "redactedAttributes" : ["foo"]
                },
                "key": "alice",
                "kind": "user",
                "anonymous": true
              },
              "key": "flag",
              "value": false,
              "variation": 1,
              "default": false,
              "reason": {
                "kind": "FALLTHROUGH"
              },
              "version": 42
            });

            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_feature_request_event_with_local_private_attribute() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .set_value("foo", AttributeValue::Bool(true))
            .add_private_attribute("foo")
            .build()
            .expect("Failed to create context");
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event =
            event_factory.new_eval_event(&flag.key, context, &flag, fallthrough, default, None);
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(
                feature_request_event.into_inline(false, HashSet::new()),
            );
            let event_json = json!({
              "kind": "feature",
              "creationDate": 1234,
              "context": {
                "_meta": {
                  "redactedAttributes" : ["foo"]
                },
                "key": "alice",
                "kind": "user",
                "anonymous": true
              },
              "key": "flag",
              "value": false,
              "variation": 1,
              "default": false,
              "reason": {
                "kind": "FALLTHROUGH"
              },
              "version": 42
            });

            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_feature_request_event_without_inlining_user() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event =
            event_factory.new_eval_event(&flag.key, context, &flag, fallthrough, default, None);
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(feature_request_event);
            let event_json = json!({
                "kind": "feature",
                "creationDate": 1234,
                "contextKeys": {
                    "user": "alice"
                },
                "key": "flag",
                "value": false,
                "variation": 1,
                "default": false,
                "reason": {
                    "kind": "FALLTHROUGH"
                },
                "version": 42
            });
            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_identify_event() {
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let event_factory = EventFactory::new(true);
        let mut identify = event_factory.new_identify(context);
        identify.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::Identify(identify) = identify {
            let output_event = OutputEvent::Identify(identify.into_inline(false, HashSet::new()));
            let event_json = json!({
              "kind": "identify",
              "creationDate": 1234,
              "context": {
                "key": "alice",
                "kind": "user",
                "anonymous": true
              },
              "key": "alice"
            });
            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_custom_event() {
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .build()
            .expect("Failed to create context");

        let event_factory = EventFactory::new(true);
        let mut custom_event = event_factory
            .new_custom(
                context,
                "custom-key",
                Some(12345.0),
                serde_json::Value::Null,
            )
            .unwrap();
        // fix creation date so JSON is predictable
        custom_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::Custom(custom_event) = custom_event {
            let output_event = OutputEvent::Custom(custom_event);
            let event_json = json!({
                "kind": "custom",
                "creationDate": 1234,
                "contextKeys": {
                    "user": "alice"
                },
                "key": "custom-key",
                "metricValue": 12345.0
            });
            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_custom_event_without_inlining_user() {
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .build()
            .expect("Failed to create context");

        let event_factory = EventFactory::new(true);
        let mut custom_event = event_factory
            .new_custom(
                context,
                "custom-key",
                Some(12345.0),
                serde_json::Value::Null,
            )
            .unwrap();
        // fix creation date so JSON is predictable
        custom_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::Custom(custom_event) = custom_event {
            let output_event = OutputEvent::Custom(custom_event);
            let event_json = json!({
                "kind": "custom",
                "creationDate": 1234,
                "contextKeys": {
                    "user": "alice"
                },
                "key": "custom-key",
                "metricValue": 12345.0
            });
            assert_json_eq!(output_event, event_json);
        }
    }

    #[test]
    fn serializes_summary_event() {
        let summary = EventSummary {
            start_date: 1234,
            end_date: 4567,
            features: hashmap! {
                "f".into() => FlagSummary {
                    counters: hashmap! {
                        VariationKey{version: Some(2), variation: Some(1)} => VariationSummary{count: 1, value: true.into()},
                    },
                    default: false.into(),
                    context_kinds: HashSet::new(),
                }
            },
        };
        let summary_event = OutputEvent::Summary(summary);

        let event_json = json!({
            "kind": "summary",
            "startDate": 1234,
            "endDate": 4567,
            "features": {
                "f": {
                    "default": false,
                    "contextKinds": [],
                    "counters": [{
                        "value": true,
                        "version": 2,
                        "count": 1,
                        "variation": 1
                    }]
                }
        }});
        assert_json_eq!(summary_event, event_json);
    }

    #[test]
    fn summary_resets_appropriately() {
        let mut summary = EventSummary {
            start_date: 1234,
            end_date: 4567,
            features: hashmap! {
                    "f".into() => FlagSummary {
                        counters: hashmap!{
                            VariationKey{version: Some(2), variation: Some(1)} => VariationSummary{count: 1, value: true.into()}
                        },
                        default: false.into(),
                        context_kinds: HashSet::new(),
                }
            },
        };

        summary.reset();

        assert!(summary.features.is_empty());
        assert_eq!(summary.start_date, u64::MAX);
        assert_eq!(summary.end_date, 0);
    }

    #[test]
    fn serializes_index_event() {
        let context = ContextBuilder::new("alice")
            .anonymous(true)
            .build()
            .expect("Failed to create context");
        let base_event = BaseEvent::new(1234, context);
        let index_event = OutputEvent::Index(base_event.into());

        let event_json = json!({
              "kind": "index",
              "creationDate": 1234,
              "context": {
                "key": "alice",
                "kind": "user",
                "anonymous": true
              }
        });

        assert_json_eq!(index_event, event_json);
    }

    #[test]
    fn summarises_feature_request() {
        let mut summary = EventSummary::new();
        assert!(summary.is_empty());
        assert!(summary.start_date > summary.end_date);

        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let context = MultiContextBuilder::new()
            .add_context(
                ContextBuilder::new("alice")
                    .build()
                    .expect("Failed to create context"),
            )
            .add_context(
                ContextBuilder::new("LaunchDarkly")
                    .kind("org")
                    .build()
                    .expect("Failed to create context"),
            )
            .build()
            .expect("Failed to create multi-context");

        let value = FlagValue::from(false);
        let variation_index = 1;
        let reason = Reason::Fallthrough {
            in_experiment: false,
        };
        let eval_at = 1234;

        let fallthrough_request = FeatureRequestEvent {
            base: BaseEvent::new(eval_at, context),
            key: flag.key.clone(),
            value: value.clone(),
            variation: Some(variation_index),
            default: default.clone(),
            version: Some(flag.version),
            reason: Some(reason),
            prereq_of: None,
            track_events: false,
            debug_events_until_date: None,
        };

        summary.add(&fallthrough_request);
        assert!(!summary.is_empty());
        assert_eq!(summary.start_date, eval_at);
        assert_eq!(summary.end_date, eval_at);

        let fallthrough_key = VariationKey {
            version: Some(flag.version),
            variation: Some(variation_index),
        };

        let feature = summary.features.get(&flag.key);
        assert!(feature.is_some());
        let feature = feature.unwrap();
        assert_eq!(feature.default, default);
        assert_eq!(2, feature.context_kinds.len());
        assert!(feature.context_kinds.contains(&Kind::user()));
        assert!(feature
            .context_kinds
            .contains(&Kind::try_from("org").unwrap()));

        let fallthrough_summary = feature.counters.get(&fallthrough_key);
        if let Some(VariationSummary { count: c, value: v }) = fallthrough_summary {
            assert_eq!(*c, 1);
            assert_eq!(*v, value);
        } else {
            panic!("Fallthrough summary is wrong type");
        }

        summary.add(&fallthrough_request);
        let feature = summary
            .features
            .get(&flag.key)
            .expect("Failed to get expected feature.");
        let fallthrough_summary = feature
            .counters
            .get(&fallthrough_key)
            .expect("Failed to get counters");
        assert_eq!(fallthrough_summary.count, 2);
        assert_eq!(2, feature.context_kinds.len());
    }

    #[test]
    fn event_factory_unknown_flags_do_not_track_events() {
        let event_factory = EventFactory::new(true);
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Off,
        };
        let event =
            event_factory.new_unknown_flag_event("myFlag", context, detail, FlagValue::Bool(true));

        if let InputEvent::FeatureRequest(event) = event {
            assert!(!event.track_events);
        } else {
            panic!("Event should be a feature request type");
        }
    }

    // Test for flag.track-events
    #[test_case(true, true, false, Reason::Off, true, true)]
    #[test_case(true, false, false, Reason::Off, false, true)]
    #[test_case(false, true, false, Reason::Off, true, false)]
    #[test_case(false, false, false, Reason::Off, false, false)]
    // Test for flag.track_events_fallthrough
    #[test_case(true, false, true, Reason::Off, false, true)]
    #[test_case(true, false, true, Reason::Fallthrough { in_experiment: false }, true, true)]
    #[test_case(true, false, false, Reason::Fallthrough { in_experiment: false }, false, true)]
    #[test_case(false, false, true, Reason::Off, false, false)]
    #[test_case(false, false, true, Reason::Fallthrough { in_experiment: false }, true, true)]
    #[test_case(false, false, false, Reason::Fallthrough { in_experiment: false }, false, false)]
    // Test for Flagthrough.in_experiment
    #[test_case(true, false, false, Reason::Fallthrough { in_experiment: true }, true, true)]
    #[test_case(false, false, false, Reason::Fallthrough { in_experiment: true }, true, true)]
    fn event_factory_eval_tracks_events(
        event_factory_send_events: bool,
        flag_track_events: bool,
        flag_track_events_fallthrough: bool,
        reason: Reason,
        should_events_be_tracked: bool,
        should_include_reason: bool,
    ) {
        let event_factory = EventFactory::new(event_factory_send_events);
        let mut flag = basic_flag("myFlag");
        flag.track_events = flag_track_events;
        flag.track_events_fallthrough = flag_track_events_fallthrough;

        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason,
        };
        let event = event_factory.new_eval_event(
            "myFlag",
            context,
            &flag,
            detail,
            FlagValue::Bool(true),
            None,
        );

        if let InputEvent::FeatureRequest(event) = event {
            assert_eq!(event.track_events, should_events_be_tracked);
            assert_eq!(event.reason.is_some(), should_include_reason);
        } else {
            panic!("Event should be a feature request type");
        }
    }

    #[test_case(true, 0, false, true, true)]
    #[test_case(true, 0, true, true, true)]
    #[test_case(true, 1, false, false, true)]
    #[test_case(true, 1, true, true, true)]
    #[test_case(false, 0, false, true, true)]
    #[test_case(false, 0, true, true, true)]
    #[test_case(false, 1, false, false, false)]
    #[test_case(false, 1, true, true, true)]
    fn event_factory_eval_tracks_events_for_rule_matches(
        event_factory_send_events: bool,
        rule_index: usize,
        rule_in_experiment: bool,
        should_events_be_tracked: bool,
        should_include_reason: bool,
    ) {
        let event_factory = EventFactory::new(event_factory_send_events);
        let flag: Flag = serde_json::from_value(json!({
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
          "trackEventsFallthrough": false,
          "offVariation": 0,
          "clientSideAvailability": {
            "usingMobileKey": false,
            "usingEnvironmentId": false
          },
          "salt": "kosher",
          "version": 2,
          "variations": [false, true]
        }))
        .unwrap();

        let context = ContextBuilder::new("do-track")
            .build()
            .expect("Failed to create context");
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::RuleMatch {
                rule_index,
                rule_id: format!("rule-{}", rule_index),
                in_experiment: rule_in_experiment,
            },
        };
        let event = event_factory.new_eval_event(
            "myFlag",
            context,
            &flag,
            detail,
            FlagValue::Bool(true),
            None,
        );

        if let InputEvent::FeatureRequest(event) = event {
            assert_eq!(event.track_events, should_events_be_tracked);
            assert_eq!(event.reason.is_some(), should_include_reason);
        } else {
            panic!("Event should be a feature request type");
        }
    }
}
