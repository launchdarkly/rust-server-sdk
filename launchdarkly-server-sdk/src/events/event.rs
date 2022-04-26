use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};

use launchdarkly_server_sdk_evaluation::{
    Detail, Flag, FlagValue, Reason, User, UserAttributes, VariationIndex,
};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ContextKind {
    AnonymousUser,
    User,
}

impl ContextKind {
    fn is_user(&self) -> bool {
        *self == ContextKind::User
    }
}

impl From<User> for ContextKind {
    fn from(user: User) -> Self {
        match user.anonymous() {
            Some(true) => Self::AnonymousUser,
            Some(false) | None => Self::User,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BaseEvent {
    pub creation_date: u64,
    pub user: User,

    // These attributes will not be serialized. They exist only to help serialize base event into
    // the right structure
    inline: bool,
    all_attribute_private: bool,
    global_private_attributes: HashSet<String>,
}

impl Serialize for BaseEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BaseEvent", 2)?;
        state.serialize_field("creationDate", &self.creation_date)?;

        if self.inline {
            let user_attribute = UserAttributes::from_user(
                self.user.clone(),
                self.all_attribute_private,
                &self.global_private_attributes,
            );
            state.serialize_field("user", &user_attribute)?;
        } else {
            state.serialize_field("userKey", &self.user.key())?;
        }

        state.end()
    }
}

impl BaseEvent {
    pub fn new(creation_date: u64, user: User) -> Self {
        Self {
            creation_date,
            user,
            inline: false,
            all_attribute_private: false,
            global_private_attributes: HashSet::new(),
        }
    }

    pub(crate) fn into_inline(
        self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<String>,
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
    #[serde(skip_serializing_if = "ContextKind::is_user")]
    context_kind: ContextKind,

    #[serde(skip)]
    pub(crate) track_events: bool,

    #[serde(skip)]
    pub(crate) debug_events_until_date: Option<u64>,
}

impl FeatureRequestEvent {
    pub fn to_index_event(
        &self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<String>,
    ) -> IndexEvent {
        self.base
            .clone()
            .into_inline(all_attribute_private, global_private_attributes)
            .into()
    }

    pub(crate) fn into_inline(
        self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<String>,
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
        global_private_attributes: HashSet<String>,
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
pub struct AliasEvent {
    pub creation_date: u64,
    key: String,
    context_kind: ContextKind,
    previous_key: String,
    previous_context_kind: ContextKind,
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
    #[serde(skip_serializing_if = "ContextKind::is_user")]
    context_kind: ContextKind,
}

impl CustomEvent {
    pub fn to_index_event(
        &self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<String>,
    ) -> IndexEvent {
        self.base
            .clone()
            .into_inline(all_attribute_private, global_private_attributes)
            .into()
    }

    pub(crate) fn into_inline(
        self,
        all_attribute_private: bool,
        global_private_attributes: HashSet<String>,
    ) -> Self {
        Self {
            base: self
                .base
                .into_inline(all_attribute_private, global_private_attributes),
            ..self
        }
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

    #[serde(rename = "alias")]
    Alias(AliasEvent),

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
            OutputEvent::Alias { .. } => "alias",
            OutputEvent::Custom { .. } => "custom",
            OutputEvent::Summary { .. } => "summary",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum InputEvent {
    FeatureRequest(FeatureRequestEvent),
    Identify(IdentifyEvent),
    Alias(AliasEvent),
    Custom(CustomEvent),
}

impl InputEvent {
    #[cfg(test)]
    pub fn base_mut(&mut self) -> Option<&mut BaseEvent> {
        match self {
            InputEvent::FeatureRequest(FeatureRequestEvent { base, .. }) => Some(base),
            InputEvent::Identify(IdentifyEvent { base, .. }) => Some(base),
            InputEvent::Alias(_) => None,
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
        user: User,
        detail: Detail<FlagValue>,
        default: FlagValue,
    ) -> InputEvent {
        self.new_feature_request_event(flag_key, user, None, detail, default, None)
    }

    pub fn new_eval_event(
        &self,
        flag_key: &str,
        user: User,
        flag: &Flag,
        detail: Detail<FlagValue>,
        default: FlagValue,
        prereq_of: Option<String>,
    ) -> InputEvent {
        self.new_feature_request_event(flag_key, user, Some(flag), detail, default, prereq_of)
    }

    fn new_feature_request_event(
        &self,
        flag_key: &str,
        user: User,
        flag: Option<&Flag>,
        detail: Detail<FlagValue>,
        default: FlagValue,
        prereq_of: Option<String>,
    ) -> InputEvent {
        // TODO(ch108604) Events created during prereq evaluation might not have a value set (e.g.
        // flag is off and the off variation is None). In those situations, we are going to default
        // to a JSON null until we can better sort out the Detail struct.
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
            base: BaseEvent::new(Self::now(), user.clone()),
            key: flag_key.to_owned(),
            default,
            reason,
            value,
            variation: detail.variation_index,
            version: flag.map(|f| f.version),
            prereq_of,
            context_kind: user.into(),
            track_events: flag_track_events || require_experiment_data,
            debug_events_until_date,
        })
    }

    pub fn new_identify(&self, user: User) -> InputEvent {
        InputEvent::Identify(IdentifyEvent {
            key: user.key().to_string(),
            base: BaseEvent::new(Self::now(), user),
        })
    }

    pub fn new_alias(&self, user: User, previous_user: User) -> InputEvent {
        InputEvent::Alias(AliasEvent {
            creation_date: Self::now(),
            key: user.key().to_string(),
            context_kind: user.into(),
            previous_key: previous_user.key().to_string(),
            previous_context_kind: previous_user.into(),
        })
    }

    pub fn new_custom(
        &self,
        user: User,
        key: impl Into<String>,
        metric_value: Option<f64>,
        data: impl Serialize,
    ) -> serde_json::Result<InputEvent> {
        let data = serde_json::to_value(data)?;

        Ok(InputEvent::Custom(CustomEvent {
            base: BaseEvent::new(Self::now(), user.clone()),
            key: key.into(),
            metric_value,
            data,
            context_kind: user.into(),
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(into = "EventSummaryOutput")]
pub struct EventSummary {
    pub start_date: u64,
    pub end_date: u64,
    pub features: HashMap<VariationKey, VariationSummary>,
}

impl Default for EventSummary {
    fn default() -> Self {
        EventSummary::new()
    }
}

impl EventSummary {
    pub fn new() -> Self {
        EventSummary {
            start_date: std::u64::MAX,
            end_date: 0,
            features: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.features.is_empty()
    }

    pub fn add(&mut self, event: &FeatureRequestEvent) {
        let FeatureRequestEvent {
            base: BaseEvent { creation_date, .. },
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
            flag_key: key.clone(),
            version: *version,
            variation: *variation,
        };
        match self.features.get_mut(&variation_key) {
            Some(summary) => summary.count_request(value, default),
            None => {
                self.features.insert(
                    variation_key,
                    VariationSummary::new(value.clone(), default.clone()),
                );
            }
        }
    }

    pub fn reset(&mut self) {
        self.features.clear();
        self.start_date = std::u64::MAX;
        self.end_date = 0;
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct VariationKey {
    pub flag_key: String,
    pub version: Option<u64>,
    pub variation: Option<VariationIndex>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VariationSummary {
    pub count: u64,
    pub value: FlagValue,
    pub default: FlagValue,
}

impl VariationSummary {
    fn new(value: FlagValue, default: FlagValue) -> Self {
        VariationSummary {
            count: 1,
            value,
            default,
        }
    }

    fn count_request(&mut self, value: &FlagValue, default: &FlagValue) {
        self.count += 1;

        if &self.value != value {
            self.value = value.clone();
        }
        if &self.default != default {
            self.default = default.clone();
        }
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
        let mut features = HashMap::new();

        for (variation_key, variation_summary) in summary.features {
            match features.get_mut(&variation_key.flag_key) {
                None => {
                    let feature_summary =
                        FeatureSummaryOutput::from((&variation_key, variation_summary));
                    features.insert(variation_key.flag_key, feature_summary);
                }
                Some(feature_summary) => feature_summary.add(&variation_key, variation_summary),
            }
        }

        EventSummaryOutput {
            start_date: summary.start_date,
            end_date: summary.end_date,
            features,
        }
    }
}

#[derive(Serialize)]
struct FeatureSummaryOutput {
    default: FlagValue,
    counters: Vec<VariationCounterOutput>,
}

impl From<(&VariationKey, VariationSummary)> for FeatureSummaryOutput {
    fn from((variation_key, variation_summary): (&VariationKey, VariationSummary)) -> Self {
        let counters = vec![VariationCounterOutput::from((
            variation_key,
            variation_summary.value,
            variation_summary.count,
        ))];

        FeatureSummaryOutput {
            default: variation_summary.default,
            counters,
        }
    }
}

impl FeatureSummaryOutput {
    fn add(&mut self, variation_key: &VariationKey, variation_summary: VariationSummary) {
        if self.default != variation_summary.default {
            self.default = variation_summary.default;
        }

        self.counters.push(VariationCounterOutput::from((
            variation_key,
            variation_summary.value,
            variation_summary.count,
        )));
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

impl From<(&VariationKey, FlagValue, u64)> for VariationCounterOutput {
    fn from((variation_key, flag_value, count): (&VariationKey, FlagValue, u64)) -> Self {
        VariationCounterOutput {
            value: flag_value,
            unknown: variation_key.version.map_or(Some(true), |_| None),
            version: variation_key.version,
            count,
            variation: variation_key.variation,
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::{hashmap, hashset};

    use super::*;
    use crate::test_common::basic_flag;
    use test_case::test_case;

    #[test_case(None, ContextKind::User; "default users are user")]
    #[test_case(Some(false), ContextKind::User; "non-anonymous users are user")]
    #[test_case(Some(true), ContextKind::AnonymousUser; "anonymous users are anonymousUser")]
    fn eval_event_context_kind_is_set_appropriately(
        is_anonymous: Option<bool>,
        context_kind: ContextKind,
    ) {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let mut user = User::with_key("alice".to_string());

        if let Some(b) = is_anonymous {
            user.anonymous(b);
        }

        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let eval_event = event_factory.new_eval_event(
            &flag.key.clone(),
            user.build(),
            &flag,
            fallthrough.clone(),
            default.clone(),
            None,
        );

        if let InputEvent::FeatureRequest(event) = eval_event {
            assert_eq!(event.context_kind, context_kind);
        } else {
            panic!("new_eval_event did not create a FeatureRequestEvent");
        }
    }

    #[test_case(false, false, ContextKind::User, ContextKind::User; "neither are anonymous")]
    #[test_case(true, true, ContextKind::AnonymousUser, ContextKind::AnonymousUser; "both are anonymous")]
    #[test_case(false, true, ContextKind::User, ContextKind::AnonymousUser; "previous is anonymous")]
    #[test_case(true, false, ContextKind::AnonymousUser, ContextKind::User; "user is anonymous")]
    fn alias_event_contains_correct_information(
        is_anonymous: bool,
        previous_is_anonymous: bool,
        context_kind: ContextKind,
        previous_context_kind: ContextKind,
    ) {
        let user = User::with_key("alice".to_string())
            .anonymous(is_anonymous)
            .build();
        let previous_user = User::with_key("previous-alice".to_string())
            .anonymous(previous_is_anonymous)
            .build();

        let event_factory = EventFactory::new(true);
        let event = event_factory.new_alias(user.clone(), previous_user.clone());

        if let InputEvent::Alias(alias) = event {
            assert_eq!(alias.key, user.key());
            assert_eq!(alias.context_kind, context_kind);
            assert_eq!(alias.previous_key, previous_user.key());
            assert_eq!(alias.previous_context_kind, previous_context_kind);
        } else {
            panic!("new_alias did not create an AliasEvent");
        }
    }

    #[test_case(None, ContextKind::User; "default users are user")]
    #[test_case(Some(false), ContextKind::User; "non-anonymous users are user")]
    #[test_case(Some(true), ContextKind::AnonymousUser; "anonymous users are anonymousUser")]
    fn custom_event_context_kind_is_set_appropriately(
        is_anonymous: Option<bool>,
        context_kind: ContextKind,
    ) {
        let flag = basic_flag("flag");
        let mut user = User::with_key("alice".to_string());

        if let Some(b) = is_anonymous {
            user.anonymous(b);
        }

        let event_factory = EventFactory::new(true);
        let custom_event = event_factory.new_custom(user.build(), &flag.key.clone(), None, "");

        if let Ok(InputEvent::Custom(event)) = custom_event {
            assert_eq!(event.context_kind, context_kind);
        } else {
            panic!("new_custom did not create a Custom event");
        }
    }

    #[test]
    fn serializes_feature_request_event() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let user = User::with_key("alice".to_string()).anonymous(true).build();
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event = event_factory.new_eval_event(
            &flag.key.clone(),
            user,
            &flag,
            fallthrough.clone(),
            default.clone(),
            None,
        );
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(
                feature_request_event.into_inline(false, HashSet::new()),
            );
            let event_json = r#"{
  "kind": "feature",
  "creationDate": 1234,
  "user": {
    "key": "alice",
    "anonymous": true
  },
  "key": "flag",
  "value": false,
  "variation": 1,
  "default": false,
  "reason": {
    "kind": "FALLTHROUGH"
  },
  "version": 42,
  "contextKind": "anonymousUser"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_feature_request_event_with_private_attributes() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let user = User::with_key("alice".to_string())
            .anonymous(true)
            .secondary("Secondary")
            .build();
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event = event_factory.new_eval_event(
            &flag.key.clone(),
            user,
            &flag,
            fallthrough.clone(),
            default.clone(),
            None,
        );
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(
                feature_request_event.into_inline(false, hashset!["secondary".into()]),
            );
            let event_json = r#"{
  "kind": "feature",
  "creationDate": 1234,
  "user": {
    "key": "alice",
    "anonymous": true,
    "privateAttrs": [
      "secondary"
    ]
  },
  "key": "flag",
  "value": false,
  "variation": 1,
  "default": false,
  "reason": {
    "kind": "FALLTHROUGH"
  },
  "version": 42,
  "contextKind": "anonymousUser"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_feature_request_event_with_all_private_attributes() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let user = User::with_key("alice".to_string())
            .anonymous(true)
            .secondary("Secondary")
            .build();
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event = event_factory.new_eval_event(
            &flag.key.clone(),
            user,
            &flag,
            fallthrough.clone(),
            default.clone(),
            None,
        );
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(
                feature_request_event.into_inline(true, HashSet::new()),
            );
            let event_json = r#"{
  "kind": "feature",
  "creationDate": 1234,
  "user": {
    "key": "alice",
    "privateAttrs": [
      "secondary",
      "anonymous"
    ]
  },
  "key": "flag",
  "value": false,
  "variation": 1,
  "default": false,
  "reason": {
    "kind": "FALLTHROUGH"
  },
  "version": 42,
  "contextKind": "anonymousUser"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_feature_request_event_without_inlining_user() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let user = User::with_key("alice".to_string()).anonymous(true).build();
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough {
                in_experiment: false,
            },
        };

        let event_factory = EventFactory::new(true);
        let mut feature_request_event = event_factory.new_eval_event(
            &flag.key.clone(),
            user,
            &flag,
            fallthrough.clone(),
            default.clone(),
            None,
        );
        // fix creation date so JSON is predictable
        feature_request_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::FeatureRequest(feature_request_event) = feature_request_event {
            let output_event = OutputEvent::FeatureRequest(feature_request_event);
            let event_json = r#"{
  "kind": "feature",
  "creationDate": 1234,
  "userKey": "alice",
  "key": "flag",
  "value": false,
  "variation": 1,
  "default": false,
  "reason": {
    "kind": "FALLTHROUGH"
  },
  "version": 42,
  "contextKind": "anonymousUser"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_identify_event() {
        let user = User::with_key("alice".to_string()).anonymous(true).build();
        let event_factory = EventFactory::new(true);
        let mut identify = event_factory.new_identify(user);
        identify.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::Identify(identify) = identify {
            let output_event = OutputEvent::Identify(identify.into_inline(false, HashSet::new()));
            let event_json = r#"{
  "kind": "identify",
  "creationDate": 1234,
  "user": {
    "key": "alice",
    "anonymous": true
  },
  "key": "alice"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_alias_event() {
        let user = User::with_key("alice".to_string()).anonymous(true).build();
        let previous_user = User::with_key("bob".to_string()).anonymous(true).build();
        let event_factory = EventFactory::new(true);
        let alias = event_factory.new_alias(user, previous_user);

        if let InputEvent::Alias(mut alias) = alias {
            alias.creation_date = 1234;
            let output_event = OutputEvent::Alias(alias);
            let event_json = r#"{
  "kind": "alias",
  "creationDate": 1234,
  "key": "alice",
  "contextKind": "anonymousUser",
  "previousKey": "bob",
  "previousContextKind": "anonymousUser"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_custom_event() {
        let user = User::with_key("alice".to_string()).anonymous(true).build();

        let event_factory = EventFactory::new(true);
        let mut custom_event = event_factory
            .new_custom(user, "custom-key", Some(12345.0), serde_json::Value::Null)
            .unwrap();
        // fix creation date so JSON is predictable
        custom_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::Custom(custom_event) = custom_event {
            let output_event = OutputEvent::Custom(custom_event.into_inline(false, HashSet::new()));
            let event_json = r#"{
  "kind": "custom",
  "creationDate": 1234,
  "user": {
    "key": "alice",
    "anonymous": true
  },
  "key": "custom-key",
  "metricValue": 12345.0,
  "contextKind": "anonymousUser"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_custom_event_without_inlining_user() {
        let user = User::with_key("alice".to_string()).anonymous(true).build();

        let event_factory = EventFactory::new(true);
        let mut custom_event = event_factory
            .new_custom(user, "custom-key", Some(12345.0), serde_json::Value::Null)
            .unwrap();
        // fix creation date so JSON is predictable
        custom_event.base_mut().unwrap().creation_date = 1234;

        if let InputEvent::Custom(custom_event) = custom_event {
            let output_event = OutputEvent::Custom(custom_event);
            let event_json = r#"{
  "kind": "custom",
  "creationDate": 1234,
  "userKey": "alice",
  "key": "custom-key",
  "metricValue": 12345.0,
  "contextKind": "anonymousUser"
}
        "#
            .trim();

            let json = serde_json::to_string_pretty(&output_event);
            assert!(json.is_ok());
            assert_eq!(json.unwrap(), event_json.to_string());
        }
    }

    #[test]
    fn serializes_summary_event() {
        let summary = EventSummary {
            start_date: 1234,
            end_date: 4567,
            features: hashmap! {
                VariationKey{flag_key: "f".into(), version: Some(2), variation: Some(1)} => VariationSummary{count: 1, value: true.into(), default: false.into()},
            },
        };
        let summary_event = OutputEvent::Summary(summary);

        let event_json = r#"
{
  "kind": "summary",
  "startDate": 1234,
  "endDate": 4567,
  "features": {
    "f": {
      "default": false,
      "counters": [
        {
          "value": true,
          "version": 2,
          "count": 1,
          "variation": 1
        }
      ]
    }
  }
}
        "#
        .trim();

        let json = serde_json::to_string_pretty(&summary_event);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), event_json.to_string());
    }

    #[test]
    fn summary_resets_appropriately() {
        let mut summary = EventSummary {
            start_date: 1234,
            end_date: 4567,
            features: hashmap! {
                VariationKey{flag_key: "f".into(), version: Some(2), variation: Some(1)} => VariationSummary{count: 1, value: true.into(), default: false.into()},
            },
        };

        summary.reset();

        assert!(summary.features.is_empty());
        assert_eq!(summary.start_date, std::u64::MAX);
        assert_eq!(summary.end_date, 0);

        assert_eq!(summary, EventSummary::default());
    }

    #[test]
    fn serializes_index_event() {
        let user = User::with_key("alice".to_string()).anonymous(true).build();
        let base_event = BaseEvent::new(1234, user);
        let index_event = OutputEvent::Index(base_event.into());

        let event_json = r#"
{
  "kind": "index",
  "creationDate": 1234,
  "user": {
    "key": "alice",
    "anonymous": true
  }
}
        "#
        .trim();

        let json = serde_json::to_string_pretty(&index_event);
        assert!(json.is_ok());
        assert_eq!(json.unwrap(), event_json.to_string());
    }

    #[test]
    fn summarises_feature_request() {
        let mut summary = EventSummary::new();
        assert!(summary.is_empty());
        assert!(summary.start_date > summary.end_date);

        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let user = User::with_key("alice".to_string()).build();

        let value = FlagValue::from(false);
        let variation_index = 1;
        let reason = Reason::Fallthrough {
            in_experiment: false,
        };
        let eval_at = 1234;

        let fallthrough_request = FeatureRequestEvent {
            base: BaseEvent::new(eval_at, user.clone()),
            key: flag.key.clone(),
            value: value.clone(),
            variation: Some(variation_index),
            default: default.clone(),
            version: Some(flag.version),
            reason: Some(reason),
            prereq_of: None,
            context_kind: user.into(),
            track_events: false,
            debug_events_until_date: None,
        };

        summary.add(&fallthrough_request);
        assert!(!summary.is_empty());
        assert_eq!(summary.start_date, eval_at);
        assert_eq!(summary.end_date, eval_at);

        let fallthrough_key = VariationKey {
            flag_key: flag.key,
            version: Some(flag.version),
            variation: Some(variation_index),
        };

        let fallthrough_summary = summary.features.get(&fallthrough_key);
        if let Some(VariationSummary {
            count: c,
            value: v,
            default: d,
        }) = fallthrough_summary
        {
            assert_eq!(*c, 1);
            assert_eq!(*v, value);
            assert_eq!(*d, default);
        } else {
            panic!("Fallthrough summary is wrong type");
        }

        summary.add(&fallthrough_request);
        let fallthrough_summary = summary.features.get(&fallthrough_key).unwrap();
        assert_eq!(fallthrough_summary.count, 2);
    }

    #[test]
    fn event_factory_unknown_flags_do_not_track_events() {
        let event_factory = EventFactory::new(true);
        let user = User::with_key("bob").build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Off,
        };
        let event =
            event_factory.new_unknown_flag_event("myFlag", user, detail, FlagValue::Bool(true));

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

        let user = User::with_key("bob").build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason,
        };
        let event = event_factory.new_eval_event(
            "myFlag",
            user,
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
                 "trackEventsFallthrough": false,
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

        let user = User::with_key("do-track").build();
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
            user,
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
