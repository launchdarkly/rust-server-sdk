use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use rust_server_sdk_evaluation::{Detail, Flag, FlagValue, Reason, User, VariationIndex};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
/// a user that may be inlined in the event.
///
/// TODO(ch108613) have the event processor handle this
pub enum MaybeInlinedUser {
    Inlined(User),
    NotInlined(User),
}

impl MaybeInlinedUser {
    pub fn new(inline: bool, user: User) -> Self {
        if inline {
            MaybeInlinedUser::Inlined(user)
        } else {
            MaybeInlinedUser::NotInlined(user)
        }
    }

    fn is_inlined(&self) -> bool {
        match self {
            MaybeInlinedUser::Inlined(_) => true,
            MaybeInlinedUser::NotInlined(_) => false,
        }
    }

    fn not_inlined(&self) -> bool {
        !self.is_inlined()
    }

    fn force_inlined(self) -> Self {
        match self {
            MaybeInlinedUser::Inlined(_) => self,
            MaybeInlinedUser::NotInlined(u) => MaybeInlinedUser::Inlined(u),
        }
    }

    fn user(&self) -> &User {
        match self {
            MaybeInlinedUser::Inlined(u) => u,
            MaybeInlinedUser::NotInlined(u) => u,
        }
    }

    pub fn key(&self) -> &str {
        self.user().key()
    }
}

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

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseEvent {
    pub creation_date: u64,
    #[serde(skip_serializing_if = "MaybeInlinedUser::not_inlined")]
    pub user: MaybeInlinedUser,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureRequestEvent {
    #[serde(flatten)]
    base: BaseEvent,
    key: String,
    user_key: String,
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
}

pub type IndexEvent = BaseEvent;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct IdentifyEvent {
    #[serde(flatten)]
    base: BaseEvent,
    key: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AliasEvent {
    #[serde(flatten)]
    base: BaseEvent,
    key: String,
    context_kind: ContextKind,
    previous_key: String,
    previous_context_kind: ContextKind,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomEvent {
    #[serde(flatten)]
    base: BaseEvent,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metric_value: Option<f64>,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    data: serde_json::Value,
    #[serde(skip_serializing_if = "ContextKind::is_user")]
    context_kind: ContextKind,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind")]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    #[serde(rename = "feature")]
    FeatureRequest(FeatureRequestEvent),
    #[serde(rename = "index")]
    Index(IndexEvent),
    #[serde(rename = "identify")]
    Identify(IdentifyEvent),
    #[serde(rename = "alias")]
    Alias(AliasEvent),
    #[serde(rename = "custom")]
    Custom(CustomEvent),
    #[serde(rename = "summary")]
    Summary(EventSummary),
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let json = serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("JSON serialization failed ({}): {:?}", e, self));
        write!(f, "{}", json)
    }
}

pub struct EventFactory {
    send_reason: bool,
    inline_users_in_events: bool,
}

impl EventFactory {
    pub fn new(send_reason: bool, inline_users_in_events: bool) -> Self {
        Self {
            send_reason,
            inline_users_in_events,
        }
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
    ) -> Event {
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
    ) -> Event {
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
    ) -> Event {
        // TODO(ch108604) Events created during prereq evaluation might not have a value set (e.g.
        // flag is off and the off variation is None). In those situations, we are going to default
        // to a JSON null until we can better sort out the Detail struct.
        let value = detail
            .value
            .unwrap_or(FlagValue::Json(serde_json::Value::Null));

        let flag_track_events;
        let require_experiment_data;

        if let Some(f) = flag {
            flag_track_events = f.track_events;
            require_experiment_data = f.is_experimentation_enabled(&detail.reason);
        } else {
            flag_track_events = false;
            require_experiment_data = false;
        }

        let reason = if self.send_reason || require_experiment_data {
            Some(detail.reason)
        } else {
            None
        };

        Event::FeatureRequest(FeatureRequestEvent {
            user_key: user.key().to_string(),
            base: BaseEvent {
                creation_date: Self::now(),
                user: MaybeInlinedUser::new(self.inline_users_in_events, user.clone()),
            },
            key: flag_key.to_owned(),
            default,
            reason,
            value,
            variation: detail.variation_index,
            version: flag.map(|f| f.version),
            prereq_of,
            context_kind: user.into(),
            track_events: flag_track_events || require_experiment_data,
        })
    }

    pub fn new_identify(&self, user: User) -> Event {
        Event::Identify(IdentifyEvent {
            key: user.key().to_string(),
            base: BaseEvent {
                creation_date: Self::now(),
                user: MaybeInlinedUser::new(true, user),
            },
        })
    }

    pub fn new_alias(&self, user: User, previous_user: User) -> Event {
        Event::Alias(AliasEvent {
            base: BaseEvent {
                creation_date: Self::now(),
                user: MaybeInlinedUser::new(false, user.clone()),
            },
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
    ) -> serde_json::Result<Event> {
        let data = serde_json::to_value(data)?;

        Ok(Event::Custom(CustomEvent {
            base: BaseEvent {
                creation_date: Self::now(),
                user: MaybeInlinedUser::new(self.inline_users_in_events, user.clone()),
            },
            key: key.into(),
            metric_value,
            data,
            context_kind: user.into(),
        }))
    }
}

impl Event {
    pub fn to_index_event(&self) -> Option<IndexEvent> {
        let base = match self {
            Event::FeatureRequest(FeatureRequestEvent { base, .. }) => base,
            Event::Custom(CustomEvent { base, .. }) => base,
            Event::Alias { .. }
            | Event::Index { .. }
            | Event::Identify { .. }
            | Event::Summary { .. } => return None,
        };

        // difficult to avoid clone here because we can't express that we're not "really"
        // borrowing base.clone().user
        let mut base = base.clone();
        base.user = base.user.force_inlined();
        Some(base)
    }

    #[cfg(test)]
    pub fn kind(&self) -> &'static str {
        match self {
            Event::FeatureRequest { .. } => "feature",
            Event::Alias { .. } => "alias",
            Event::Index { .. } => "index",
            Event::Identify { .. } => "identify",
            Event::Custom { .. } => "custom",
            Event::Summary { .. } => "summary",
        }
    }

    #[cfg(test)]
    pub fn base_mut(&mut self) -> Option<&mut BaseEvent> {
        Some(match self {
            Event::FeatureRequest(FeatureRequestEvent { base, .. }) => base,
            Event::Alias(AliasEvent { base, .. }) => base,
            Event::Index(base) => base,
            Event::Identify(IdentifyEvent { base, .. }) => base,
            Event::Custom(CustomEvent { base, .. }) => base,
            Event::Summary(_) => return None,
        })
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
    use maplit::hashmap;
    use spectral::prelude::*;

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

        let event_factory = EventFactory::new(true, true);
        let eval_event = event_factory.new_eval_event(
            &flag.key.clone(),
            user.build(),
            &flag,
            fallthrough.clone(),
            default.clone(),
            None,
        );

        if let Event::FeatureRequest(event) = eval_event {
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

        let event_factory = EventFactory::new(true, true);
        let event = event_factory.new_alias(user.clone(), previous_user.clone());

        if let Event::Alias(alias) = event {
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

        let event_factory = EventFactory::new(true, true);
        let custom_event = event_factory.new_custom(user.build(), &flag.key.clone(), None, "");

        if let Ok(Event::Custom(event)) = custom_event {
            assert_eq!(event.context_kind, context_kind);
        } else {
            panic!("new_custom did not create a Custom event");
        }
    }

    #[test]
    fn serializes_eval_event() {
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

        let event_factory = EventFactory::new(true, true);
        let mut eval_event = event_factory.new_eval_event(
            &flag.key.clone(),
            user,
            &flag,
            fallthrough.clone(),
            default.clone(),
            None,
        );
        // fix creation date so JSON is predictable
        eval_event.base_mut().unwrap().creation_date = 1234;

        let eval_event_json = r#"
{
  "kind": "feature",
  "creationDate": 1234,
  "user": {
    "key": "alice",
    "anonymous": true,
    "custom": {}
  },
  "key": "flag",
  "userKey": "alice",
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

        assert_that!(serde_json::to_string_pretty(&eval_event))
            .is_ok_containing(eval_event_json.to_string());
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
        let summary_event = Event::Summary(summary);

        let summary_json = r#"
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

        assert_that!(serde_json::to_string_pretty(&summary_event))
            .is_ok_containing(summary_json.to_string());
    }

    #[test]
    fn summarises_feature_request() {
        let mut summary = EventSummary::new();
        assert_that!(summary.is_empty()).is_true();
        assert_that!(summary.start_date).is_greater_than(summary.end_date);

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
            base: BaseEvent {
                user: MaybeInlinedUser::Inlined(user.clone()),
                creation_date: eval_at,
            },
            key: flag.key.clone(),
            user_key: user.key().to_string(),
            value: value.clone(),
            variation: Some(variation_index),
            default: default.clone(),
            version: Some(flag.version),
            reason: Some(reason),
            prereq_of: None,
            context_kind: user.into(),
            track_events: false,
        };

        summary.add(&fallthrough_request);
        assert_that!(summary.is_empty()).is_false();
        assert_that!(summary.start_date).is_equal_to(eval_at);
        assert_that!(summary.end_date).is_equal_to(eval_at);

        let fallthrough_key = VariationKey {
            flag_key: flag.key,
            version: Some(flag.version),
            variation: Some(variation_index),
        };

        let fallthrough_summary = summary.features.get(&fallthrough_key);
        asserting!("counts the eval for the flag key, version and variation")
            .that(&fallthrough_summary)
            .contains_value(&VariationSummary {
                count: 1,
                value,
                default,
            });

        summary.add(&fallthrough_request);
        let fallthrough_summary = summary.features.get(&fallthrough_key).unwrap();
        asserting!("another request for same variation increments the count")
            .that(&fallthrough_summary.count)
            .is_equal_to(2);
    }

    #[test]
    fn event_factory_unknown_flags_do_not_track_events() {
        let event_factory = EventFactory::new(true, true);
        let user = User::with_key("bob").build();
        let detail = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Off,
        };
        let event =
            event_factory.new_unknown_flag_event("myFlag", user, detail, FlagValue::Bool(true));

        if let Event::FeatureRequest(event) = event {
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
        let event_factory = EventFactory::new(event_factory_send_events, true);
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

        if let Event::FeatureRequest(event) = event {
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
        let event_factory = EventFactory::new(event_factory_send_events, true);
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

        if let Event::FeatureRequest(event) = event {
            assert_eq!(event.track_events, should_events_be_tracked);
            assert_eq!(event.reason.is_some(), should_include_reason);
        } else {
            panic!("Event should be a feature request type");
        }
    }
}
