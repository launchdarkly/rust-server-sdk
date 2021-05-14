use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use rust_server_sdk_evaluation::{Detail, Flag, FlagValue, Reason, User, VariationIndex};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
/// a user that may be inlined in the event. TODO have the event processor handle this
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
#[serde(tag = "kind")]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    #[serde(rename = "feature")]
    FeatureRequest(FeatureRequestEvent),
    #[serde(rename = "index")]
    Index(IndexEvent),
    #[serde(rename = "identify")]
    Identify(IdentifyEvent),
    #[serde(rename = "custom", rename_all = "camelCase")]
    Custom {
        #[serde(flatten)]
        base: BaseEvent,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        metric_value: Option<f64>,
        #[serde(skip_serializing_if = "serde_json::Value::is_null")]
        data: serde_json::Value,
    },
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

impl Event {
    // TODO separate out new_unknown_flag_event so flag doesn't have to be Option
    pub fn new_feature_request(
        flag_key: &str,
        user: MaybeInlinedUser,
        flag: Option<Flag>,
        detail: Detail<FlagValue>,
        default: FlagValue,
        send_reason: bool,
    ) -> Self {
        // unwrap is safe here because value should have been replaced with default if it was None.
        // TODO that is ugly, use the type system to fix it
        let value = detail.value.unwrap();

        let flag_track_events;
        let require_experiment_data;
        if let Some(f) = flag.as_ref() {
            flag_track_events = f.track_events;
            require_experiment_data = f.is_experimentation_enabled(&detail.reason);
        } else {
            flag_track_events = false;
            require_experiment_data = false;
        }

        let reason = if send_reason || require_experiment_data {
            Some(detail.reason)
        } else {
            None
        };

        Event::FeatureRequest(FeatureRequestEvent {
            user_key: user.key().to_string(),
            base: BaseEvent {
                creation_date: Self::now(),
                user,
            },
            key: flag_key.to_owned(),
            default,
            reason,
            value,
            variation: detail.variation_index,
            version: flag.map(|f| f.version),
            prereq_of: None,
            track_events: flag_track_events || require_experiment_data,
        })
    }

    pub fn new_identify(user: User) -> Self {
        Event::Identify(IdentifyEvent {
            key: user.key().to_string(),
            base: BaseEvent {
                creation_date: Self::now(),
                user: MaybeInlinedUser::new(true, user),
            },
        })
    }

    pub fn new_custom(
        user: MaybeInlinedUser,
        key: impl Into<String>,
        metric_value: Option<f64>,
        data: impl Serialize,
    ) -> serde_json::Result<Self> {
        let data = serde_json::to_value(data)?;

        Ok(Event::Custom {
            base: BaseEvent {
                creation_date: Self::now(),
                user,
            },
            key: key.into(),
            metric_value,
            data,
        })
    }

    pub fn to_index_event(&self) -> Option<IndexEvent> {
        let base = match self {
            Event::FeatureRequest(FeatureRequestEvent { base, .. }) => base,
            Event::Custom { base, .. } => base,
            Event::Index { .. } | Event::Identify { .. } | Event::Summary { .. } => return None,
        };

        // difficult to avoid clone here because we can't express that we're not "really"
        // borrowing base.clone().user
        let mut base = base.clone();
        base.user = base.user.force_inlined();
        Some(base)
    }

    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[cfg(test)]
    pub fn kind(&self) -> &'static str {
        match self {
            Event::FeatureRequest { .. } => "feature",
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
            Event::Index(base) => base,
            Event::Identify(IdentifyEvent { base, .. }) => base,
            Event::Custom { base, .. } => base,
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

    #[test]
    fn serializes_feature_event() {
        let flag = basic_flag("flag");
        let default = FlagValue::from(false);
        let user = MaybeInlinedUser::Inlined(User::with_key("alice".to_string()).build());
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough,
        };

        let mut fre = Event::new_feature_request(
            &flag.key.clone(),
            user.clone(),
            Some(flag.clone()),
            fallthrough.clone(),
            default.clone(),
            true,
        );
        // fix creation date so JSON is predictable
        fre.base_mut().unwrap().creation_date = 1234;

        let fre_json = r#"
{
  "kind": "feature",
  "creationDate": 1234,
  "user": {
    "key": "alice",
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
  "version": 42
}
        "#
        .trim();

        assert_that!(serde_json::to_string_pretty(&fre)).is_ok_containing(fre_json.to_string());
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
        let user = MaybeInlinedUser::Inlined(User::with_key("alice".to_string()).build());

        let value = FlagValue::from(false);
        let variation_index = 1;
        let reason = Reason::Fallthrough;
        let eval_at = 1234;

        let fallthrough_request = FeatureRequestEvent {
            base: BaseEvent {
                user: user.clone(),
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
}
