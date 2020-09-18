use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use serde::Serialize;

use super::eval::{Detail, Reason, VariationIndex};
use super::store::{FeatureFlag, FlagValue};
use super::users::User;

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

    pub fn key(&self) -> Option<&String> {
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

pub type IndexEvent = BaseEvent;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct IdentifyEvent {
    #[serde(flatten)]
    base: BaseEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind")]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    #[serde(rename = "feature", rename_all = "camelCase")]
    FeatureRequest {
        #[serde(flatten)]
        base: BaseEvent,
        key: String,
        user_key: Option<String>,
        value: FlagValue,
        variation: Option<VariationIndex>,
        default: FlagValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<Reason>,
        version: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        prereq_of: Option<String>,
    },
    #[serde(rename = "index", rename_all = "camelCase")]
    Index(IndexEvent),
    #[serde(rename = "identify", rename_all = "camelCase")]
    Identify(IdentifyEvent),
    #[serde(rename = "summary", rename_all = "camelCase")]
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
    pub fn new_feature_request(
        flag_key: &str,
        user: MaybeInlinedUser,
        flag: Option<FeatureFlag>,
        detail: Detail<FlagValue>,
        default: FlagValue,
        send_reason: bool,
    ) -> Self {
        let user_key = user.key().cloned();

        // unwrap is safe here because value should have been replaced with default if it was None.
        // TODO that is ugly, use the type system to fix it
        let value = detail.value.unwrap();

        let reason = if send_reason {
            Some(detail.reason)
        } else {
            None
        };

        Event::FeatureRequest {
            base: BaseEvent {
                creation_date: Self::now(),
                user,
            },
            user_key,
            key: flag_key.to_owned(),
            default,
            reason,
            value,
            variation: detail.variation_index,
            version: flag.map(|f| f.version),
            prereq_of: None,
        }
    }

    pub fn new_identify(user: User) -> Self {
        let key = user.key().cloned();
        Event::Identify(IdentifyEvent {
            base: BaseEvent {
                creation_date: Self::now(),
                user: MaybeInlinedUser::new(true, user),
            },
            key,
        })
    }

    pub fn to_index_event(&self) -> Option<IndexEvent> {
        let base = match self {
            Event::FeatureRequest { base, .. } => base,
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
            Event::Summary { .. } => "summary",
        }
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

    pub fn add(&mut self, event: &Event) {
        if let Event::FeatureRequest {
            base: BaseEvent { creation_date, .. },
            key,
            value,
            version,
            variation,
            default,
            ..
        } = event
        {
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
        let mut counters = Vec::new();
        counters.push(VariationCounterOutput::from((
            variation_key,
            variation_summary.value,
            variation_summary.count,
        )));

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
    use spectral::prelude::*;

    use super::*;

    #[test]
    fn summarises_feature_request() {
        let mut summary = EventSummary::new();
        assert_that!(summary.is_empty()).is_true();
        assert_that!(summary.start_date).is_greater_than(summary.end_date);

        let flag = FeatureFlag::basic_flag("flag");
        let default = FlagValue::from(false);
        let user = MaybeInlinedUser::Inlined(User::with_key("alice".to_string()).build());
        let fallthrough = Detail {
            value: Some(FlagValue::from(false)),
            variation_index: Some(1),
            reason: Reason::Fallthrough,
        };

        let fallthrough_request = Event::new_feature_request(
            &flag.key.clone(),
            user.clone(),
            Some(flag.clone()),
            fallthrough.clone(),
            default.clone(),
            true,
        );

        summary.add(
            &fallthrough_request
                .to_index_event()
                .map(Event::Index)
                .unwrap(),
        );

        asserting!("ignores index events")
            .that(&summary.is_empty())
            .is_true();

        summary.add(&fallthrough_request);
        assert_that!(summary.is_empty()).is_false();
        assert_that!(summary.start_date).is_equal_to(summary.end_date);

        let fallthrough_key = VariationKey {
            flag_key: flag.key,
            version: Some(flag.version),
            variation: fallthrough.variation_index,
        };

        let fallthrough_summary = summary.features.get(&fallthrough_key);
        asserting!("counts the eval for the flag key, version and variation")
            .that(&fallthrough_summary)
            .contains_value(&VariationSummary {
                count: 1,
                value: fallthrough.value.unwrap(),
                default,
            });

        summary.add(&fallthrough_request);
        let fallthrough_summary = summary.features.get(&fallthrough_key).unwrap();
        asserting!("another request for same variation increments the count")
            .that(&fallthrough_summary.count)
            .is_equal_to(2);
    }
}
