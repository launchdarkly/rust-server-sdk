use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};

use serde::Serialize;

use super::eval::{Detail, Reason, VariationIndex};
use super::store::{FeatureFlag, FlagValue};
use super::users::User;

#[derive(Clone, Debug, Serialize)]
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

    fn key(&self) -> Option<&String> {
        self.user().key()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseEvent {
    pub creation_date: u64,
    #[serde(skip_serializing_if = "MaybeInlinedUser::not_inlined")]
    pub user: MaybeInlinedUser,
}

#[derive(Debug, Serialize)]
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
    Index {
        #[serde(flatten)]
        base: BaseEvent,
    },
    #[serde(rename = "summary", rename_all = "camelCase")]
    Summary {
        start_date: u64,
        end_date: u64,
        features: HashMap<String, FeatureSummary>,
    },
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

        let reason = match send_reason {
            false => None,
            true => Some(detail.reason),
        };

        Event::FeatureRequest {
            base: BaseEvent {
                creation_date: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
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

    pub fn make_index_event(&self) -> Option<Event> {
        match self {
            Event::FeatureRequest { base, .. } => {
                // difficult to avoid clone here because we can't express that we're not "really"
                // borrowing base.clone().user
                let mut base = base.clone();
                base.user = base.user.force_inlined();
                Some(Event::Index { base })
            }
            Event::Index { .. } | Event::Summary { .. } => None,
        }
    }

    pub fn make_singleton_summary(&self) -> Option<Event> {
        match self {
            Event::FeatureRequest {
                base: BaseEvent { creation_date, .. },
                key,
                value,
                version,
                variation,
                default,
                ..
            } => {
                let feature = FeatureSummary {
                    default: default.clone(),
                    counters: vec![VariationCounter {
                        value: value.clone(),
                        version: *version,
                        variation: *variation,
                        count: 1,
                    }],
                };
                let mut features = HashMap::with_capacity(1);
                features.insert(key.clone(), feature);
                Some(Event::Summary {
                    start_date: *creation_date,
                    end_date: *creation_date,
                    features,
                })
            }
            Event::Index { .. } | Event::Summary { .. } => None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FeatureSummary {
    pub default: FlagValue,
    pub counters: Vec<VariationCounter>,
}

#[derive(Debug, Serialize)]
pub struct VariationCounter {
    pub value: FlagValue,
    pub version: Option<u64>,
    pub count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variation: Option<VariationIndex>,
}
