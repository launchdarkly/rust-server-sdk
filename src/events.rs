use std::fmt::{self, Display, Formatter};

use serde::Serialize;

use super::store::FlagValue;
use super::users::User;

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
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
        default: FlagValue,
        version: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        prereq_of: Option<String>,
    },
    #[serde(rename = "index", rename_all = "camelCase")]
    Index {
        #[serde(flatten)]
        base: BaseEvent,
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
    pub fn make_index_event(&self) -> Option<Event> {
        match self {
            Event::FeatureRequest { base, .. } => {
                // difficult to avoid clone here because we can't express that we're not "really"
                // borrowing base.clone().user
                let mut base = base.clone();
                base.user = base.user.force_inlined();
                Some(Event::Index { base })
            }
            Event::Index { .. } => None,
        }
    }
}
