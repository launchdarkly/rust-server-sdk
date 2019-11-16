use std::fmt::{self, Display, Formatter};

use serde::Serialize;

use super::store::FlagValue;
use super::users::User;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BaseEvent {
    pub creation_date: u64,
    #[serde(skip_serializing)]
    pub user: User,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind")]
pub enum Event<'a> {
    #[serde(rename = "feature", rename_all = "camelCase")]
    FeatureRequest {
        #[serde(flatten)]
        base: BaseEvent,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        user: Option<User>,
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
        user: &'a User,
    },
}

impl<'a> Display for Event<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let json = serde_json::to_string_pretty(self)
            .unwrap_or_else(|e| format!("JSON serialization failed ({}): {:?}", e, self));
        write!(f, "{}", json)
    }
}

impl<'a> Event<'a> {
    pub fn make_index_event(&self) -> Option<Event> {
        match self {
            Event::FeatureRequest { base, .. } => Some(Event::Index {
                // difficult to avoid clone here because we can't express internal references
                base: base.clone(),
                user: &base.user,
            }),
            Event::Index { .. } => None,
        }
    }
}
