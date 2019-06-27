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
pub enum Event {
    #[serde(rename = "feature", rename_all = "camelCase")]
    FeatureRequest {
        #[serde(flatten)]
        base: BaseEvent,
        key: String,
        user_key: String,
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
        user: User,
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
            Event::FeatureRequest { base, .. } => Some(Event::Index {
                base: base.clone(),      // TODO avoid clone
                user: base.user.clone(), // TODO avoid clone
            }),
            Event::Index { .. } => None,
        }
    }
}
