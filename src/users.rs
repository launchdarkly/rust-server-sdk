use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Array(Vec<AttributeValue>),
    // TODO implement other attribute types
    NotYetImplemented(serde_json::Value),
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> AttributeValue {
        AttributeValue::String(s.to_owned())
    }
}

impl<T> From<Vec<T>> for AttributeValue
where
    AttributeValue: From<T>,
{
    fn from(v: Vec<T>) -> AttributeValue {
        AttributeValue::Array(v.into_iter().map(|i| i.into()).collect())
    }
}

impl AttributeValue {
    pub fn as_str(&self) -> Option<&String> {
        match self {
            AttributeValue::String(s) => Some(s),
            other => {
                warn!(
                    "Don't know how or whether to stringify attribute value {:?}",
                    other
                );
                None
            }
        }
    }

    pub fn find<P>(&self, p: P) -> Option<&AttributeValue>
    where
        P: Fn(&AttributeValue) -> bool,
    {
        match self {
            AttributeValue::String(_) => {
                if p(self) {
                    Some(&self)
                } else {
                    None
                }
            }
            AttributeValue::Array(values) => values.iter().find(|v| p(v)),
            AttributeValue::NotYetImplemented(_) => {
                warn!("attribute type not implemented: {:?}", self);
                None
            }
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct User {
    pub key: Option<String>,

    custom: HashMap<String, AttributeValue>,
}

impl User {
    pub fn new<S: Into<String>>(key: S) -> User {
        Self::new_with_custom(key, HashMap::with_capacity(0))
    }

    pub fn new_with_custom<S: Into<String>>(
        key: S,
        custom: HashMap<String, AttributeValue>,
    ) -> User {
        User {
            key: Some(key.into()),
            custom: custom,
        }
    }

    pub fn new_without_key() -> User {
        User {
            key: None,
            custom: HashMap::default(),
        }
    }

    pub fn value_of<Q>(&self, attr: &Q) -> Option<&AttributeValue>
    where
        String: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.custom.get(attr)
    }
}
