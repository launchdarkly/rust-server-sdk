use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Array(Vec<AttributeValue>),
    Number(f64),
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
    /// as_str returns None unless self is a String. It will not convert.
    pub fn as_str(&self) -> Option<&String> {
        match self {
            AttributeValue::String(s) => Some(s),
            AttributeValue::Number(_) => None,
            other => {
                warn!(
                    "Don't know how or whether to stringify attribute value {:?}",
                    other
                );
                None
            }
        }
    }

    /// to_f64 will return self if it is a float, otherwise convert it if possible, or else return
    /// None.
    pub fn to_f64(&self) -> Option<f64> {
        match self {
            AttributeValue::Number(f) => Some(*f),
            AttributeValue::String(s) => s.parse().ok(),
            other => {
                warn!(
                    "Don't know how or whether to convert attribute value {:?} to float",
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
            AttributeValue::String(_) | AttributeValue::Number(_) => {
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
    _key: Option<AttributeValue>,
    // TODO support other non-custom attributes

    custom: HashMap<String, AttributeValue>,
}

impl User {
    pub fn new(key: &str) -> User {
        Self::new_with_custom(key, HashMap::with_capacity(0))
    }

    pub fn new_with_custom(key: &str, custom: HashMap<String, AttributeValue>) -> User {
        User {
            _key: Some(key.into()),
            custom: custom,
        }
    }

    pub fn new_without_key() -> User {
        User {
            _key: None,
            custom: HashMap::default(),
        }
    }

    pub fn key(&self) -> Option<&String> {
        self._key.as_ref().and_then(|av| av.as_str())
    }

    pub fn value_of<Q>(&self, attr: &Q) -> Option<&AttributeValue>
    where
        String: Borrow<Q>,
        Q: Hash + Eq + PartialEq<str>,
    {
        if attr == "key" {
            return self._key.as_ref();
        }
        // TODO handle other non-custom attributes
        self.custom.get(attr)
    }
}
