use std::collections::HashMap;

use serde::{Deserialize, Serialize};

const USER_CUSTOM_STARTING_CAPACITY: usize = 10;

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

impl From<String> for AttributeValue {
    fn from(s: String) -> AttributeValue {
        AttributeValue::String(s)
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
    // TODO support other non-custom attributes
    _key: Option<AttributeValue>,
    _name: Option<AttributeValue>,
    _email: Option<AttributeValue>,

    custom: HashMap<String, AttributeValue>,
}

impl User {
    pub fn new(key: String) -> UserBuilder {
        UserBuilder::new(key)
    }

    pub fn key(&self) -> Option<&String> {
        self._key.as_ref().and_then(|av| av.as_str())
    }
    pub fn name(&self) -> Option<&String> {
        self._name.as_ref().and_then(|av| av.as_str())
    }
    pub fn email(&self) -> Option<&String> {
        self._email.as_ref().and_then(|av| av.as_str())
    }
    // TODO expose other non-custom attributes

    pub fn value_of(&self, attr: &str) -> Option<&AttributeValue> {
        match attr {
            "key" => self._key.as_ref(),
            "name" => self._name.as_ref(),
            "email" => self._email.as_ref(),
            // TODO handle other non-custom attributes
            _ => self.custom.get(attr),
        }
    }
}

pub struct UserBuilder {
    key: Option<String>,
    name: Option<String>,
    email: Option<String>,
    custom: HashMap<String, AttributeValue>,
}

impl UserBuilder {
    pub fn new(key: String) -> Self {
        Self::new_with_optional_key(Some(key))
    }

    pub fn new_with_optional_key(key: Option<String>) -> Self {
        Self {
            key,
            name: None,
            email: None,
            custom: HashMap::with_capacity(USER_CUSTOM_STARTING_CAPACITY),
        }
    }

    pub fn name(&mut self, name: String) -> &Self {
        self.name = Some(name);
        self
    }

    pub fn email(&mut self, email: String) -> &Self {
        self.email = Some(email);
        self
    }

    pub fn custom(&mut self, custom: HashMap<String, AttributeValue>) -> &Self {
        self.custom.extend(custom);
        self
    }

    pub fn build(&self) -> User {
        use AttributeValue::String as AString;
        User {
            _key: self.key.clone().map(AString),
            _name: self.name.clone().map(AString),
            _email: self.email.clone().map(AString),
            custom: self.custom.clone(),
        }
    }
}
