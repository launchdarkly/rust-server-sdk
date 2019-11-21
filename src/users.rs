use std::collections::HashMap;

use chrono::{self, TimeZone, Utc};
use serde::{Deserialize, Serialize};

const USER_CUSTOM_STARTING_CAPACITY: usize = 10;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Array(Vec<AttributeValue>),
    Number(f64),
    Bool(bool),
    Null,
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

impl From<bool> for AttributeValue {
    fn from(b: bool) -> AttributeValue {
        AttributeValue::Bool(b)
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
            _ => None,
        }
    }

    /// to_f64 will return self if it is a float, otherwise convert it if possible, or else return
    /// None.
    pub fn to_f64(&self) -> Option<f64> {
        match self {
            AttributeValue::Number(f) => Some(*f),
            AttributeValue::String(s) => s.parse().ok(),
            AttributeValue::Bool(_) => None, // TODO check this
            AttributeValue::Null => None,
            other => {
                warn!(
                    "Don't know how or whether to convert attribute value {:?} to float",
                    other
                );
                None
            }
        }
    }

    /// as_bool returns None unless self is a bool. It will not convert.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            AttributeValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// to_datetime will attempt to convert any of the following into a chrono::DateTime in UTC:
    ///  * RFC3339/ISO8601 timestamp (example: "2016-04-16T17:09:12.759-07:00")
    ///  * Unix epoch milliseconds as string
    ///  * Unix milliseconds as number
    /// It will return None if the conversion fails or if no conversion is possible.
    pub fn to_datetime(&self) -> Option<chrono::DateTime<Utc>> {
        match self {
            AttributeValue::Number(millis) => {
                // TODO this has undefined behaviour for huge floats: https://stackoverflow.com/a/41139453
                Some(Utc.timestamp_nanos((millis * 1e6).round() as i64))
            }
            AttributeValue::String(s) => {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                    Some(dt.with_timezone(&Utc))
                } else if let Ok(millis) = s.parse() {
                    Utc.timestamp_millis_opt(millis).single()
                } else {
                    None
                }
            }
            AttributeValue::Bool(_) | AttributeValue::Null => None,
            other => {
                warn!(
                    "Don't know how or whether to convert attribute value {:?} to datetime",
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
            AttributeValue::String(_) | AttributeValue::Number(_) | AttributeValue::Bool(_) => {
                if p(self) {
                    Some(&self)
                } else {
                    None
                }
            }
            AttributeValue::Array(values) => values.iter().find(|v| p(v)),
            AttributeValue::Null => None,
            AttributeValue::NotYetImplemented(_) => {
                warn!("attribute type not implemented: {:?}", self);
                None
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct User {
    #[serde(rename = "key", skip_serializing_if = "Option::is_none")]
    _key: Option<AttributeValue>,
    #[serde(rename = "secondary", skip_serializing_if = "Option::is_none")]
    _secondary: Option<AttributeValue>,
    #[serde(rename = "ip", skip_serializing_if = "Option::is_none")]
    _ip: Option<AttributeValue>,
    #[serde(rename = "country", skip_serializing_if = "Option::is_none")]
    _country: Option<AttributeValue>,
    #[serde(rename = "email", skip_serializing_if = "Option::is_none")]
    _email: Option<AttributeValue>,
    #[serde(rename = "firstName", skip_serializing_if = "Option::is_none")]
    _first_name: Option<AttributeValue>,
    #[serde(rename = "lastName", skip_serializing_if = "Option::is_none")]
    _last_name: Option<AttributeValue>,
    #[serde(rename = "avatar", skip_serializing_if = "Option::is_none")]
    _avatar: Option<AttributeValue>,
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    _name: Option<AttributeValue>,
    #[serde(rename = "anonymous", skip_serializing_if = "Option::is_none")]
    _anonymous: Option<AttributeValue>,

    custom: HashMap<String, AttributeValue>,
}

impl User {
    pub fn new(key: String) -> UserBuilder {
        UserBuilder::new(key)
    }

    pub fn key(&self) -> Option<&String> {
        self._key.as_ref().and_then(|av| av.as_str())
    }
    pub fn secondary(&self) -> Option<&String> {
        self._secondary.as_ref().and_then(|av| av.as_str())
    }
    pub fn ip(&self) -> Option<&String> {
        self._ip.as_ref().and_then(|av| av.as_str())
    }
    pub fn country(&self) -> Option<&String> {
        self._country.as_ref().and_then(|av| av.as_str())
    }
    pub fn email(&self) -> Option<&String> {
        self._email.as_ref().and_then(|av| av.as_str())
    }
    pub fn first_name(&self) -> Option<&String> {
        self._first_name.as_ref().and_then(|av| av.as_str())
    }
    pub fn last_name(&self) -> Option<&String> {
        self._last_name.as_ref().and_then(|av| av.as_str())
    }
    pub fn avatar(&self) -> Option<&String> {
        self._avatar.as_ref().and_then(|av| av.as_str())
    }
    pub fn name(&self) -> Option<&String> {
        self._name.as_ref().and_then(|av| av.as_str())
    }
    pub fn anonymous(&self) -> Option<bool> {
        self._anonymous.as_ref().and_then(|av| av.as_bool())
    }

    pub fn value_of(&self, attr: &str) -> Option<&AttributeValue> {
        match attr {
            "key" => self._key.as_ref(),
            "secondary" => self._secondary.as_ref(),
            "ip" => self._ip.as_ref(),
            "country" => self._country.as_ref(),
            "email" => self._email.as_ref(),
            "firstName" => self._first_name.as_ref(),
            "lastName" => self._last_name.as_ref(),
            "avatar" => self._avatar.as_ref(),
            "name" => self._name.as_ref(),
            "anonymous" => self._anonymous.as_ref(),
            _ => self.custom.get(attr),
        }
    }
}

pub struct UserBuilder {
    key: Option<String>,
    secondary: Option<String>,
    ip: Option<String>,
    country: Option<String>,
    email: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    avatar: Option<String>,
    name: Option<String>,
    anonymous: Option<bool>,
    custom: HashMap<String, AttributeValue>,
}

impl UserBuilder {
    pub fn new(key: String) -> Self {
        Self::new_with_optional_key(Some(key))
    }

    pub fn new_with_optional_key(key: Option<String>) -> Self {
        Self {
            key,
            secondary: None,
            ip: None,
            country: None,
            email: None,
            first_name: None,
            last_name: None,
            avatar: None,
            name: None,
            anonymous: None,
            custom: HashMap::with_capacity(USER_CUSTOM_STARTING_CAPACITY),
        }
    }

    pub fn secondary(&mut self, secondary: String) -> &Self {
        self.secondary = Some(secondary);
        self
    }
    pub fn ip(&mut self, ip: String) -> &Self {
        self.ip = Some(ip);
        self
    }
    pub fn country(&mut self, country: String) -> &Self {
        self.country = Some(country);
        self
    }

    pub fn email(&mut self, email: String) -> &Self {
        self.email = Some(email);
        self
    }

    pub fn first_name(&mut self, first_name: String) -> &Self {
        self.first_name = Some(first_name);
        self
    }
    pub fn last_name(&mut self, last_name: String) -> &Self {
        self.last_name = Some(last_name);
        self
    }
    pub fn avatar(&mut self, avatar: String) -> &Self {
        self.avatar = Some(avatar);
        self
    }

    pub fn name(&mut self, name: String) -> &Self {
        self.name = Some(name);
        self
    }

    pub fn anonymous(&mut self, anonymous: bool) -> &Self {
        self.anonymous = Some(anonymous);
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
            _secondary: self.secondary.clone().map(AString),
            _ip: self.ip.clone().map(AString),
            _country: self.country.clone().map(AString),
            _email: self.email.clone().map(AString),
            _first_name: self.first_name.clone().map(AString),
            _last_name: self.last_name.clone().map(AString),
            _avatar: self.avatar.clone().map(AString),
            _name: self.name.clone().map(AString),
            _anonymous: self.anonymous.map(AttributeValue::Bool),
            custom: self.custom.clone(),
        }
    }
}
