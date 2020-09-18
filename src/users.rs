use std::collections::HashMap;

use chrono::{self, TimeZone, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha1::Sha1;

const USER_CUSTOM_STARTING_CAPACITY: usize = 10;
const BUCKET_SCALE_INT: i64 = 0x0FFF_FFFF_FFFF_FFFF;
const BUCKET_SCALE: f32 = BUCKET_SCALE_INT as f32;

lazy_static! {
    static ref VERSION_NUMERIC_COMPONENTS_REGEX: Regex =
        Regex::new(r"^\d+(\.\d+)?(\.\d+)?").unwrap();
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum AttributeValue {
    String(String),
    Array(Vec<AttributeValue>),
    Int(i64),
    Float(f64),
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

impl From<i64> for AttributeValue {
    fn from(i: i64) -> AttributeValue {
        AttributeValue::Int(i)
    }
}

impl From<f64> for AttributeValue {
    fn from(f: f64) -> Self {
        AttributeValue::Float(f)
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
            AttributeValue::Float(f) => Some(*f),
            AttributeValue::Int(i) => Some(*i as f64),
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
            AttributeValue::Int(millis) => Some(Utc.timestamp_millis(*millis)),
            AttributeValue::Float(millis) => {
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

    /// as_semver will attempt to parse a string attribute into a semver version.
    /// It will return None if it cannot parse it, or for non-string attributes.
    pub fn as_semver(&self) -> Option<semver::Version> {
        let version_str = self.as_str()?.as_str();
        semver::Version::parse(version_str)
            .ok()
            .or_else(|| AttributeValue::parse_semver_loose(version_str))
    }

    fn parse_semver_loose(version_str: &str) -> Option<semver::Version> {
        let parts = VERSION_NUMERIC_COMPONENTS_REGEX.captures(version_str)?;

        let numeric_parts = parts.get(0).unwrap();
        let mut transformed_version_str = numeric_parts.as_str().to_string();

        for i in 1..parts.len() {
            if let None = parts.get(i) {
                transformed_version_str.push_str(".0");
            }
        }

        let rest = &version_str[numeric_parts.end()..];
        transformed_version_str.push_str(rest);

        semver::Version::parse(&transformed_version_str).ok()
    }

    pub fn find<P>(&self, p: P) -> Option<&AttributeValue>
    where
        P: Fn(&AttributeValue) -> bool,
    {
        match self {
            AttributeValue::String(_)
            | AttributeValue::Int(_)
            | AttributeValue::Float(_)
            | AttributeValue::Bool(_) => {
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

    fn as_bucketable(&self) -> Option<String> {
        match self {
            AttributeValue::String(s) => Some(s.clone()),
            AttributeValue::Int(i) => Some(i.to_string()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
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
    pub fn with_key(key: impl Into<String>) -> UserBuilder {
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

    pub fn attribute<T: Into<AttributeValue>>(&mut self, key: &str, value: T) {
        // TODO handle non-custom too
        self.custom.insert(key.to_string(), value.into());
    }

    pub fn bucket(&self, bucketing_key: &str, by_attr: Option<&str>, salt: &str) -> f32 {
        let attr_value = match by_attr {
            Some(attr) => self.value_of(attr),
            None => self._key.as_ref(),
        };
        self._bucket(bucketing_key, attr_value, salt).unwrap_or(0.0)
    }

    fn _bucket(
        &self,
        bucketing_key: &str,
        attr_value: Option<&AttributeValue>,
        salt: &str,
    ) -> Option<f32> {
        let mut id = attr_value?.as_bucketable()?;

        if let Some(secondary) = self.secondary() {
            id.push('.');
            id.push_str(secondary);
        }

        let mut hash = Sha1::new();
        hash.update(bucketing_key.as_bytes());
        hash.update(b".");
        hash.update(salt.as_bytes());
        hash.update(b".");
        hash.update(id.as_bytes());
        let hexhash = hash.hexdigest();

        let hexhash_15 = &hexhash[..15]; // yes, 15 chars, not 16
        let numhash = i64::from_str_radix(hexhash_15, 16).unwrap();

        Some(numhash as f32 / BUCKET_SCALE)
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
    pub fn new(key: impl Into<String>) -> Self {
        Self::new_with_optional_key(Some(key.into()))
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

#[cfg(test)]
mod tests {
    use super::*;

    use spectral::prelude::*;

    const BUCKET_TOLERANCE: f32 = 0.0000001;

    #[test]
    fn bucket_user_by_key() {
        let user = User::with_key("userKeyA").build();
        let bucket = user.bucket("hashKey", None, "saltyA");
        assert_that!(bucket).is_close_to(0.42157587, BUCKET_TOLERANCE);

        let user = User::with_key("userKeyB").build();
        let bucket = user.bucket("hashKey", None, "saltyA");
        assert_that!(bucket).is_close_to(0.6708485, BUCKET_TOLERANCE);

        let user = User::with_key("userKeyC").build();
        let bucket = user.bucket("hashKey", None, "saltyA");
        assert_that!(bucket).is_close_to(0.10343106, BUCKET_TOLERANCE);
    }

    #[test]
    fn bucket_user_by_int_attr() {
        const USER_KEY: &str = "userKeyD";

        let custom = hashmap! {
            "intAttr".into() => 33333.into(),
        };
        let user = User::with_key(USER_KEY).custom(custom).build();
        let bucket = user.bucket("hashKey", Some("intAttr"), "saltyA");
        assert_that!(bucket).is_close_to(0.54771423, BUCKET_TOLERANCE);

        let custom = hashmap! {
        "stringAttr".into() => "33333".into(),
        };
        let user = User::with_key(USER_KEY).custom(custom).build();
        let bucket2 = user.bucket("hashKey", Some("stringAttr"), "saltyA");
        assert_that!(bucket).is_close_to(bucket2, BUCKET_TOLERANCE);
    }

    #[test]
    fn bucket_user_by_float_attr_not_allowed() {
        const USER_KEY: &str = "userKeyE";
        let custom = hashmap! {
            "floatAttr".into() => 999.999.into(),
        };
        let user = User::with_key(USER_KEY).custom(custom).build();
        let bucket = user.bucket("hashKey", Some("floatAttr"), "saltyA");
        assert_that!(bucket).is_close_to(0.0, BUCKET_TOLERANCE);
    }
}
