use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};

use super::eval::{self, Detail, Reason};
use super::users::User;

use serde::Deserialize;

const FLAGS_PREFIX: &'static str = "/flags/";

type VariationIndex = usize;

pub enum FlagValue {
    Bool(bool),
}

impl FlagValue {
    // TODO this error handling is a mess (returns Option but panics)
    fn from_json(json: &serde_json::Value) -> Option<Self> {
        match json {
            serde_json::Value::Bool(b) => Some(FlagValue::Bool(*b)),
            serde_json::Value::String(_) => panic!("TODO string not implemented"),
            serde_json::Value::Number(_) => panic!("TODO number not implemented"),
            serde_json::Value::Object(_) => panic!("TODO object not implemented"),
            serde_json::Value::Array(_) => panic!("TODO array not implemented"),
            serde_json::Value::Null => panic!("TODO handle null"),
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FlagValue::Bool(b) => Some(*b),
        }
    }
}

// TODO more-typed flag type, to pull errors earlier
#[derive(Clone, Deserialize)]
pub struct FeatureFlag(serde_json::Value);

impl Debug for FeatureFlag {
    // implemented manually rather than derived in order to output JSON rather
    // than serde_json's type constructors
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(fmt, "{}", self.0)
    }
}

impl FeatureFlag {
    pub fn evaluate(&self, user: &User) -> Detail<FlagValue> {
        if !self.on() {
            return Detail::new(self.off_value(), Reason::Off);
        }

        for target in self.targets() {
            let values = target.get("values").expect("wtf").as_array().expect("wtf");
            for value in values {
                let value_str = value.as_str().expect("wtf");
                if value_str == &user.key {
                    let variation_index =
                        target.get("variation").expect("wtf").as_u64().expect("wtf")
                            as VariationIndex;
                    return match self.variation(variation_index) {
                        Some(result) => Detail::new(result, Reason::TargetMatch),
                        None => Detail::err(eval::Error::MalformedFlag),
                    };
                }
            }
        }

        // just return the fallthrough for now
        self.value_for_variation_or_rollout(self.fallthrough())
            .map(|val| Detail::new(val, Reason::Fallthrough))
            .unwrap_or(Detail::err(eval::Error::MalformedFlag))
    }

    pub fn on(&self) -> bool {
        self.get_loudly("on")
            .as_bool()
            .expect("'on' field should be boolean")
    }

    pub fn variation(&self, index: VariationIndex) -> Option<FlagValue> {
        self.variations()
            .get(index)
            .and_then(|json| FlagValue::from_json(json))
    }

    pub fn off_value(&self) -> FlagValue {
        self.variation(self.off_variation())
            .expect("my error handling is messed up")
    }

    pub fn off_variation(&self) -> VariationIndex {
        self.get_loudly("offVariation")
            .as_u64()
            .expect("'offVariation' field should be an integer") as VariationIndex
    }

    pub fn fallthrough(&self) -> &serde_json::Value {
        self.get_loudly("fallthrough")
    }

    pub fn variations(&self) -> &Vec<serde_json::Value> {
        self.get_loudly("variations")
            .as_array()
            .expect("'variations' field should be an array")
    }

    pub fn targets(&self) -> &Vec<serde_json::Value> {
        self.get_loudly("targets")
            .as_array()
            .expect("'targets' field should be an array")
    }

    pub fn value_for_variation_or_rollout(
        &self,
        vr: &serde_json::Value, /*, TODO user*/
    ) -> Option<FlagValue> {
        let variation_index =
            vr.get("variation")
                .expect("only variation supported for now")
                .as_u64()
                .expect("variation should be an integer") as VariationIndex;
        self.variation(variation_index)
    }

    fn get_loudly(&self, key: &str) -> &serde_json::Value {
        self.get(key)
            .expect(&format!("should have an {:?} field", key))
    }

    fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.0.get(key)
    }
}

pub type Segment = serde_json::Value; // TODO

#[derive(Clone, Debug, Deserialize)]
pub struct AllData {
    flags: HashMap<String, FeatureFlag>,
    segments: HashMap<String, Segment>,
}

pub struct FeatureStore {
    pub data: AllData,
}

impl FeatureStore {
    pub fn new() -> FeatureStore {
        FeatureStore {
            data: AllData {
                flags: HashMap::new(),
                segments: HashMap::new(),
            },
        }
    }

    pub fn init(&mut self, new_data: AllData) {
        self.data = new_data;
    }

    pub fn flag(&self, flag_name: &str) -> Option<&FeatureFlag> {
        self.data.flags.get(flag_name)
    }

    pub fn patch(&mut self, path: &str, data: FeatureFlag) {
        if !path.starts_with(FLAGS_PREFIX) {
            error!("Oops, can only patch flags atm");
            return;
        }

        let flag_name = &path[FLAGS_PREFIX.len()..];
        self.data.flags.insert(flag_name.to_string(), data);
    }
}
