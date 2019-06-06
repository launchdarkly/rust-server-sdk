use std::collections::HashMap;

use super::eval::{self, Detail, Reason};
use super::users::User;

use serde::Deserialize;

const FLAGS_PREFIX: &'static str = "/flags/";

type VariationIndex = usize;

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum FlagValue {
    Bool(bool),
    NotYetImplemented(serde_json::Value),
}

impl FlagValue {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FlagValue::Bool(b) => Some(*b),
            _ => {
                warn!("variation type is not bool but {:?}", self);
                None
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Target {
    values: Vec<String>,
    variation: VariationIndex,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum VariationOrRollout {
    Variation(VariationIndex),
    // TODO Rollout
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureFlag {
    pub key: String,

    on: bool,
    targets: Vec<Target>,
    fallthrough: VariationOrRollout,
    off_variation: VariationIndex,
    variations: Vec<FlagValue>,
}

impl FeatureFlag {
    pub fn evaluate(&self, user: &User) -> Detail<&FlagValue> {
        if !self.on {
            return Detail::new(self.off_value(), Reason::Off);
        }

        for target in &self.targets {
            for value in &target.values {
                if value == &user.key {
                    return match self.variation(target.variation) {
                        Some(result) => Detail::new(result, Reason::TargetMatch),
                        None => Detail::err(eval::Error::MalformedFlag),
                    };
                }
            }
        }

        // just return the fallthrough for now
        self.value_for_variation_or_rollout(&self.fallthrough)
            .map(|val| Detail::new(val, Reason::Fallthrough))
            .unwrap_or(Detail::err(eval::Error::MalformedFlag))
    }

    pub fn variation(&self, index: VariationIndex) -> Option<&FlagValue> {
        self.variations.get(index)
    }

    pub fn off_value(&self) -> &FlagValue {
        self.variation(self.off_variation)
            .expect("my error handling is messed up")
    }

    fn value_for_variation_or_rollout(
        &self,
        vr: &VariationOrRollout, /*, TODO user*/
    ) -> Option<&FlagValue> {
        match vr {
            VariationOrRollout::Variation(index) => self.variation(*index),
        }
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
