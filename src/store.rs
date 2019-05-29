use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};

use serde::Deserialize;

const FLAGS_PREFIX: &'static str = "/flags/";

pub type Error = String;
pub type Result<T> = std::result::Result<T, Error>;

type VariationIndex = usize;

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
    pub fn evaluate(&self) -> Result<&serde_json::Value> {
        if !self.on() {
            return Ok(self.off_value());
        }

        // just return the fallthrough for now
        self.value_for_variation_or_rollout(self.fallthrough())
    }

    pub fn on(&self) -> bool {
        self.get_loudly("on")
            .as_bool()
            .expect("'on' field should be boolean")
    }

    pub fn variation(&self, index: VariationIndex) -> Result<&serde_json::Value> {
        self.variations()
            .get(index)
            .ok_or("malformed flag".to_string())
    }

    pub fn off_value(&self) -> &serde_json::Value {
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

    pub fn value_for_variation_or_rollout(
        &self,
        vr: &serde_json::Value, /*, TODO user*/
    ) -> Result<&serde_json::Value> {
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
