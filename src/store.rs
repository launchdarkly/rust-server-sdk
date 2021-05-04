use std::collections::HashMap;

use rust_server_sdk_evaluation::{Flag, Segment, Store};
use serde::Deserialize;

const FLAGS_PREFIX: &str = "/flags/";
const SEGMENTS_PREFIX: &str = "/segments/";

type Error = String; // TODO

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum PatchTarget {
    Flag(Flag),
    Segment(Segment),
    Other(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize)]
pub struct AllData {
    flags: HashMap<String, Flag>,
    segments: HashMap<String, Segment>,
}

// TODO implement Error::ClientNotReady
pub struct FeatureStore {
    pub data: AllData,
}

impl Store for FeatureStore {
    fn flag(&self, flag_key: &str) -> Option<&Flag> {
        self.data.flags.get(flag_key)
    }

    fn segment(&self, segment_key: &str) -> Option<&Segment> {
        self.data.segments.get(segment_key)
    }
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

    pub fn all_flags(&self) -> &HashMap<String, Flag> {
        &self.data.flags
    }

    pub fn patch(&mut self, path: &str, data: PatchTarget) -> Result<(), Error> {
        if let Some(flag_key) = path.strip_prefix(FLAGS_PREFIX) {
            self.patch_flag(flag_key, data)
        } else if let Some(segment_key) = path.strip_prefix(SEGMENTS_PREFIX) {
            self.patch_segment(segment_key, data)
        } else {
            Err(format!("can't patch {}", path))
        }
    }

    fn patch_flag(&mut self, flag_key: &str, data: PatchTarget) -> Result<(), Error> {
        let flag = match data {
            PatchTarget::Flag(f) => Ok(f),
            PatchTarget::Segment(_) => Err("expected a flag, got a segment".to_string()),
            PatchTarget::Other(json) => Err(format!("couldn't parse JSON as a flag: {}", json)),
        }?;

        self.data.flags.insert(flag_key.to_string(), flag);
        Ok(())
    }

    fn patch_segment(&mut self, segment_key: &str, data: PatchTarget) -> Result<(), Error> {
        let segment = match data {
            PatchTarget::Segment(s) => Ok(s),
            PatchTarget::Flag(_) => Err("expected a segment, got a flag".to_string()),
            PatchTarget::Other(json) => Err(format!("couldn't parse JSON as a segment: {}", json)),
        }?;

        self.data.segments.insert(segment_key.to_string(), segment);
        Ok(())
    }

    pub fn delete(&mut self, path: &str) -> Result<(), Error> {
        if let Some(flag_key) = path.strip_prefix(FLAGS_PREFIX) {
            self.data.flags.remove(flag_key);
        } else if let Some(segment_key) = path.strip_prefix(SEGMENTS_PREFIX) {
            self.data.segments.remove(segment_key);
        } else {
            return Err(format!("can't delete {}", path));
        }

        Ok(())
    }
}

impl Default for FeatureStore {
    fn default() -> Self {
        Self::new()
    }
}
