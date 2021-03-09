use std::collections::HashMap;

use super::eval::{self, Detail, Reason, VariationIndex};
use super::users::{AttributeValue, User};

use chrono::{self, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};

const FLAGS_PREFIX: &str = "/flags/";
const SEGMENTS_PREFIX: &str = "/segments/";

type Error = String; // TODO

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FlagValue {
    Bool(bool),
    Str(String),
    Float(f64),
    Int(i64),
    Json(serde_json::Value),
}

impl From<bool> for FlagValue {
    fn from(b: bool) -> FlagValue {
        FlagValue::Bool(b)
    }
}

impl From<String> for FlagValue {
    fn from(s: String) -> FlagValue {
        FlagValue::Str(s)
    }
}

impl From<f64> for FlagValue {
    fn from(f: f64) -> FlagValue {
        FlagValue::Float(f)
    }
}

impl From<i64> for FlagValue {
    fn from(i: i64) -> FlagValue {
        FlagValue::Int(i)
    }
}

impl From<serde_json::Value> for FlagValue {
    fn from(v: serde_json::Value) -> Self {
        use serde_json::Value;
        match v {
            Value::Bool(b) => b.into(),
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    f.into()
                } else if let Some(i) = n.as_i64() {
                    i.into()
                } else {
                    warn!("unrepresentable number {}, converting to string", n);
                    FlagValue::Json(format!("{}", n).into())
                }
            }
            Value::String(s) => s.into(),
            Value::Null | Value::Object(_) | Value::Array(_) => FlagValue::Json(v),
        }
    }
}

impl FlagValue {
    // TODO implement type coercion here?

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FlagValue::Bool(b) => Some(*b),
            _ => {
                warn!("variation type is not bool but {:?}", self);
                None
            }
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            FlagValue::Str(s) => Some(s.clone()),
            _ => {
                warn!("variation type is not str but {:?}", self);
                None
            }
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            FlagValue::Float(f) => Some(*f),
            _ => {
                warn!("variation type is not float but {:?}", self);
                None
            }
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            FlagValue::Int(i) => Some(*i),
            _ => {
                // TODO this has undefined behaviour for huge floats: https://stackoverflow.com/a/41139453
                self.as_float().map(|f| f.round() as i64)
            }
        }
    }

    pub fn as_json(&self) -> Option<serde_json::Value> {
        use serde_json::Value;
        match self {
            FlagValue::Bool(b) => Some(Value::from(*b)),
            FlagValue::Str(s) => Some(Value::from(s.as_str())),
            FlagValue::Float(f) => Some(Value::from(*f)),
            FlagValue::Int(i) => Some(Value::from(*i)),
            FlagValue::Json(v) => Some(v.clone()),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Target {
    values: Vec<String>,
    variation: VariationIndex,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
enum Op {
    In,
    StartsWith,
    EndsWith,
    Contains,
    Matches,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Before,
    After,
    SegmentMatch,
    SemVerEqual,
    SemVerGreaterThan,
    SemVerLessThan,
}

impl Op {
    fn matches(&self, lhs: &AttributeValue, rhs: &AttributeValue) -> bool {
        match self {
            Op::In => lhs == rhs,

            // string ops
            Op::StartsWith => string_op(lhs, rhs, |l, r| l.starts_with(r)),
            Op::EndsWith => string_op(lhs, rhs, |l, r| l.ends_with(r)),
            Op::Contains => string_op(lhs, rhs, |l, r| l.contains(r)),
            Op::Matches => string_op(lhs, rhs, |l, r| match Regex::new(r) {
                Ok(re) => re.is_match(l),
                Err(e) => {
                    warn!("Invalid regex for 'matches' operator ({}): {}", e, l);
                    false
                }
            }),

            // numeric ops
            Op::LessThan => numeric_op(lhs, rhs, |l, r| l < r),
            Op::LessThanOrEqual => numeric_op(lhs, rhs, |l, r| l <= r),
            Op::GreaterThan => numeric_op(lhs, rhs, |l, r| l > r),
            Op::GreaterThanOrEqual => numeric_op(lhs, rhs, |l, r| l >= r),

            Op::Before => time_op(lhs, rhs, |l, r| l < r),
            Op::After => time_op(lhs, rhs, |l, r| l > r),

            Op::SegmentMatch => {
                error!("segmentMatch operator should be special-cased, shouldn't get here");
                false
            }

            // TODO test semver ops
            Op::SemVerEqual => semver_op(lhs, rhs, |l, r| l == r),
            Op::SemVerLessThan => semver_op(lhs, rhs, |l, r| l < r),
            Op::SemVerGreaterThan => semver_op(lhs, rhs, |l, r| l > r),
        }
    }
}

fn string_op<F: Fn(&String, &String) -> bool>(
    lhs: &AttributeValue,
    rhs: &AttributeValue,
    f: F,
) -> bool {
    match (lhs.as_str(), rhs.as_str()) {
        (Some(l), Some(r)) => f(l, r),
        _ => false,
    }
}

fn numeric_op<F: Fn(f64, f64) -> bool>(lhs: &AttributeValue, rhs: &AttributeValue, f: F) -> bool {
    match (lhs.to_f64(), rhs.to_f64()) {
        (Some(l), Some(r)) => f(l, r),
        _ => false,
    }
}

fn time_op<F: Fn(chrono::DateTime<Utc>, chrono::DateTime<Utc>) -> bool>(
    lhs: &AttributeValue,
    rhs: &AttributeValue,
    f: F,
) -> bool {
    match (lhs.to_datetime(), rhs.to_datetime()) {
        (Some(l), Some(r)) => f(l, r),
        _ => false,
    }
}

fn semver_op<F: Fn(semver::Version, semver::Version) -> bool>(
    lhs: &AttributeValue,
    rhs: &AttributeValue,
    f: F,
) -> bool {
    match (lhs.as_semver(), rhs.as_semver()) {
        (Some(l), Some(r)) => f(l, r),
        _ => false,
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Clause {
    attribute: String,
    negate: bool,
    op: Op,
    values: Vec<AttributeValue>,
}

impl Clause {
    fn matches(&self, user: &User, store: &FeatureStore) -> bool {
        if let Op::SegmentMatch = self.op {
            self.matches_segment(user, store)
        } else {
            self.matches_non_segment(user)
        }
    }

    fn maybe_negate(&self, v: bool) -> bool {
        if self.negate {
            !v
        } else {
            v
        }
    }

    fn matches_segment(&self, user: &User, store: &FeatureStore) -> bool {
        let any_match = self.values.iter().find(|value| {
            value
                .as_str()
                .and_then(|segment_key| store.segment(segment_key))
                .map(|segment| segment.contains(user))
                .unwrap_or(false)
        });
        self.maybe_negate(any_match.is_some())
    }

    fn matches_non_segment(&self, user: &User) -> bool {
        let user_val = match user.value_of(&self.attribute) {
            Some(v) => v,
            None => return false,
        };

        let any_match = user_val.find(|user_val_v| {
            let any_match_for_v = self
                .values
                .iter()
                .find(|clause_val| self.op.matches(user_val_v, clause_val));
            any_match_for_v.is_some()
        });

        self.maybe_negate(any_match.is_some())
    }
}

#[derive(Clone, Debug, Deserialize)]
struct FlagRule {
    clauses: Vec<Clause>,
    #[serde(flatten)]
    variation_or_rollout: VariationOrRollout,
}

impl FlagRule {
    fn matches(&self, user: &User, store: &FeatureStore) -> bool {
        // rules match if _all_ of their clauses do
        for clause in &self.clauses {
            if !clause.matches(user, store) {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
enum VariationOrRolloutOrMalformed {
    VariationOrRollout(VariationOrRollout),
    Malformed(serde_json::Value),
}

impl VariationOrRolloutOrMalformed {
    fn get(&self) -> Result<&VariationOrRollout, String> {
        match self {
            VariationOrRolloutOrMalformed::VariationOrRollout(v) => Ok(v),
            VariationOrRolloutOrMalformed::Malformed(v) => {
                Err(format!("malformed variation_or_rollout: {}", v))
            }
        }
    }
}

impl From<VariationOrRollout> for VariationOrRolloutOrMalformed {
    fn from(vor: VariationOrRollout) -> VariationOrRolloutOrMalformed {
        VariationOrRolloutOrMalformed::VariationOrRollout(vor)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum PatchTarget {
    Flag(FeatureFlag),
    Segment(Segment),
    Other(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum VariationOrRollout {
    Variation(VariationIndex),
    Rollout {
        bucket_by: Option<String>,
        variations: Vec<WeightedVariation>,
    },
}

impl VariationOrRollout {
    fn variation(&self, flag_key: &str, user: &User, salt: &str) -> Option<VariationIndex> {
        match self {
            VariationOrRollout::Variation(index) => Some(*index),
            VariationOrRollout::Rollout {
                bucket_by,
                variations,
            } => {
                let bucket = user.bucket(flag_key, bucket_by.as_ref().map(String::as_str), salt);
                let mut sum = 0.0;
                for variation in variations {
                    sum += variation.weight / 100_000.0;
                    if bucket < sum {
                        return Some(variation.variation);
                    }
                }
                None
            }
        }
    }
}

type VariationWeight = f32;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct WeightedVariation {
    pub variation: VariationIndex,
    pub weight: VariationWeight,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Prereq {
    key: String,
    variation: VariationIndex,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureFlag {
    pub key: String,
    pub version: u64,

    on: bool,

    targets: Vec<Target>,
    rules: Vec<FlagRule>,
    prerequisites: Vec<Prereq>,

    fallthrough: VariationOrRolloutOrMalformed,
    off_variation: Option<VariationIndex>,
    variations: Vec<FlagValue>,

    salt: String,
    // TODO implement more flag fields
}

impl FeatureFlag {
    #[cfg(test)]
    pub fn basic_flag(key: &str) -> FeatureFlag {
        FeatureFlag {
            key: key.to_string(),
            version: 42,
            on: true,
            targets: vec![],
            rules: vec![],
            prerequisites: vec![],
            fallthrough: VariationOrRolloutOrMalformed::VariationOrRollout(
                VariationOrRollout::Variation(1),
            ),
            off_variation: Some(0),
            variations: vec![false.into(), true.into()],
            salt: "kosher".to_string(),
        }
    }

    #[cfg(test)]
    pub fn basic_int_flag(key: &str) -> FeatureFlag {
        FeatureFlag {
            key: key.to_string(),
            version: 42,
            on: true,
            targets: vec![],
            rules: vec![],
            prerequisites: vec![],
            fallthrough: VariationOrRolloutOrMalformed::VariationOrRollout(
                VariationOrRollout::Variation(1),
            ),
            off_variation: Some(0),
            variations: vec![0.into(), std::i64::MAX.into()],
            salt: "kosher".to_string(),
        }
    }

    #[cfg(test)]
    pub fn basic_json_flag(key: &str) -> FeatureFlag {
        use serde_json::Value;

        FeatureFlag {
            key: key.to_string(),
            version: 42,
            on: true,
            targets: vec![],
            rules: vec![],
            prerequisites: vec![],
            fallthrough: VariationOrRolloutOrMalformed::VariationOrRollout(
                VariationOrRollout::Variation(1),
            ),
            off_variation: Some(0),
            variations: vec![Value::Null.into(), json!({ "foo": "bar" }).into()],
            salt: "kosher".to_string(),
        }
    }

    pub fn evaluate(&self, user: &User, store: &FeatureStore) -> Detail<&FlagValue> {
        if !self.on {
            return self.off_value(Reason::Off);
        }

        for prereq in &self.prerequisites {
            if let Some(flag) = store.flag(&prereq.key) {
                if !flag.on || flag.evaluate(user, store).variation_index != Some(prereq.variation)
                {
                    // TODO capture prereq event
                    return self.off_value(Reason::PrerequisiteFailed {
                        prerequisite_key: prereq.key.to_string(),
                    });
                }
            } else {
                return self.off_value(Reason::PrerequisiteFailed {
                    prerequisite_key: prereq.key.to_string(),
                });
            }
        }

        for target in &self.targets {
            for value in &target.values {
                if value == user.key() {
                    return self.variation(target.variation, Reason::TargetMatch);
                }
            }
        }

        for rule in &self.rules {
            if rule.matches(&user, store) {
                return self.value_for_variation_or_rollout(
                    &rule.variation_or_rollout,
                    &user,
                    Reason::RuleMatch,
                );
            }
        }

        // just return the fallthrough for now
        self.fallthrough
            // TODO ugh, clean this up
            .get()
            .as_ref()
            .ok()
            .map(|vor| self.value_for_variation_or_rollout(vor, &user, Reason::Fallthrough))
            .unwrap_or_else(|| Detail::err(eval::Error::MalformedFlag))
    }

    pub fn variation(&self, index: VariationIndex, reason: Reason) -> Detail<&FlagValue> {
        Detail {
            value: self.variations.get(index),
            variation_index: Some(index),
            reason,
        }
        .should_have_value(eval::Error::MalformedFlag)
    }

    pub fn off_value(&self, reason: Reason) -> Detail<&FlagValue> {
        match self.off_variation {
            Some(index) => self.variation(index, reason),
            None => Detail::empty(reason),
        }
    }

    fn value_for_variation_or_rollout(
        &self,
        vr: &VariationOrRollout,
        user: &User,
        reason: Reason,
    ) -> Detail<&FlagValue> {
        vr.variation(&self.key, user, &self.salt)
            .map_or(Detail::err(eval::Error::MalformedFlag), |variation| {
                self.variation(variation, reason)
            })
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    pub key: String,

    included: Vec<String>,
    excluded: Vec<String>,
    rules: Vec<SegmentRule>,
    salt: String,
}

impl Segment {
    // TODO segment explanations
    fn contains(&self, user: &User) -> bool {
        let user_key = user.key();

        if self.included.contains(user_key) {
            return true;
        }
        if self.excluded.contains(user_key) {
            return false;
        }

        for rule in &self.rules {
            if rule.matches(user) {
                return true;
            }
        }

        false
    }
}

#[derive(Clone, Debug, Deserialize)]
struct SegmentRule {
    clauses: Vec<Clause>,
    // TODO segment rollout
    // weight: Option<VariationWeight>
    // bucket_by: Option<String>,
}

impl SegmentRule {
    fn matches(&self, user: &User) -> bool {
        // rules match if _all_ of their clauses do
        for clause in &self.clauses {
            if !clause.matches_non_segment(user) {
                return false;
            }
        }
        true
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct AllData {
    flags: HashMap<String, FeatureFlag>,
    segments: HashMap<String, Segment>,
}

// TODO implement Error::ClientNotReady
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

    pub fn flag(&self, flag_key: &str) -> Option<&FeatureFlag> {
        self.data.flags.get(flag_key)
    }

    pub fn segment(&self, segment_key: &str) -> Option<&Segment> {
        self.data.segments.get(segment_key)
    }

    pub fn all_flags(&self) -> &HashMap<String, FeatureFlag> {
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use spectral::prelude::*;

    use super::FlagValue::*;
    use super::*;

    use crate::eval::Reason::*;
    use crate::users::User;

    #[test]
    fn test_parse_variation_or_rollout() {
        let variation: VariationOrRolloutOrMalformed =
            serde_json::from_str(r#"{"variation":4}"#).expect("should parse");
        assert_that!(variation.get()).is_ok_containing(&VariationOrRollout::Variation(4));

        let rollout: VariationOrRolloutOrMalformed =
            serde_json::from_str(r#"{"rollout":{"variations":[{"variation":1,"weight":100000}]}}"#)
                .expect("should parse");
        assert_that!(rollout.get()).is_ok_containing(&VariationOrRollout::Rollout {
            bucket_by: None,
            variations: vec![WeightedVariation {
                variation: 1,
                weight: 100000.0,
            }],
        });

        let malformed: VariationOrRolloutOrMalformed =
            serde_json::from_str("{}").expect("should parse");
        assert_that!(malformed.get()).is_err();
    }

    const TEST_FLAG_JSON: &str = r#"{
        "key": "test-flag",
        "version": 1,
        "on": false,
        "targets": [
            {"values": ["bob"], "variation": 1}
        ],
        "rules": [],
        "prerequisites": [],
        "fallthrough": {"variation": 0},
        "offVariation": 1,
        "salt": "kosher",
        "variations": [true, false]
    }"#;

    const FLAG_WITH_RULES_JSON: &str = r#"{
        "key": "with-rules",
        "version": 1,
        "on": false,
        "targets": [],
        "rules": [
            {
                "clauses": [
                    {
                        "attribute": "team",
                        "negate": false,
                        "op": "in",
                        "values": ["Avengers"]
                    }
                ],
                "id": "667e5007-01e4-4b51-9e33-5abe7f892790",
                "variation": 1
            }
        ],
        "prerequisites": [],
        "fallthrough": {"variation": 0},
        "offVariation": 1,
        "salt": "kosher",
        "variations": [true, false]
    }"#;

    const TEST_SEGMENT_JSON: &str = r#"{
        "deleted": false,
        "excluded": ["bob"],
        "included": ["alice"],
        "key": "awesome-people",
        "rules": [],
        "salt": "kosher",
        "version": 1
    }"#;

    #[test]
    fn test_parse_flag() {
        let flags = btreemap! {
            "test-flag" => TEST_FLAG_JSON,
            "with-rules" => FLAG_WITH_RULES_JSON,
        };
        for (key, json) in flags {
            let f: FeatureFlag = serde_json::from_str(json)
                .unwrap_or_else(|e| panic!("should parse flag {}: {}", key, e));
            assert_eq!(f.key, key);
            assert!(!f.on);
            assert_eq!(f.off_variation, Some(1));
        }
    }

    #[test]
    fn test_parse_segment() {
        let segments = btreemap! {
            "test-segment" => TEST_SEGMENT_JSON,
        };
        for (key, json) in segments {
            let s: Segment = serde_json::from_str(json)
                .unwrap_or_else(|e| panic!("should parse segment {}: {}", key, e));
            assert_that!(s.key).is_equal_to("awesome-people".to_string());
            assert_that!(s.included).is_equal_to(vec!["alice".into()]);
            assert_that!(s.excluded).is_equal_to(vec!["bob".into()]);
            assert_that!(s.salt).is_equal_to("kosher".to_string());
        }
    }

    #[test]
    fn test_eval_flag_basic() {
        let alice = User::with_key("alice").build(); // not targeted
        let bob = User::with_key("bob").build(); // targeted
        let mut flag: FeatureFlag = serde_json::from_str(TEST_FLAG_JSON).unwrap();

        assert!(!flag.on);
        let detail = flag.evaluate(&alice, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&Off);

        assert_that!(flag.evaluate(&bob, &FeatureStore::new())).is_equal_to(&detail);

        // flip off variation
        flag.off_variation = Some(0);
        let detail = flag.evaluate(&alice, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(true));
        assert_that!(detail.variation_index).contains_value(0);

        // off variation unspecified
        flag.off_variation = None;
        let detail = flag.evaluate(&alice, &FeatureStore::new());
        assert_that!(detail.value).is_none();
        assert_that!(detail.variation_index).is_none();
        assert_that!(detail.reason).is_equal_to(&Off);

        // flip targeting on
        flag.on = true;
        let detail = flag.evaluate(&alice, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(true));
        assert_that!(detail.variation_index).contains_value(0);
        assert_that!(detail.reason).is_equal_to(&Fallthrough);

        let detail = flag.evaluate(&bob, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&TargetMatch);

        // flip default variation
        flag.fallthrough = VariationOrRollout::Variation(1).into();
        let detail = flag.evaluate(&alice, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);

        // bob's reason should still be TargetMatch even though his value is now the default
        let detail = flag.evaluate(&bob, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&TargetMatch);
    }

    #[test]
    fn test_eval_flag_rules() {
        let alice = User::with_key("alice").build();
        let bob = User::with_key("bob")
            .custom(hashmap! {
                "team".into() => "Avengers".into(),
            })
            .build();

        let mut flag: FeatureFlag = serde_json::from_str(FLAG_WITH_RULES_JSON).unwrap();

        assert!(!flag.on);
        for user in vec![&alice, &bob] {
            let detail = flag.evaluate(user, &FeatureStore::new());
            assert_that!(detail.value).contains_value(&Bool(false));
            assert_that!(detail.variation_index).contains_value(1);
            assert_that!(detail.reason).is_equal_to(&Off);
        }

        // flip targeting on
        flag.on = true;
        let detail = flag.evaluate(&alice, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(true));
        assert_that!(detail.variation_index).contains_value(0);
        assert_that!(detail.reason).is_equal_to(&Fallthrough);

        let detail = flag.evaluate(&bob, &FeatureStore::new());
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&RuleMatch);
    }

    #[test]
    fn test_eval_flag_prereqs() {
        let mut store = FeatureStore::new();
        let mut flag = FeatureFlag::basic_flag("flag");
        assert!(flag.on);
        flag.prerequisites.push(Prereq {
            key: "prereq".to_string(),
            variation: 1,
        });
        store
            .patch("/flags/flag", PatchTarget::Flag(flag.clone()))
            .expect("patch should succeed");

        let alice = User::with_key("alice").build();
        let bob = User::with_key("bob").build();

        // prerequisite missing => prerequisite failed.
        for user in vec![&alice, &bob] {
            let detail = flag.evaluate(user, &FeatureStore::new());
            assert_that!(detail.value).contains_value(&Bool(false));
            assert_that!(detail.reason).is_equal_to(&PrerequisiteFailed {
                prerequisite_key: "prereq".to_string(),
            });
        }

        let mut prereq = FeatureFlag::basic_flag("prereq");
        assert!(prereq.on);
        prereq.targets.push(Target {
            values: vec!["bob".into()],
            variation: 0,
        });
        store
            .patch("/flags/prereq", PatchTarget::Flag(prereq.clone()))
            .expect("patch should succeed");

        // prerequisite on
        let detail = flag.evaluate(&alice, &store);
        asserting!("alice should pass prereq and see fallthrough")
            .that(&detail.value)
            .contains_value(&Bool(true));
        let detail = flag.evaluate(&bob, &store);
        asserting!("bob should see prereq failed due to target")
            .that(&detail.value)
            .contains_value(&Bool(false));
        assert_that!(detail.reason).is_equal_to(Reason::PrerequisiteFailed {
            prerequisite_key: "prereq".to_string(),
        });

        prereq.on = false;
        store
            .patch("/flags/prereq", PatchTarget::Flag(prereq.clone()))
            .expect("patch should succeed");

        // prerequisite off
        for user in vec![&alice, &bob] {
            let detail = flag.evaluate(user, &store);
            assert_that!(detail.value).contains_value(&Bool(false));
            assert_that!(detail.reason).is_equal_to(&PrerequisiteFailed {
                prerequisite_key: "prereq".to_string(),
            });
        }
    }

    #[test]
    fn test_eval_flag_segments() {
        let mut store = FeatureStore::new();

        let mut segment = Segment::default();
        segment.key = "segment".to_string();
        segment.included.push("alice".to_string());
        store
            .patch_segment("segment", PatchTarget::Segment(segment))
            .expect("patch should succeed");

        let mut flag = FeatureFlag::basic_flag("flag");
        flag.on = true;
        flag.rules.push(FlagRule {
            clauses: vec![Clause {
                attribute: "segmentMatch".to_string(),
                op: Op::SegmentMatch,
                values: vec!["segment".into()],
                negate: false,
            }],
            variation_or_rollout: VariationOrRollout::Variation(0),
        });
        store
            .patch("/flags/flag", PatchTarget::Flag(flag.clone()))
            .expect("patch should succeed");

        let alice = User::with_key("alice").build();
        let bob = User::with_key("bob").build();

        let detail = flag.evaluate(&alice, &store);
        asserting!("alice is in segment, should see false with RuleMatch")
            .that(&detail.value)
            .contains_value(&Bool(false));
        assert_that!(detail.reason).is_equal_to(Reason::RuleMatch);
        let detail = flag.evaluate(&bob, &store);
        asserting!("bob is not in segment and should see fallthrough")
            .that(&detail.value)
            .contains_value(&Bool(true));
        assert_that!(detail.reason).is_equal_to(Reason::Fallthrough);
    }

    fn astring(s: &str) -> AttributeValue {
        AttributeValue::String(s.into())
    }
    fn afloat(f: f64) -> AttributeValue {
        AttributeValue::Float(f)
    }
    fn aint(i: i64) -> AttributeValue {
        AttributeValue::Int(i)
    }

    #[test]
    fn test_op_in() {
        // strings
        assert!(Op::In.matches(&astring("foo"), &astring("foo")));

        assert!(!Op::In.matches(&astring("foo"), &astring("bar")));
        assert!(
            !Op::In.matches(&astring("Foo"), &astring("foo")),
            "case sensitive"
        );

        // numbers
        assert!(Op::In.matches(&afloat(42.0), &afloat(42.0)));
        assert!(!Op::In.matches(&afloat(42.0), &afloat(3.0)));
        assert!(Op::In.matches(&afloat(0.0), &afloat(-0.0)));
    }

    #[test]
    fn test_op_starts_with() {
        // degenerate cases
        assert!(Op::StartsWith.matches(&astring(""), &astring("")));
        assert!(Op::StartsWith.matches(&astring("a"), &astring("")));
        assert!(Op::StartsWith.matches(&astring("a"), &astring("a")));

        // test asymmetry
        assert!(Op::StartsWith.matches(&astring("food"), &astring("foo")));
        assert!(!Op::StartsWith.matches(&astring("foo"), &astring("food")));

        assert!(
            !Op::StartsWith.matches(&astring("Food"), &astring("foo")),
            "case sensitive"
        );
    }

    #[test]
    fn test_op_ends_with() {
        // degenerate cases
        assert!(Op::EndsWith.matches(&astring(""), &astring("")));
        assert!(Op::EndsWith.matches(&astring("a"), &astring("")));
        assert!(Op::EndsWith.matches(&astring("a"), &astring("a")));

        // test asymmetry
        assert!(Op::EndsWith.matches(&astring("food"), &astring("ood")));
        assert!(!Op::EndsWith.matches(&astring("ood"), &astring("food")));

        assert!(
            !Op::EndsWith.matches(&astring("FOOD"), &astring("ood")),
            "case sensitive"
        );
    }

    #[test]
    fn test_op_contains() {
        // degenerate cases
        assert!(Op::Contains.matches(&astring(""), &astring("")));
        assert!(Op::Contains.matches(&astring("a"), &astring("")));
        assert!(Op::Contains.matches(&astring("a"), &astring("a")));

        // test asymmetry
        assert!(Op::Contains.matches(&astring("food"), &astring("oo")));
        assert!(!Op::Contains.matches(&astring("oo"), &astring("food")));

        assert!(
            !Op::Contains.matches(&astring("FOOD"), &astring("oo")),
            "case sensitive"
        );
    }

    #[test]
    fn test_op_matches() {
        // degenerate cases
        assert!(Op::Matches.matches(&astring(""), &astring("")));
        assert!(Op::Matches.matches(&astring("a"), &astring("")));

        // simple regexes
        assert!(Op::Matches.matches(&astring("a"), &astring("a")));
        assert!(Op::Matches.matches(&astring("a"), &astring(".")));
        assert!(Op::Matches.matches(&astring("abc"), &astring(".*")));

        assert!(!Op::Matches.matches(&astring(""), &astring(".")));

        assert!(
            Op::Matches.matches(&astring("party"), &astring("art")),
            "should match part of string"
        );

        assert!(
            !Op::Matches.matches(&astring(""), &astring(r"\")),
            "invalid regex should match nothing"
        );

        // TODO test more cases
    }

    #[test]
    fn test_ops_numeric() {
        // basic numeric comparisons
        assert!(Op::LessThan.matches(&afloat(0.0), &afloat(1.0)));
        assert!(!Op::LessThan.matches(&afloat(0.0), &afloat(0.0)));
        assert!(!Op::LessThan.matches(&afloat(1.0), &afloat(0.0)));

        assert!(Op::GreaterThan.matches(&afloat(1.0), &afloat(0.0)));
        assert!(!Op::GreaterThan.matches(&afloat(0.0), &afloat(0.0)));
        assert!(!Op::GreaterThan.matches(&afloat(0.0), &afloat(1.0)));

        assert!(Op::LessThanOrEqual.matches(&afloat(0.0), &afloat(1.0)));
        assert!(Op::LessThanOrEqual.matches(&afloat(0.0), &afloat(0.0)));
        assert!(!Op::LessThanOrEqual.matches(&afloat(1.0), &afloat(0.0)));

        assert!(Op::GreaterThanOrEqual.matches(&afloat(1.0), &afloat(0.0)));
        assert!(Op::GreaterThanOrEqual.matches(&afloat(0.0), &afloat(0.0)));
        assert!(!Op::GreaterThanOrEqual.matches(&afloat(0.0), &afloat(1.0)));

        // conversions
        assert!(
            Op::LessThan.matches(&astring("0"), &afloat(1.0)),
            "should convert numeric string on LHS"
        );
        assert!(
            Op::LessThan.matches(&afloat(0.0), &astring("1")),
            "should convert numeric string on RHS"
        );

        assert!(
            !Op::LessThan.matches(&astring("Tuesday"), &afloat(7.0)),
            "non-numeric strings don't match"
        );
    }

    #[test]
    fn test_ops_time() {
        let today = SystemTime::now();
        let today_millis = today
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64;
        let yesterday_millis = today_millis - 86_400_000 as f64;

        // basic UNIX timestamp comparisons
        assert!(Op::Before.matches(&afloat(yesterday_millis), &afloat(today_millis)));
        assert!(!Op::Before.matches(&afloat(today_millis), &afloat(yesterday_millis)));
        assert!(!Op::Before.matches(&afloat(today_millis), &afloat(today_millis)));

        assert!(Op::After.matches(&afloat(today_millis), &afloat(yesterday_millis)));
        assert!(!Op::After.matches(&afloat(yesterday_millis), &afloat(today_millis)));
        assert!(!Op::After.matches(&afloat(today_millis), &afloat(today_millis)));

        // numeric strings get converted as millis
        assert!(Op::Before.matches(
            &astring(&yesterday_millis.to_string()),
            &afloat(today_millis)
        ));
        assert!(Op::After.matches(
            &afloat(today_millis),
            &astring(&yesterday_millis.to_string())
        ));

        // date-formatted strings get parsed
        assert!(Op::Before.matches(
            &astring("2019-11-19T17:29:00.000000-07:00"),
            &afloat(today_millis)
        ));
        assert!(
            Op::Before.matches(&astring("2019-11-19T17:29:00-07:00"), &afloat(today_millis)),
            "fractional seconds part is optional"
        );

        assert!(Op::After.matches(
            &afloat(today_millis),
            &astring("2019-11-19T17:29:00.000000-07:00")
        ));

        // nonsense strings don't match
        assert!(!Op::Before.matches(&astring("fish"), &afloat(today_millis)));
        assert!(!Op::After.matches(&afloat(today_millis), &astring("fish")));
    }

    #[test]
    fn test_semver_ops() {
        assert!(Op::SemVerEqual.matches(&astring("2.0.0"), &astring("2.0.0")));

        assert!(
            Op::SemVerEqual.matches(&astring("2.0"), &astring("2.0.0")),
            "we allow missing components (filled in with zeroes)"
        );
        assert!(
            Op::SemVerEqual.matches(&astring("2"), &astring("2.0.0")),
            "we allow missing components (filled in with zeroes)"
        );

        assert!(!Op::SemVerEqual.matches(&astring("2.0.0"), &astring("3.0.0")));
        assert!(!Op::SemVerEqual.matches(&astring("2.0.0"), &astring("2.1.0")));
        assert!(!Op::SemVerEqual.matches(&astring("2.0.0"), &astring("2.0.1")));

        assert!(Op::SemVerGreaterThan.matches(&astring("3.0.0"), &astring("2.0.0")));
        assert!(Op::SemVerGreaterThan.matches(&astring("2.1.0"), &astring("2.0.0")));
        assert!(Op::SemVerGreaterThan.matches(&astring("2.0.1"), &astring("2.0.0")));
        assert!(Op::SemVerGreaterThan
            .matches(&astring("2.0.0-rc.10.green"), &astring("2.0.0-rc.2.green")));
        assert!(
            Op::SemVerGreaterThan.matches(&astring("2.0.0-rc.2.red"), &astring("2.0.0-rc.2.green")),
            "red > green"
        );
        assert!(
            Op::SemVerGreaterThan
                .matches(&astring("2.0.0-rc.2.green.1"), &astring("2.0.0-rc.2.green")),
            "adding more version components makes it greater"
        );

        assert!(!Op::SemVerGreaterThan.matches(&astring("2.0.0"), &astring("2.0.0")));
        assert!(!Op::SemVerGreaterThan.matches(&astring("1.9.0"), &astring("2.0.0")));
        assert!(
            !Op::SemVerGreaterThan.matches(&astring("2.0.0-rc"), &astring("2.0.0")),
            "prerelease version < released version"
        );
        assert!(
            !Op::SemVerGreaterThan.matches(&astring("2.0.0+build"), &astring("2.0.0")),
            "build metadata is ignored, these versions are equal"
        );

        assert!(!Op::SemVerEqual.matches(&astring("2.0.0"), &astring("200")));

        // we don't convert
        assert!(!Op::SemVerEqual.matches(&astring("2.0.0"), &afloat(2.0)));
        assert!(!Op::SemVerEqual.matches(&astring("2.0.0"), &aint(2)));
    }

    #[test]
    fn test_clause_matches() {
        let one_val_clause = Clause {
            attribute: "a".into(),
            negate: false,
            op: Op::In,
            values: vec!["foo".into()],
        };
        let many_val_clause = Clause {
            attribute: "a".into(),
            negate: false,
            op: Op::In,
            values: vec!["foo".into(), "bar".into()],
        };
        let negated_clause = Clause {
            attribute: "a".into(),
            negate: true,
            op: Op::In,
            values: vec!["foo".into()],
        };
        let negated_many_val_clause = Clause {
            attribute: "a".into(),
            negate: true,
            op: Op::In,
            values: vec!["foo".into(), "bar".into()],
        };
        let key_clause = Clause {
            attribute: "key".into(),
            negate: false,
            op: Op::In,
            values: vec!["mu".into()],
        };

        let matching_user = User::with_key("mu")
            .custom(hashmap! {"a".into() => "foo".into()})
            .build();
        let non_matching_user = User::with_key("nmu")
            .custom(hashmap! {"a".into() => "lol".into()})
            .build();
        let user_without_attr = User::with_key("uwa").build();

        assert!(one_val_clause.matches(&matching_user, &FeatureStore::new()));
        assert!(!one_val_clause.matches(&non_matching_user, &FeatureStore::new()));
        assert!(!one_val_clause.matches(&user_without_attr, &FeatureStore::new()));

        assert!(!negated_clause.matches(&matching_user, &FeatureStore::new()));
        assert!(negated_clause.matches(&non_matching_user, &FeatureStore::new()));

        assert!(
            !negated_clause.matches(&user_without_attr, &FeatureStore::new()),
            "targeting missing attribute does not match even when negated"
        );

        assert!(
            many_val_clause.matches(&matching_user, &FeatureStore::new()),
            "requires only one of the values"
        );
        assert!(!many_val_clause.matches(&non_matching_user, &FeatureStore::new()));
        assert!(!many_val_clause.matches(&user_without_attr, &FeatureStore::new()));

        assert!(
            !negated_many_val_clause.matches(&matching_user, &FeatureStore::new()),
            "requires all values are missing"
        );
        assert!(negated_many_val_clause.matches(&non_matching_user, &FeatureStore::new()));

        assert!(
            !negated_many_val_clause.matches(&user_without_attr, &FeatureStore::new()),
            "targeting missing attribute does not match even when negated"
        );

        assert!(
            key_clause.matches(&matching_user, &FeatureStore::new()),
            "should match key"
        );
        assert!(
            !key_clause.matches(&non_matching_user, &FeatureStore::new()),
            "should not match non-matching key"
        );

        let user_with_many = User::with_key("uwm")
            .custom(hashmap! {"a".into() => vec!["foo", "bar", "lol"].into()})
            .build();

        assert!(one_val_clause.matches(&user_with_many, &FeatureStore::new()));
        assert!(many_val_clause.matches(&user_with_many, &FeatureStore::new()));

        assert!(!negated_clause.matches(&user_with_many, &FeatureStore::new()));
        assert!(!negated_many_val_clause.matches(&user_with_many, &FeatureStore::new()));
    }

    struct AttributeTestCase {
        matching_user: User,
        non_matching_user: User,
        user_without_attr: Option<User>,
    }

    #[test]
    fn test_clause_matches_attributes() {
        let tests: HashMap<&str, AttributeTestCase> = hashmap! {
            "key" => AttributeTestCase {
                matching_user: User::with_key("match").build(),
                non_matching_user: User::with_key("nope").build(),
                user_without_attr: None,
            },
            "secondary" => AttributeTestCase {
                matching_user: User::with_key("mu").secondary("match").build(),
                non_matching_user: User::with_key("nmu").secondary("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
            "ip" => AttributeTestCase {
                matching_user: User::with_key("mu").ip("match").build(),
                non_matching_user: User::with_key("nmu").ip("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
            "country" => AttributeTestCase {
                matching_user: User::with_key("mu").country("match").build(),
                non_matching_user: User::with_key("nmu").country("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
            "email" => AttributeTestCase {
                matching_user: User::with_key("mu").email("match").build(),
                non_matching_user: User::with_key("nmu").email("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
            "firstName" => AttributeTestCase {
                matching_user: User::with_key("mu").first_name("match").build(),
                non_matching_user: User::with_key("nmu").first_name("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
            "lastName" => AttributeTestCase {
                matching_user: User::with_key("mu").last_name("match").build(),
                non_matching_user: User::with_key("nmu").last_name("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
            "avatar" => AttributeTestCase {
                matching_user: User::with_key("mu").avatar("match").build(),
                non_matching_user: User::with_key("nmu").avatar("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
            "name" => AttributeTestCase {
                matching_user: User::with_key("mu").name("match").build(),
                non_matching_user: User::with_key("nmu").name("nope").build(),
                user_without_attr: Some(User::with_key("uwa").build()),
            },
        };

        for (attr, test_case) in tests {
            let clause = Clause {
                attribute: attr.into(),
                negate: false,
                op: Op::In,
                values: vec!["match".into()],
            };

            assert!(
                clause.matches(&test_case.matching_user, &FeatureStore::new()),
                "should match {}",
                attr
            );
            assert!(
                !clause.matches(&test_case.non_matching_user, &FeatureStore::new()),
                "should not match non-matching {}",
                attr
            );
            if let Some(user_without_attr) = test_case.user_without_attr {
                assert!(
                    !clause.matches(&user_without_attr, &FeatureStore::new()),
                    "should not match user with null {}",
                    attr
                );
            }
        }
    }

    #[test]
    fn test_clause_matches_anonymous_attribute() {
        let clause = Clause {
            attribute: "anonymous".into(),
            negate: false,
            op: Op::In,
            values: vec![true.into()],
        };

        let anon_user = User::with_key("anon").anonymous(true).build();
        let non_anon_user = User::with_key("nonanon").anonymous(false).build();
        let implicitly_non_anon_user = User::with_key("implicit").build();

        assert!(clause.matches(&anon_user, &FeatureStore::new()));
        assert!(!clause.matches(&non_anon_user, &FeatureStore::new()));
        assert!(!clause.matches(&implicitly_non_anon_user, &FeatureStore::new()));
    }

    #[test]
    fn test_clause_matches_custom_attributes() {
        for attr in vec![
            "custom",  // check we can have an attribute called "custom"
            "custom1", // check custom attributes work the same
        ] {
            let clause = Clause {
                attribute: attr.into(),
                negate: false,
                op: Op::In,
                values: vec!["match".into()],
            };

            let matching_user = User::with_key("mu")
                .custom(hashmap! {attr.into() => "match".into()})
                .build();
            let non_matching_user = User::with_key("nmu")
                .custom(hashmap! {attr.into() => "nope".into()})
                .build();
            let user_without_attr = User::with_key("uwa")
                .custom(hashmap! {attr.into() => AttributeValue::Null})
                .build();

            assert!(
                clause.matches(&matching_user, &FeatureStore::new()),
                "should match {}",
                attr
            );
            assert!(
                !clause.matches(&non_matching_user, &FeatureStore::new()),
                "should not match non-matching {}",
                attr
            );
            assert!(
                !clause.matches(&user_without_attr, &FeatureStore::new()),
                "should not match user with null {}",
                attr
            );
        }
    }

    #[test]
    fn variation_index_for_user() {
        const HASH_KEY: &str = "hashKey";
        const SALT: &str = "saltyA";

        let wv1 = WeightedVariation {
            variation: 0,
            weight: 60_000.0,
        };
        let wv2 = WeightedVariation {
            variation: 1,
            weight: 40_000.0,
        };
        let rollout = VariationOrRollout::Rollout {
            bucket_by: None,
            variations: vec![wv1, wv2],
        };

        asserting!("userKeyA should get variation 0")
            .that(&rollout.variation(HASH_KEY, &User::with_key("userKeyA").build(), SALT))
            .contains_value(0);
        asserting!("userKeyB should get variation 1")
            .that(&rollout.variation(HASH_KEY, &User::with_key("userKeyB").build(), SALT))
            .contains_value(1);
        asserting!("userKeyC should get variation 0")
            .that(&rollout.variation(HASH_KEY, &User::with_key("userKeyC").build(), SALT))
            .contains_value(0);
    }
}
