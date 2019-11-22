use std::collections::HashMap;

use super::eval::{self, Detail, Reason, VariationIndex};
use super::users::{AttributeValue, User};

use chrono::{self, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};

const FLAGS_PREFIX: &str = "/flags/";

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum FlagValue {
    Bool(bool),
    Str(String),
    Float(f64),
    // TODO implement other variation types
    NotYetImplemented(serde_json::Value),
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
        // TODO this has undefined behaviour for huge floats: https://stackoverflow.com/a/41139453
        self.as_float().map(|f| f.round() as i64)
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
    // TODO actually implement these
    SegmentMatch,
    SemVerEqual,
    SemVerGreaterThan,
    SemVerLessThan,
    // TODO implement other matching operations
}

impl Op {
    fn matches(&self, lhs: &AttributeValue, rhs: &AttributeValue) -> bool {
        match self {
            Op::In => lhs == rhs,

            // string ops
            Op::StartsWith => string_op(lhs, rhs, |l, r| l.starts_with(r)),
            Op::EndsWith => string_op(lhs, rhs, |l, r| l.ends_with(r)),
            Op::Contains => string_op(lhs, rhs, |l, r| l.find(r).is_some()),
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
            Op::SegmentMatch | Op::SemVerEqual | Op::SemVerGreaterThan | Op::SemVerLessThan => {
                error!("Encountered unimplemented flag rule operation {:?}", self);
                false
            }
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

#[derive(Clone, Debug, Deserialize)]
struct Clause {
    attribute: String,
    negate: bool,
    op: Op,
    values: Vec<AttributeValue>,
}

impl Clause {
    fn matches(&self, user: &User) -> bool {
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

        if self.negate {
            any_match.is_none()
        } else {
            any_match.is_some()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Rule {
    clauses: Vec<Clause>,
    #[serde(flatten)]
    variation_or_rollout: VariationOrRollout,
}

impl Rule {
    fn matches(&self, user: &User) -> bool {
        // rules match if _all_ of their clauses do
        for clause in &self.clauses {
            if !clause.matches(user) {
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
pub enum PatchTarget {
    Flag(FeatureFlag),
    // TODO support segments too
    Other(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
enum VariationOrRollout {
    Variation(VariationIndex),
    Rollout(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureFlag {
    pub key: String,
    pub version: u64,

    on: bool,
    targets: Vec<Target>,
    rules: Vec<Rule>,
    fallthrough: VariationOrRolloutOrMalformed,
    off_variation: Option<VariationIndex>,
    variations: Vec<FlagValue>,
    // TODO implement more flag fields
}

impl FeatureFlag {
    pub fn evaluate(&self, user: &User) -> Detail<&FlagValue> {
        if user.key().is_none() {
            return Detail::err(eval::Error::UserNotSpecified);
        }

        if !self.on {
            return self.off_value(Reason::Off);
        }

        for target in &self.targets {
            for value in &target.values {
                if Some(value) == user.key() {
                    return self.variation(target.variation, Reason::TargetMatch);
                }
            }
        }

        for rule in &self.rules {
            if rule.matches(&user) {
                return self
                    .value_for_variation_or_rollout(&rule.variation_or_rollout, Reason::RuleMatch);
            }
        }

        // just return the fallthrough for now
        self.fallthrough
            // TODO ugh, clean this up
            .get()
            .as_ref()
            .ok()
            .map(|vor| self.value_for_variation_or_rollout(vor, Reason::Fallthrough))
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
        vr: &VariationOrRollout, /*, TODO user*/
        reason: Reason,
    ) -> Detail<&FlagValue> {
        match vr {
            VariationOrRollout::Variation(index) => self.variation(*index, reason),
            VariationOrRollout::Rollout(json) => {
                error!("Rollout not yet implemented: {:?}", json);
                Detail::err(eval::Error::Exception)
            }
        }
    }
}

pub type Segment = serde_json::Value; // TODO

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

    pub fn flag(&self, flag_name: &str) -> Option<&FeatureFlag> {
        self.data.flags.get(flag_name)
    }

    pub fn all_flags(&self) -> &HashMap<String, FeatureFlag> {
        &self.data.flags
    }

    pub fn patch(&mut self, path: &str, data: PatchTarget) {
        if !path.starts_with(FLAGS_PREFIX) {
            error!("Ignoring patch for {}, can only patch flags atm", path);
            return;
        }
        let flag = match data {
            PatchTarget::Flag(f) => f,
            PatchTarget::Other(json) => {
                error!("Couldn't parse JSON as a flag to patch {}: {}", path, json);
                return;
            }
        };

        let flag_name = &path[FLAGS_PREFIX.len()..];
        self.data.flags.insert(flag_name.to_string(), flag);
    }

    pub fn delete(&mut self, path: &str) {
        if !path.starts_with(FLAGS_PREFIX) {
            error!("Ignoring delete for {}, can only delete flags atm", path);
            return;
        }

        let flag_name = &path[FLAGS_PREFIX.len()..];
        self.data.flags.remove(flag_name);
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
    use crate::users::{User, UserBuilder};

    #[test]
    fn test_parse_variation_or_rollout() {
        let variation: VariationOrRolloutOrMalformed =
            serde_json::from_str("{\"variation\":4}").expect("should parse");
        assert_that!(variation.get()).is_ok_containing(&VariationOrRollout::Variation(4));

        let rollout: VariationOrRolloutOrMalformed = serde_json::from_str(
            "{\"rollout\":{\"variations\":[{\"variation\":1,\"weight\":100000}]}}",
        )
        .expect("should parse");
        assert_that!(rollout.get()).is_ok_containing(&VariationOrRollout::Rollout(
            json! { {"variations": [{"variation": 1, "weight": 100000}]} },
        ));

        let malformed: VariationOrRolloutOrMalformed =
            serde_json::from_str("{}").expect("should parse");
        assert_that!(malformed.get()).is_err();
    }

    const TEST_FLAG_JSON: &str = "{
        \"key\": \"test-flag\",
        \"version\": 1,
        \"on\": false,
        \"targets\": [
            {\"values\": [\"bob\"], \"variation\": 1}
        ],
        \"rules\": [],
        \"fallthrough\": {\"variation\": 0},
        \"offVariation\": 1,
        \"variations\": [true, false]
    }";

    const FLAG_WITH_RULES_JSON: &str = "{
        \"key\": \"with-rules\",
        \"version\": 1,
        \"on\": false,
        \"targets\": [],
        \"rules\": [
            {
                \"clauses\": [
                    {
                        \"attribute\": \"team\",
                        \"negate\": false,
                        \"op\": \"in\",
                        \"values\": [\"Avengers\"]
                    }
                ],
                \"id\": \"667e5007-01e4-4b51-9e33-5abe7f892790\",
                \"variation\": 1
            }
        ],
        \"fallthrough\": {\"variation\": 0},
        \"offVariation\": 1,
        \"variations\": [true, false]
    }";

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
    fn test_eval_flag_basic() {
        let alice = User::with_key("alice".into()).build(); // not targeted
        let bob = User::with_key("bob".into()).build(); // targeted
        let mut flag: FeatureFlag = serde_json::from_str(TEST_FLAG_JSON).unwrap();

        assert!(!flag.on);
        let detail = flag.evaluate(&alice);
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&Off);

        assert_that!(flag.evaluate(&bob)).is_equal_to(&detail);

        // flip off variation
        flag.off_variation = Some(0);
        let detail = flag.evaluate(&alice);
        assert_that!(detail.value).contains_value(&Bool(true));
        assert_that!(detail.variation_index).contains_value(0);

        // off variation unspecified
        flag.off_variation = None;
        let detail = flag.evaluate(&alice);
        assert_that!(detail.value).is_none();
        assert_that!(detail.variation_index).is_none();
        assert_that!(detail.reason).is_equal_to(&Off);

        // flip targeting on
        flag.on = true;
        let detail = flag.evaluate(&alice);
        assert_that!(detail.value).contains_value(&Bool(true));
        assert_that!(detail.variation_index).contains_value(0);
        assert_that!(detail.reason).is_equal_to(&Fallthrough);

        let detail = flag.evaluate(&bob);
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&TargetMatch);

        // flip default variation
        flag.fallthrough = VariationOrRollout::Variation(1).into();
        let detail = flag.evaluate(&alice);
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);

        // bob's reason should still be TargetMatch even though his value is now the default
        let detail = flag.evaluate(&bob);
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&TargetMatch);
    }

    #[test]
    fn test_eval_flag_missing_user_key() {
        let nameless = UserBuilder::new_with_optional_key(None).build(); // untargetable
        let mut flag: FeatureFlag = serde_json::from_str(TEST_FLAG_JSON).unwrap();

        assert!(!flag.on);
        let detail = flag.evaluate(&nameless);
        assert_that!(detail.value).is_none();
        assert_that!(detail.variation_index).is_none();
        assert_that!(detail.reason).is_equal_to(&Reason::Error {
            error: eval::Error::UserNotSpecified,
        });

        // flip targeting on
        flag.on = true;
        let detail = flag.evaluate(&nameless);
        assert_that!(detail.value).is_none();
        assert_that!(detail.variation_index).is_none();
        assert_that!(detail.reason).is_equal_to(&Reason::Error {
            error: eval::Error::UserNotSpecified,
        });
    }

    #[test]
    fn test_eval_flag_rules() {
        let alice = User::with_key("alice".into()).build();
        let bob = User::with_key("bob".into())
            .custom(hashmap! {
                "team".into() => "Avengers".into(),
            })
            .build();

        let mut flag: FeatureFlag = serde_json::from_str(FLAG_WITH_RULES_JSON).unwrap();

        assert!(!flag.on);
        for user in vec![&alice, &bob] {
            let detail = flag.evaluate(user);
            assert_that!(detail.value).contains_value(&Bool(false));
            assert_that!(detail.variation_index).contains_value(1);
            assert_that!(detail.reason).is_equal_to(&Off);
        }

        // flip targeting on
        flag.on = true;
        let detail = flag.evaluate(&alice);
        assert_that!(detail.value).contains_value(&Bool(true));
        assert_that!(detail.variation_index).contains_value(0);
        assert_that!(detail.reason).is_equal_to(&Fallthrough);

        let detail = flag.evaluate(&bob);
        assert_that!(detail.value).contains_value(&Bool(false));
        assert_that!(detail.variation_index).contains_value(1);
        assert_that!(detail.reason).is_equal_to(&RuleMatch);
    }

    fn astring(s: &str) -> AttributeValue {
        AttributeValue::String(s.into())
    }
    fn anum(f: f64) -> AttributeValue {
        AttributeValue::Number(f)
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
        assert!(Op::In.matches(&anum(42.0), &anum(42.0)));
        assert!(!Op::In.matches(&anum(42.0), &anum(3.0)));
        assert!(Op::In.matches(&anum(0.0), &anum(-0.0)));
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
        assert!(Op::LessThan.matches(&anum(0.0), &anum(1.0)));
        assert!(!Op::LessThan.matches(&anum(0.0), &anum(0.0)));
        assert!(!Op::LessThan.matches(&anum(1.0), &anum(0.0)));

        assert!(Op::GreaterThan.matches(&anum(1.0), &anum(0.0)));
        assert!(!Op::GreaterThan.matches(&anum(0.0), &anum(0.0)));
        assert!(!Op::GreaterThan.matches(&anum(0.0), &anum(1.0)));

        assert!(Op::LessThanOrEqual.matches(&anum(0.0), &anum(1.0)));
        assert!(Op::LessThanOrEqual.matches(&anum(0.0), &anum(0.0)));
        assert!(!Op::LessThanOrEqual.matches(&anum(1.0), &anum(0.0)));

        assert!(Op::GreaterThanOrEqual.matches(&anum(1.0), &anum(0.0)));
        assert!(Op::GreaterThanOrEqual.matches(&anum(0.0), &anum(0.0)));
        assert!(!Op::GreaterThanOrEqual.matches(&anum(0.0), &anum(1.0)));

        // conversions
        assert!(
            Op::LessThan.matches(&astring("0"), &anum(1.0)),
            "should convert numeric string on LHS"
        );
        assert!(
            Op::LessThan.matches(&anum(0.0), &astring("1")),
            "should convert numeric string on RHS"
        );

        assert!(
            !Op::LessThan.matches(&astring("Tuesday"), &anum(7.0)),
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
        assert!(Op::Before.matches(&anum(yesterday_millis), &anum(today_millis)));
        assert!(!Op::Before.matches(&anum(today_millis), &anum(yesterday_millis)));
        assert!(!Op::Before.matches(&anum(today_millis), &anum(today_millis)));

        assert!(Op::After.matches(&anum(today_millis), &anum(yesterday_millis)));
        assert!(!Op::After.matches(&anum(yesterday_millis), &anum(today_millis)));
        assert!(!Op::After.matches(&anum(today_millis), &anum(today_millis)));

        // numeric strings get converted as millis
        assert!(Op::Before.matches(&astring(&yesterday_millis.to_string()), &anum(today_millis)));
        assert!(Op::After.matches(&anum(today_millis), &astring(&yesterday_millis.to_string())));

        // date-formatted strings get parsed
        assert!(Op::Before.matches(
            &astring("2019-11-19T17:29:00.000000-07:00"),
            &anum(today_millis)
        ));
        assert!(
            Op::Before.matches(&astring("2019-11-19T17:29:00-07:00"), &anum(today_millis)),
            "fractional seconds part is optional"
        );

        assert!(Op::After.matches(
            &anum(today_millis),
            &astring("2019-11-19T17:29:00.000000-07:00")
        ));

        // nonsense strings don't match
        assert!(!Op::Before.matches(&astring("fish"), &anum(today_millis)));
        assert!(!Op::After.matches(&anum(today_millis), &astring("fish")));
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

        let matching_user = User::with_key("mu".into())
            .custom(hashmap! {"a".into() => "foo".into()})
            .build();
        let non_matching_user = User::with_key("nmu".into())
            .custom(hashmap! {"a".into() => "lol".into()})
            .build();
        let user_without_attr = User::with_key("uwa".into()).build();

        assert!(one_val_clause.matches(&matching_user));
        assert!(!one_val_clause.matches(&non_matching_user));
        assert!(!one_val_clause.matches(&user_without_attr));

        assert!(!negated_clause.matches(&matching_user));
        assert!(negated_clause.matches(&non_matching_user));

        assert!(
            !negated_clause.matches(&user_without_attr),
            "targeting missing attribute does not match even when negated"
        );

        assert!(
            many_val_clause.matches(&matching_user),
            "requires only one of the values"
        );
        assert!(!many_val_clause.matches(&non_matching_user));
        assert!(!many_val_clause.matches(&user_without_attr));

        assert!(
            !negated_many_val_clause.matches(&matching_user),
            "requires all values are missing"
        );
        assert!(negated_many_val_clause.matches(&non_matching_user));

        assert!(
            !negated_many_val_clause.matches(&user_without_attr),
            "targeting missing attribute does not match even when negated"
        );

        assert!(key_clause.matches(&matching_user), "should match key");
        assert!(
            !key_clause.matches(&non_matching_user),
            "should not match non-matching key"
        );

        let user_with_many = User::with_key("uwm".into())
            .custom(hashmap! {"a".into() => vec!["foo", "bar", "lol"].into()})
            .build();

        assert!(one_val_clause.matches(&user_with_many));
        assert!(many_val_clause.matches(&user_with_many));

        assert!(!negated_clause.matches(&user_with_many));
        assert!(!negated_many_val_clause.matches(&user_with_many));
    }

    struct AttributeTestCase {
        matching_user: User,
        non_matching_user: User,
        user_without_attr: User,
    }

    #[test]
    fn test_clause_matches_attributes() {
        let tests: HashMap<&str, AttributeTestCase> = hashmap! {
            "key" => AttributeTestCase {
                matching_user: User::with_key("match".into()).build(),
                non_matching_user: User::with_key("nope".into()).build(),
                user_without_attr: UserBuilder::new_with_optional_key(None).build(),
            },
            "secondary" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).secondary("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).secondary("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
            },
            "ip" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).ip("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).ip("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
            },
            "country" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).country("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).country("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
            },
            "email" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).email("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).email("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
            },
            "firstName" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).first_name("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).first_name("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
            },
            "lastName" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).last_name("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).last_name("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
            },
            "avatar" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).avatar("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).avatar("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
            },
            "name" => AttributeTestCase {
                matching_user: User::with_key("mu".into()).name("match".into()).build(),
                non_matching_user: User::with_key("nmu".into()).name("nope".into()).build(),
                user_without_attr: User::with_key("uwa".into()).build(),
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
                clause.matches(&test_case.matching_user),
                "should match {}",
                attr
            );
            assert!(
                !clause.matches(&test_case.non_matching_user),
                "should not match non-matching {}",
                attr
            );
            assert!(
                !clause.matches(&test_case.user_without_attr),
                "should not match user with null {}",
                attr
            );
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

        let anon_user = User::with_key("anon".into()).anonymous(true).build();
        let non_anon_user = User::with_key("nonanon".into()).anonymous(false).build();
        let implicitly_non_anon_user = User::with_key("implicit".into()).build();

        assert!(clause.matches(&anon_user));
        assert!(!clause.matches(&non_anon_user));
        assert!(!clause.matches(&implicitly_non_anon_user));
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

            let matching_user = User::with_key("mu".into())
                .custom(hashmap! {attr.into() => "match".into()})
                .build();
            let non_matching_user = User::with_key("nmu".into())
                .custom(hashmap! {attr.into() => "nope".into()})
                .build();
            let user_without_attr = User::with_key("uwa".into())
                .custom(hashmap! {attr.into() => AttributeValue::Null})
                .build();

            assert!(clause.matches(&matching_user), "should match {}", attr);
            assert!(
                !clause.matches(&non_matching_user),
                "should not match non-matching {}",
                attr
            );
            assert!(
                !clause.matches(&user_without_attr),
                "should not match user with null {}",
                attr
            );
        }
    }
}
