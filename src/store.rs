use std::collections::HashMap;

use super::eval::{self, Detail, Reason, VariationIndex};
use super::users::{AttributeValue, User};

use serde::{Deserialize, Serialize};

const FLAGS_PREFIX: &'static str = "/flags/";

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
    // TODO actually implement these
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
    // TODO implement other matching operations
}

impl Op {
    fn matches(&self, lhs: &AttributeValue, rhs: &AttributeValue) -> bool {
        match self {
            Op::In => lhs == rhs,
            Op::StartsWith
                | Op::EndsWith
                | Op::Contains
                | Op::Matches
                | Op::LessThan
                | Op::LessThanOrEqual
                | Op::GreaterThan
                | Op::GreaterThanOrEqual
                | Op::Before
                | Op::After
                | Op::SegmentMatch
                | Op::SemVerEqual
                | Op::SemVerGreaterThan
                | Op::SemVerLessThan
                => {
                error!("Encountered unimplemented flag rule operation {:?}", self);
                false
            }
        }
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
        return true;
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
        if user.key.is_none() {
            return Detail::err(eval::Error::UserNotSpecified);
        }

        if !self.on {
            return self.off_value(Reason::Off);
        }

        for target in &self.targets {
            for value in &target.values {
                if Some(value) == user.key.as_ref() {
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
            .unwrap_or(Detail::err(eval::Error::MalformedFlag))
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

#[cfg(test)]
mod tests {
    use spectral::prelude::*;

    use super::FlagValue::*;
    use super::*;

    use crate::eval::Reason::*;
    use crate::users::User;

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
        let alice = User::new("alice"); // not targeted
        let bob = User::new("bob"); // targeted
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
        let nameless = User::new_without_key(); // untargetable
        let mut flag: FeatureFlag = serde_json::from_str(TEST_FLAG_JSON).unwrap();

        assert!(!flag.on);
        let detail = flag.evaluate(&nameless);
        assert_that!(detail.value).is_none();
        assert_that!(detail.variation_index).is_none();
        assert_that!(detail.reason).is_equal_to(&Reason::Error(eval::Error::UserNotSpecified));

        // flip targeting on
        flag.on = true;
        let detail = flag.evaluate(&nameless);
        assert_that!(detail.value).is_none();
        assert_that!(detail.variation_index).is_none();
        assert_that!(detail.reason).is_equal_to(&Reason::Error(eval::Error::UserNotSpecified));
    }

    #[test]
    fn test_eval_flag_rules() {
        let alice = User::new("alice");
        let bob = User::new_with_custom(
            "bob",
            hashmap! {
                "team".into() => "Avengers".into(),
            },
        );

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

    #[test]
    fn test_ops() {
        use AttributeValue::String as AString;

        assert!(Op::In.matches(&AString("foo".into()), &AString("foo".into())));

        assert!(!Op::In.matches(&AString("foo".into()), &AString("bar".into())));
        assert!(
            !Op::In.matches(&AString("Foo".into()), &AString("foo".into())),
            "case sensitive"
        );

        // TODO test anything other than strings
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

        let matching_user = User::new_with_custom("mu", hashmap! {"a".into() => "foo".into()});
        let non_matching_user = User::new_with_custom("nmu", hashmap! {"a".into() => "lol".into()});
        let user_without_attr = User::new("uwa");

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

        let user_with_many = User::new_with_custom(
            "uwm",
            hashmap! {"a".into() => vec!["foo", "bar", "lol"].into()},
        );

        assert!(one_val_clause.matches(&user_with_many));
        assert!(many_val_clause.matches(&user_with_many));

        assert!(!negated_clause.matches(&user_with_many));
        assert!(!negated_many_val_clause.matches(&user_with_many));
    }
}
