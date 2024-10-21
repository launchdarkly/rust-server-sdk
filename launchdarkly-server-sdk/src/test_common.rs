#![cfg(test)]

use launchdarkly_server_sdk_evaluation::{Flag, Segment};

use crate::Stage;

pub const FLOAT_TO_INT_MAX: i64 = 9007199254740991;

pub fn basic_flag(key: &str) -> Flag {
    return basic_flag_with_visibility(key, false);
}

pub fn basic_flag_with_visibility(key: &str, visible_to_environment_id: bool) -> Flag {
    serde_json::from_str(&format!(
        r#"{{
            "key": {},
            "version": 42,
            "on": true,
            "targets": [],
            "rules": [],
            "prerequisites": [],
            "fallthrough": {{"variation": 1}},
            "offVariation": 0,
            "variations": [false, true],
            "clientSideAvailability": {{
                "usingMobileKey": false,
                "usingEnvironmentId": {}
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string()),
        visible_to_environment_id
    ))
        .unwrap()
}
pub fn basic_off_flag(key: &str) -> Flag {
    serde_json::from_str(&format!(
        r#"{{
            "key": {},
            "version": 42,
            "on": false,
            "targets": [],
            "rules": [],
            "prerequisites": [],
            "fallthrough": {{"variation": 1}},
            "offVariation": null,
            "variations": [false, true],
            "clientSideAvailability": {{
                "usingMobileKey": false,
                "usingEnvironmentId": false
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string())
    ))
    .unwrap()
}

pub fn basic_flag_with_prereq(key: &str, prereq_key: &str) -> Flag {
    basic_flag_with_prereqs_and_visibility(key, &[prereq_key], false)
}



pub fn basic_flag_with_prereqs_and_visibility(key: &str, prereq_keys: &[&str], visible_to_environment_id: bool) -> Flag {
    let prereqs_json: String = prereq_keys
        .iter()
        .map(|&prereq_key| {
            format!(
                r#"{{"key": {}, "variation": 1}}"#,
                serde_json::Value::String(prereq_key.to_string())
            )
        })
        .collect::<Vec<String>>()
        .join(",");

    serde_json::from_str(&format!(
        r#"{{
            "key": {},
            "version": 42,
            "on": true,
            "targets": [],
            "rules": [],
            "prerequisites": [{}],
            "fallthrough": {{"variation": 1}},
            "offVariation": 0,
            "variations": [false, true],
            "clientSideAvailability": {{
                "usingMobileKey": false,
                "usingEnvironmentId": {}
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string()),
        prereqs_json,
        visible_to_environment_id
    ))
    .unwrap()
}

pub fn basic_int_flag(key: &str) -> Flag {
    serde_json::from_str(&format!(
        r#"{{
            "key": {},
            "version": 42,
            "on": true,
            "targets": [],
            "rules": [],
            "prerequisites": [],
            "fallthrough": {{"variation": 1}},
            "offVariation": 0,
            "variations": [0, {}],
            "clientSideAvailability": {{
                "usingMobileKey": false,
                "usingEnvironmentId": false
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string()),
        FLOAT_TO_INT_MAX,
    ))
    .unwrap()
}

pub fn basic_migration_flag(key: &str, stage: Stage) -> Flag {
    let variation_index = match stage {
        Stage::Off => 0,
        Stage::DualWrite => 1,
        Stage::Shadow => 2,
        Stage::Live => 3,
        Stage::Rampdown => 4,
        Stage::Complete => 5,
    };

    serde_json::from_str(&format!(
        r#"{{
            "key": {},
            "version": 42,
            "on": true,
            "targets": [],
            "rules": [],
            "prerequisites": [],
            "fallthrough": {{"variation": {}}},
            "offVariation": 0,
            "variations": ["off", "dualwrite", "shadow", "live", "rampdown", "complete"],
            "clientSideAvailability": {{
                "usingMobileKey": false,
                "usingEnvironmentId": false
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string()),
        variation_index
    ))
    .unwrap()
}

pub fn basic_segment(key: &str) -> Segment {
    serde_json::from_str(&format!(
        r#"{{
            "key": {},
            "included": ["alice"],
            "excluded": [],
            "rules": [],
            "salt": "salty",
            "version": 1
        }}"#,
        serde_json::Value::String(key.to_string())
    ))
    .unwrap()
}
