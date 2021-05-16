#![cfg(test)]

use rust_server_sdk_evaluation::Flag;

pub const FLOAT_TO_INT_MAX: i64 = 9007199254740991;

pub fn basic_flag(key: &str) -> Flag {
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
                "usingEnvironmentId": false
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string())
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

pub fn basic_json_flag(key: &str) -> Flag {
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
            "variations": [null, {{"foo": "bar"}}],
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
