#![cfg(test)]

use launchdarkly_server_sdk_evaluation::{Flag, Segment};

use crate::Stage;

pub const FLOAT_TO_INT_MAX: i64 = 9007199254740991;

pub fn basic_flag(key: &str) -> Flag {
    basic_flag_with_visibility(key, false, false)
}

pub fn basic_flag_with_visibility(
    key: &str,
    visible_to_environment_id: bool,
    visible_to_mobile_key: bool,
) -> Flag {
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
                "usingMobileKey": {},
                "usingEnvironmentId": {}
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string()),
        visible_to_mobile_key,
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
    basic_flag_with_prereqs_and_visibility(key, &[prereq_key], false, false)
}

pub fn basic_flag_with_prereqs_and_visibility(
    key: &str,
    prereq_keys: &[&str],
    visible_to_environment_id: bool,
    visible_to_mobile_key: bool,
) -> Flag {
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
                "usingMobileKey": {},
                "usingEnvironmentId": {}
            }},
            "salt": "kosher"
        }}"#,
        serde_json::Value::String(key.to_string()),
        prereqs_json,
        visible_to_mobile_key,
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

/// A tombstone body as one of our SDKs writes it into a persistent store.
pub struct TombstoneShape {
    /// Which SDKs write this shape. Used in test failure messages.
    pub name: &'static str,
    /// The JSON body as it sits in the store.
    pub body: String,
    /// The version a reader must report for this tombstone.
    pub version: u64,
}

/// The version carried by every tombstone that [flag_tombstone_shapes] and
/// [segment_tombstone_shapes] return.
pub const TOMBSTONE_VERSION: u64 = 100;

/// Returns every tombstone body our SDKs write for a deleted flag.
///
/// A persistent store addresses the record by key already, so the key inside the body is
/// redundant and the SDKs disagree on what to put there. Any SDK can read a store that
/// another SDK wrote, so a reader has to accept all of these shapes.
pub fn flag_tombstone_shapes(key: &str) -> Vec<TombstoneShape> {
    let quoted_key = serde_json::Value::String(key.to_string());

    let full_item = |item_key: &serde_json::Value| {
        format!(
            r#"{{
                "key": {item_key},
                "version": {TOMBSTONE_VERSION},
                "on": false,
                "targets": [],
                "rules": [],
                "prerequisites": [],
                "fallthrough": {{"variation": 0}},
                "offVariation": 0,
                "variations": [false, true],
                "salt": "kosher",
                "deleted": true
            }}"#
        )
    };

    tombstone_shapes(
        &quoted_key,
        full_item(&quoted_key),
        full_item(&placeholder_key()),
    )
}

/// Returns every tombstone body our SDKs write for a deleted segment. See
/// [flag_tombstone_shapes].
pub fn segment_tombstone_shapes(key: &str) -> Vec<TombstoneShape> {
    let quoted_key = serde_json::Value::String(key.to_string());

    let full_item = |item_key: &serde_json::Value| {
        format!(
            r#"{{
                "key": {item_key},
                "version": {TOMBSTONE_VERSION},
                "included": [],
                "excluded": [],
                "rules": [],
                "salt": "salty",
                "deleted": true
            }}"#
        )
    };

    tombstone_shapes(
        &quoted_key,
        full_item(&quoted_key),
        full_item(&placeholder_key()),
    )
}

/// The placeholder key Go, the Relay Proxy and Rust write in place of the real item key.
fn placeholder_key() -> serde_json::Value {
    serde_json::Value::String("$deleted".to_string())
}

fn tombstone_shapes(
    quoted_key: &serde_json::Value,
    full_item_with_item_key: String,
    full_item_with_placeholder_key: String,
) -> Vec<TombstoneShape> {
    let shape = |name, body| TombstoneShape {
        name,
        body,
        version: TOMBSTONE_VERSION,
    };

    vec![
        shape(
            "keyless (.NET, Java, Node upsert, Haskell)",
            format!(r#"{{"version": {TOMBSTONE_VERSION}, "deleted": true}}"#),
        ),
        shape(
            "item key (Python, Ruby, C++, Erlang, Node init)",
            format!(r#"{{"key": {quoted_key}, "version": {TOMBSTONE_VERSION}, "deleted": true}}"#),
        ),
        shape(
            "placeholder key (Rust)",
            format!(r#"{{"version": {TOMBSTONE_VERSION}, "key": "$deleted", "deleted": true}}"#),
        ),
        shape("full item, item key", full_item_with_item_key),
        shape(
            "full item, placeholder key (Go, Relay Proxy)",
            full_item_with_placeholder_key,
        ),
    ]
}
