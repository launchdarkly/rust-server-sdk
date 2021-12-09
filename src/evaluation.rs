use super::data_store::DataStore;
use serde::Serialize;

use rust_server_sdk_evaluation::{evaluate, FlagValue, Reason, User};
use std::collections::HashMap;
use std::time::SystemTime;

/// Configuration struct to control the type of data returned from the [Client::all_flags_detail]
/// method. By default, each of the options default to false. However, you can selectively enable
/// them by calling the appropriate functions.
///
/// ```
/// # use launchdarkly_server_sdk::FlagDetailConfig;
/// # fn main() {
///     let mut config = FlagDetailConfig::new();
///     config.client_side_only()
///         .with_reasons()
///         .details_only_for_tracked_flags();
/// # }
/// ```
#[derive(Clone, Copy, Default)]
pub struct FlagDetailConfig {
    client_side_only: bool,
    with_reasons: bool,
    details_only_for_tracked_flags: bool,
}

impl FlagDetailConfig {
    pub fn new() -> Self {
        Self {
            client_side_only: false,
            with_reasons: false,
            details_only_for_tracked_flags: false,
        }
    }

    /// Limit to only flags that are marked for use with the client-side SDK (by
    /// default, all flags are included)
    pub fn client_side_only(&mut self) -> &mut Self {
        self.client_side_only = true;
        self
    }

    /// Include evaluation reasons in the state
    pub fn with_reasons(&mut self) -> &mut Self {
        self.with_reasons = true;
        self
    }

    /// Omit any metadata that is normally only used for event generation, such as flag versions
    /// and evaluation reasons, unless the flag has event tracking or debugging turned on
    pub fn details_only_for_tracked_flags(&mut self) -> &mut Self {
        self.details_only_for_tracked_flags = true;
        self
    }
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FlagState {
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    variation: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<Reason>,

    #[serde(skip_serializing_if = "std::ops::Not::not")]
    track_events: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    debug_events_until_date: Option<u64>,
}

#[derive(Serialize)]
pub struct FlagDetail {
    #[serde(flatten)]
    evaluations: HashMap<String, Option<FlagValue>>,

    #[serde(rename = "$flagState")]
    flag_state: HashMap<String, FlagState>,

    #[serde(rename = "$valid")]
    valid: bool,
}

impl FlagDetail {
    pub fn new(valid: bool) -> Self {
        Self {
            evaluations: HashMap::new(),
            flag_state: HashMap::new(),
            valid,
        }
    }

    pub fn populate(&mut self, store: &dyn DataStore, user: &User, config: FlagDetailConfig) {
        let mut evaluations = HashMap::new();
        let mut flag_state = HashMap::new();

        for (key, flag) in store.all_flags() {
            if config.client_side_only && !flag.client_side_availability.using_environment_id {
                continue;
            }

            let detail = evaluate(&*store.to_store(), flag, user, None);

            let with_details = match !config.details_only_for_tracked_flags || flag.track_events {
                true => true,
                false => match flag.debug_events_until_date {
                    Some(time) => {
                        let today = SystemTime::now();
                        let today_millis = today
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        (time as u128) > today_millis
                    }
                    None => false,
                },
            };

            let reason = match config.with_reasons && with_details {
                true => Some(detail.reason),
                false => None,
            };

            let version = match with_details {
                true => Some(flag.version),
                false => None,
            };

            evaluations.insert(key.clone(), detail.value.cloned());

            flag_state.insert(
                key,
                FlagState {
                    version,
                    variation: detail.variation_index,
                    reason,
                    track_events: flag.track_events,
                    debug_events_until_date: flag.debug_events_until_date,
                },
            );
        }

        self.evaluations = evaluations;
        self.flag_state = flag_state;
    }
}

#[cfg(test)]
mod tests {
    use rust_server_sdk_evaluation::User;

    use crate::data_store::PatchTarget;
    use crate::data_store::{DataStore, InMemoryDataStore};
    use crate::evaluation::FlagDetail;
    use crate::test_common::basic_flag;
    use crate::FlagDetailConfig;

    #[test]
    fn flag_detail_handles_default_configuration() {
        let user = User::with_key("bob").build();
        let mut store = InMemoryDataStore::new();

        store
            .patch("/flags/myFlag", PatchTarget::Flag(basic_flag("myFlag")))
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &user, FlagDetailConfig::new());

        let expected = json!({
            "myFlag": true,
            "$flagState": {
                "myFlag": {
                    "version": 42,
                    "variation": 1
                }
            },
            "$valid": true
        });

        assert_eq!(
            serde_json::to_string_pretty(&flag_detail).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap(),
        );
    }

    #[test]
    fn flag_detail_with_reasons_should_include_reason() {
        let user = User::with_key("bob").build();
        let mut store = InMemoryDataStore::new();

        store
            .patch("/flags/myFlag", PatchTarget::Flag(basic_flag("myFlag")))
            .expect("patch should apply");

        let mut config = FlagDetailConfig::new();
        config.with_reasons();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &user, config);

        let expected = json!({
            "myFlag": true,
            "$flagState": {
                "myFlag": {
                    "version": 42,
                    "variation": 1,
                    "reason": {
                        "kind": "FALLTHROUGH"
                    }
                }
            },
            "$valid": true
        });

        assert_eq!(
            serde_json::to_string_pretty(&flag_detail).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap(),
        );
    }

    #[test]
    fn flag_detail_details_only_should_exclude_version_and_reason() {
        let user = User::with_key("bob").build();
        let mut store = InMemoryDataStore::new();

        store
            .patch("/flags/myFlag", PatchTarget::Flag(basic_flag("myFlag")))
            .expect("patch should apply");

        let mut config = FlagDetailConfig::new();
        config.details_only_for_tracked_flags();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &user, config);

        let expected = json!({
            "myFlag": true,
            "$flagState": {
                "myFlag": {
                    "variation": 1,
                }
            },
            "$valid": true
        });

        assert_eq!(
            serde_json::to_string_pretty(&flag_detail).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap(),
        );
    }

    #[test]
    fn flag_detail_details_only_with_tracked_events_includes_version() {
        let user = User::with_key("bob").build();
        let mut store = InMemoryDataStore::new();
        let mut flag = basic_flag("myFlag");
        flag.track_events = true;

        store
            .patch("/flags/myFlag", PatchTarget::Flag(flag))
            .expect("patch should apply");

        let mut config = FlagDetailConfig::new();
        config.details_only_for_tracked_flags();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &user, config);

        let expected = json!({
            "myFlag": true,
            "$flagState": {
                "myFlag": {
                    "version": 42,
                    "variation": 1,
                    "trackEvents": true,
                }
            },
            "$valid": true
        });

        assert_eq!(
            serde_json::to_string_pretty(&flag_detail).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap(),
        );
    }

    #[test]
    fn flag_detail_with_default_config_but_tracked_event_should_include_version() {
        let user = User::with_key("bob").build();
        let mut store = InMemoryDataStore::new();
        let mut flag = basic_flag("myFlag");
        flag.track_events = true;

        store
            .patch("/flags/myFlag", PatchTarget::Flag(flag))
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &user, FlagDetailConfig::new());

        let expected = json!({
            "myFlag": true,
            "$flagState": {
                "myFlag": {
                    "version": 42,
                    "variation": 1,
                    "trackEvents": true,
                }
            },
            "$valid": true
        });

        assert_eq!(
            serde_json::to_string_pretty(&flag_detail).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap(),
        );
    }
}
