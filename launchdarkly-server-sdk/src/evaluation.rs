use super::stores::store::DataStore;
use serde::Serialize;
use std::cell::RefCell;

use launchdarkly_server_sdk_evaluation::{
    evaluate, Context, FlagValue, PrerequisiteEvent, PrerequisiteEventRecorder, Reason,
};
use std::collections::HashMap;
use std::time::SystemTime;

/// Configuration struct to control the type of data returned from the [crate::Client::all_flags_detail]
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
    /// Create a [FlagDetailConfig] with default values.
    ///
    /// By default, this config will include al flags and will not include reasons.
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

#[derive(Serialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FlagState {
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    variation: Option<isize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<Reason>,

    #[serde(skip_serializing_if = "std::ops::Not::not")]
    track_events: bool,

    #[serde(skip_serializing_if = "std::ops::Not::not")]
    track_reason: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    debug_events_until_date: Option<u64>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    prerequisites: Vec<String>,
}

/// FlagDetail is a snapshot of the state of multiple feature flags with regard to a specific user.
/// This is the return type of [crate::Client::all_flags_detail].
///
/// Serializing this object to JSON will produce the appropriate data structure for bootstrapping
/// the LaunchDarkly JavaScript client.
#[derive(Serialize, Clone, Debug)]
pub struct FlagDetail {
    #[serde(flatten)]
    evaluations: HashMap<String, Option<FlagValue>>,

    #[serde(rename = "$flagsState")]
    flag_state: HashMap<String, FlagState>,

    #[serde(rename = "$valid")]
    valid: bool,
}

/// DirectPrerequisiteRecorder records only the direct (top-level) prerequisites of a
/// flag.
struct DirectPrerequisiteRecorder {
    target_flag_key: String,
    prerequisites: RefCell<Vec<String>>,
}

impl DirectPrerequisiteRecorder {
    /// Creates a new instance of [DirectPrerequisiteRecorder] for a given target flag. The
    /// direct prerequisites of the flag will be available in the prerequisites field of the
    /// recorder.
    pub fn new(target_flag_key: impl Into<String>) -> Self {
        Self {
            target_flag_key: target_flag_key.into(),
            prerequisites: RefCell::new(Vec::new()),
        }
    }
}
impl PrerequisiteEventRecorder for DirectPrerequisiteRecorder {
    fn record(&self, event: PrerequisiteEvent) {
        if event.target_flag_key == self.target_flag_key {
            self.prerequisites
                .borrow_mut()
                .push(event.prerequisite_flag.key)
        }
    }
}

impl FlagDetail {
    /// Create a new empty instance of FlagDetail.
    pub fn new(valid: bool) -> Self {
        Self {
            evaluations: HashMap::new(),
            flag_state: HashMap::new(),
            valid,
        }
    }

    /// Populate the FlagDetail struct with the results of every flag found within the provided
    /// store, evaluated for the specified context.
    pub fn populate(&mut self, store: &dyn DataStore, context: &Context, config: FlagDetailConfig) {
        let mut evaluations = HashMap::new();
        let mut flag_state = HashMap::new();

        for (key, flag) in store.all_flags() {
            if config.client_side_only && !flag.using_environment_id() {
                continue;
            }

            let event_recorder = DirectPrerequisiteRecorder::new(key.clone());

            let detail = evaluate(store.to_store(), &flag, context, Some(&event_recorder));

            // Here we are applying the same logic used in EventFactory.new_feature_request_event
            // to determine whether the evaluation involved an experiment, in which case both
            // track_events and track_reason should be overridden.
            let require_experiment_data = flag.is_experimentation_enabled(&detail.reason);
            let track_events = flag.track_events || require_experiment_data;
            let track_reason = require_experiment_data;

            let currently_debugging = match flag.debug_events_until_date {
                Some(time) => {
                    let today = SystemTime::now();
                    let today_millis = today
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    (time as u128) > today_millis
                }
                None => false,
            };

            let mut omit_details = false;
            if config.details_only_for_tracked_flags
                && !(track_events
                    || track_reason
                    || flag.debug_events_until_date.is_some() && currently_debugging)
            {
                omit_details = true;
            }

            let mut reason = if !config.with_reasons && !track_reason {
                None
            } else {
                Some(detail.reason)
            };

            let mut version = Some(flag.version);
            if omit_details {
                reason = None;
                version = None;
            }

            evaluations.insert(key.clone(), detail.value.cloned());

            flag_state.insert(
                key,
                FlagState {
                    version,
                    variation: detail.variation_index,
                    reason,
                    track_events,
                    track_reason,
                    debug_events_until_date: flag.debug_events_until_date,
                    prerequisites: event_recorder.prerequisites.take(),
                },
            );
        }

        self.evaluations = evaluations;
        self.flag_state = flag_state;
    }
}

#[cfg(test)]
mod tests {
    use crate::evaluation::FlagDetail;
    use crate::stores::store::DataStore;
    use crate::stores::store::InMemoryDataStore;
    use crate::stores::store_types::{PatchTarget, StorageItem};
    use crate::test_common::{
        basic_flag, basic_flag_with_prereqs_and_visibility, basic_flag_with_visibility,
        basic_off_flag,
    };
    use crate::FlagDetailConfig;
    use assert_json_diff::assert_json_eq;
    use launchdarkly_server_sdk_evaluation::ContextBuilder;

    #[test]
    fn flag_detail_handles_default_configuration() {
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();

        store
            .upsert(
                "myFlag",
                PatchTarget::Flag(StorageItem::Item(basic_flag("myFlag"))),
            )
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &context, FlagDetailConfig::new());

        let expected = json!({
            "myFlag": true,
            "$flagsState": {
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
    fn flag_detail_handles_experimentation_reasons_correctly() {
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();

        let mut flag = basic_flag("myFlag");
        flag.track_events = false;
        flag.track_events_fallthrough = true;

        store
            .upsert("myFlag", PatchTarget::Flag(StorageItem::Item(flag)))
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &context, FlagDetailConfig::new());

        let expected = json!({
            "myFlag": true,
            "$flagsState": {
                "myFlag": {
                    "version": 42,
                    "variation": 1,
                    "reason": {
                        "kind": "FALLTHROUGH",
                    },
                    "trackEvents": true,
                    "trackReason": true,
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
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();

        store
            .upsert(
                "myFlag",
                PatchTarget::Flag(StorageItem::Item(basic_flag("myFlag"))),
            )
            .expect("patch should apply");

        let mut config = FlagDetailConfig::new();
        config.with_reasons();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &context, config);

        let expected = json!({
            "myFlag": true,
            "$flagsState": {
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
    fn flag_detail_details_only_should_exclude_reason() {
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();

        store
            .upsert(
                "myFlag",
                PatchTarget::Flag(StorageItem::Item(basic_flag("myFlag"))),
            )
            .expect("patch should apply");

        let mut config = FlagDetailConfig::new();
        config.details_only_for_tracked_flags();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &context, config);

        let expected = json!({
            "myFlag": true,
            "$flagsState": {
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
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();
        let mut flag = basic_flag("myFlag");
        flag.track_events = true;

        store
            .upsert("myFlag", PatchTarget::Flag(StorageItem::Item(flag)))
            .expect("patch should apply");

        let mut config = FlagDetailConfig::new();
        config.details_only_for_tracked_flags();

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &context, config);

        let expected = json!({
            "myFlag": true,
            "$flagsState": {
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
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();
        let mut flag = basic_flag("myFlag");
        flag.track_events = true;

        store
            .upsert("myFlag", PatchTarget::Flag(StorageItem::Item(flag)))
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &context, FlagDetailConfig::new());

        let expected = json!({
            "myFlag": true,
            "$flagsState": {
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
    fn flag_prerequisites_should_be_exposed() {
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();

        let prereq1 = basic_flag("prereq1");
        let prereq2 = basic_flag("prereq2");
        let toplevel =
            basic_flag_with_prereqs_and_visibility("toplevel", &["prereq1", "prereq2"], false);

        store
            .upsert("prereq1", PatchTarget::Flag(StorageItem::Item(prereq1)))
            .expect("patch should apply");

        store
            .upsert("prereq2", PatchTarget::Flag(StorageItem::Item(prereq2)))
            .expect("patch should apply");

        store
            .upsert("toplevel", PatchTarget::Flag(StorageItem::Item(toplevel)))
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);
        flag_detail.populate(&store, &context, FlagDetailConfig::new());

        let expected = json!({
            "prereq1": true,
            "prereq2": true,
            "toplevel": true,
            "$flagsState": {
                "toplevel": {
                    "version": 42,
                    "variation": 1,
                    "prerequisites": ["prereq1", "prereq2"]
                },
                "prereq2": {
                    "version": 42,
                    "variation": 1
                },
                "prereq1": {
                    "version": 42,
                    "variation": 1,
                },
            },
            "$valid": true
        });

        assert_json_eq!(expected, flag_detail);
    }

    #[test]
    fn flag_prerequisites_should_be_exposed_even_if_not_available_to_clients() {
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();

        // These two prerequisites won't be visible to clients (environment ID) SDKs.
        let prereq1 = basic_flag_with_visibility("prereq1", false);
        let prereq2 = basic_flag_with_visibility("prereq2", false);

        // But, the top-level flag will.
        let toplevel =
            basic_flag_with_prereqs_and_visibility("toplevel", &["prereq1", "prereq2"], true);

        store
            .upsert("prereq1", PatchTarget::Flag(StorageItem::Item(prereq1)))
            .expect("patch should apply");

        store
            .upsert("prereq2", PatchTarget::Flag(StorageItem::Item(prereq2)))
            .expect("patch should apply");

        store
            .upsert("toplevel", PatchTarget::Flag(StorageItem::Item(toplevel)))
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);

        let mut config = FlagDetailConfig::new();
        config.client_side_only();

        flag_detail.populate(&store, &context, config);

        // Even though the two prereqs are omitted, we should still see their metadata in the
        // toplevel flag.
        let expected = json!({
            "toplevel": true,
            "$flagsState": {
                "toplevel": {
                    "version": 42,
                    "variation": 1,
                    "prerequisites": ["prereq1", "prereq2"]
                },
            },
            "$valid": true
        });

        assert_json_eq!(expected, flag_detail);
    }

    #[test]
    fn flag_prerequisites_should_be_in_evaluation_order() {
        let context = ContextBuilder::new("bob")
            .build()
            .expect("Failed to create context");
        let mut store = InMemoryDataStore::new();

        // Since prereq1 will be listed as the first prerequisite, and it is off,
        // evaluation will short circuit and we shouldn't see the second prerequisite.
        let prereq1 = basic_off_flag("prereq1");
        let prereq2 = basic_flag("prereq2");

        let toplevel =
            basic_flag_with_prereqs_and_visibility("toplevel", &["prereq1", "prereq2"], true);

        store
            .upsert("prereq1", PatchTarget::Flag(StorageItem::Item(prereq1)))
            .expect("patch should apply");

        store
            .upsert("prereq2", PatchTarget::Flag(StorageItem::Item(prereq2)))
            .expect("patch should apply");

        store
            .upsert("toplevel", PatchTarget::Flag(StorageItem::Item(toplevel)))
            .expect("patch should apply");

        let mut flag_detail = FlagDetail::new(true);

        flag_detail.populate(&store, &context, FlagDetailConfig::new());

        let expected = json!({
            "prereq1": null,
            "prereq2": true,
            "toplevel": false,
            "$flagsState": {
                "toplevel": {
                    "version": 42,
                    "variation": 0,
                    "prerequisites": ["prereq1"]
                },
                "prereq2": {
                    "version": 42,
                    "variation": 1
                },
                "prereq1": {
                    "version": 42
                }

            },
            "$valid": true
        });

        assert_json_eq!(expected, flag_detail);
    }
}
