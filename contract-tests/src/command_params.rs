use launchdarkly_server_sdk::{FlagDetail, FlagValue, Reason, User};
use serde::{self, Deserialize, Serialize};

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
pub enum CommandResponse {
    EvaluateFlag(EvaluateFlagResponse),
    EvaluateAll(EvaluateAllFlagsResponse),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CommandParams {
    pub command: String,
    pub evaluate: Option<EvaluateFlagParams>,
    pub evaluate_all: Option<EvaluateAllFlagsParams>,
    pub custom_event: Option<CustomEventParams>,
    pub identify_event: Option<IdentifyEventParams>,
    pub alias_event: Option<AliasEventParams>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateFlagParams {
    pub flag_key: String,
    pub user: User,
    pub value_type: String,
    pub default_value: FlagValue,
    pub detail: bool,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateFlagResponse {
    pub value: Option<FlagValue>,
    pub variation_index: Option<usize>,
    pub reason: Option<Reason>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateAllFlagsParams {
    pub user: User,
    pub with_reasons: bool,
    pub client_side_only: bool,
    pub details_only_for_tracked_flags: bool,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateAllFlagsResponse {
    pub state: FlagDetail,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CustomEventParams {
    pub event_key: String,
    pub user: User,
    pub data: Option<FlagValue>,
    pub omit_null_data: bool,
    pub metric_value: Option<f64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct IdentifyEventParams {
    pub user: User,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AliasEventParams {
    pub user: User,
    pub previous_user: User,
}
