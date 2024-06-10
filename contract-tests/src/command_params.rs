use launchdarkly_server_sdk::{
    AttributeValue, Context, FlagDetail, FlagValue, Operation, Reason, Stage,
};
use serde::{self, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
pub enum CommandResponse {
    EvaluateFlag(EvaluateFlagResponse),
    EvaluateAll(EvaluateAllFlagsResponse),
    ContextBuildOrConvert(ContextResponse),
    SecureModeHash(SecureModeHashResponse),
    MigrationVariation(MigrationVariationResponse),
    MigrationOperation(MigrationOperationResponse),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CommandParams {
    pub command: String,
    pub evaluate: Option<EvaluateFlagParams>,
    pub evaluate_all: Option<EvaluateAllFlagsParams>,
    pub custom_event: Option<CustomEventParams>,
    pub identify_event: Option<IdentifyEventParams>,
    pub context_build: Option<ContextBuildParams>,
    pub context_convert: Option<ContextConvertParams>,
    pub secure_mode_hash: Option<SecureModeHashParams>,
    pub migration_variation: Option<MigrationVariationParams>,
    pub migration_operation: Option<MigrationOperationParams>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateFlagParams {
    pub flag_key: String,
    pub context: Context,
    pub value_type: String,
    pub default_value: FlagValue,
    pub detail: bool,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateFlagResponse {
    pub value: Option<FlagValue>,
    pub variation_index: Option<isize>,
    pub reason: Option<Reason>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EvaluateAllFlagsParams {
    pub context: Context,
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
    pub context: Context,
    pub data: Option<FlagValue>,
    pub omit_null_data: bool,
    pub metric_value: Option<f64>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct IdentifyEventParams {
    pub context: Context,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ContextParam {
    pub kind: Option<String>,
    pub key: String,
    pub name: Option<String>,
    pub anonymous: Option<bool>,
    pub private: Option<Vec<String>>,
    pub custom: Option<HashMap<String, AttributeValue>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ContextBuildParams {
    pub single: Option<ContextParam>,
    pub multi: Option<Vec<ContextParam>>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContextResponse {
    pub output: Option<String>,
    pub error: Option<String>,
}

impl From<Result<String, String>> for ContextResponse {
    fn from(r: Result<String, String>) -> Self {
        r.map_or_else(
            |err| ContextResponse {
                output: None,
                error: Some(err),
            },
            |json| ContextResponse {
                output: Some(json),
                error: None,
            },
        )
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ContextConvertParams {
    pub input: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SecureModeHashParams {
    pub context: Context,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SecureModeHashResponse {
    pub result: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MigrationVariationParams {
    pub key: String,
    pub context: Context,
    pub default_stage: Stage,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MigrationVariationResponse {
    pub result: Stage,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MigrationOperationParams {
    pub key: String,
    pub context: Context,
    pub default_stage: Stage,
    pub read_execution_order: String,
    pub operation: Operation,
    pub old_endpoint: String,
    pub new_endpoint: String,
    pub payload: Option<String>,
    pub track_latency: bool,
    pub track_errors: bool,
    pub track_consistency: bool,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MigrationOperationResponse {
    pub result: String,
}
