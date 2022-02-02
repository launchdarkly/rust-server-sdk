use std::time::Duration;

const DEFAULT_POLLING_BASE_URL: &str = "https://sdk.launchdarkly.com";
const DEFAULT_STREAM_BASE_URL: &str = "https://stream.launchdarkly.com";
const DEFAULT_EVENTS_BASE_URL: &str = "https://events.launchdarkly.com";

use launchdarkly_server_sdk::{
    BuildError, Client, ConfigBuilder, Detail, EventProcessorBuilder, FlagDetailConfig, FlagValue,
    NullEventProcessorBuilder, ServiceEndpointsBuilder, StreamingDataSourceBuilder,
};

use crate::{
    command_params::{
        CommandParams, CommandResponse, EvaluateAllFlagsParams, EvaluateAllFlagsResponse,
        EvaluateFlagParams, EvaluateFlagResponse,
    },
    CreateInstanceParams,
};

pub struct ClientEntity {
    client: Client,
}

impl ClientEntity {
    pub async fn new(create_instance_params: CreateInstanceParams) -> Result<Self, BuildError> {
        let mut config_builder =
            ConfigBuilder::new(&create_instance_params.configuration.credential);

        let mut service_endpoints_builder = ServiceEndpointsBuilder::new();
        service_endpoints_builder.streaming_base_url(DEFAULT_STREAM_BASE_URL);
        service_endpoints_builder.polling_base_url(DEFAULT_POLLING_BASE_URL);
        service_endpoints_builder.events_base_url(DEFAULT_EVENTS_BASE_URL);

        if let Some(streaming) = create_instance_params.configuration.streaming {
            if let Some(base_uri) = streaming.base_uri {
                service_endpoints_builder.streaming_base_url(&base_uri);
            }

            let mut streaming_builder = StreamingDataSourceBuilder::new();
            if let Some(delay) = streaming.initial_retry_delay_ms {
                streaming_builder.initial_reconnect_delay(Duration::from_millis(delay));
            }

            config_builder = config_builder.data_source(&streaming_builder);
        }

        config_builder = if let Some(events) = create_instance_params.configuration.events {
            service_endpoints_builder.events_base_url(&events.base_uri);

            let mut processor_builder = EventProcessorBuilder::new();
            if let Some(capacity) = events.capacity {
                processor_builder.capacity(capacity);
            }
            processor_builder.inline_users_in_events(events.inline_users);

            if let Some(interval) = events.flush_interval_ms {
                processor_builder.flush_interval(Duration::from_millis(interval));
            }

            config_builder.event_processor(&processor_builder)
        } else {
            config_builder.event_processor(&NullEventProcessorBuilder::new())
        };

        config_builder = config_builder.service_endpoints(&service_endpoints_builder);

        let config = config_builder.build();
        let client = Client::build(config)?;
        client.start_with_default_executor();
        client.initialized_async().await;

        Ok(Self { client })
    }

    pub fn do_command(&self, command: CommandParams) -> Result<Option<CommandResponse>, String> {
        match command.command.as_str() {
            "evaluate" => Ok(Some(CommandResponse::EvaluateFlag(
                self.evaluate(command.evaluate.ok_or("Evaluate params should be set")?),
            ))),
            "evaluateAll" => Ok(Some(CommandResponse::EvaluateAll(
                self.evaluate_all(
                    command
                        .evaluate_all
                        .ok_or("Evaluate all params should be set")?,
                ),
            ))),
            "customEvent" => {
                let params = command.custom_event.ok_or("Custom params should be set")?;

                match params.metric_value {
                    Some(mv) => self.client.track_metric(
                        params.user,
                        params.event_key,
                        mv,
                        params
                            .data
                            .unwrap_or_else(|| serde_json::Value::Null.into()),
                    ),
                    None if params.data.is_some() => {
                        let _ = self.client.track_data(
                            params.user,
                            params.event_key,
                            params
                                .data
                                .unwrap_or_else(|| serde_json::Value::Null.into()),
                        );
                    }
                    None => self.client.track_event(params.user, params.event_key),
                };

                Ok(None)
            }
            "identifyEvent" => {
                self.client.identify(
                    command
                        .identify_event
                        .ok_or("Identify params should be set")?
                        .user,
                );
                Ok(None)
            }
            "aliasEvent" => {
                let params = command.alias_event.ok_or("Alias params should be set")?;
                self.client.alias(params.user, params.previous_user);
                Ok(None)
            }
            "flushEvents" => {
                self.client.flush();
                Ok(None)
            }
            command => return Err(format!("Invalid command requested: {}", command)),
        }
    }

    fn evaluate(&self, params: EvaluateFlagParams) -> EvaluateFlagResponse {
        if params.detail {
            let detail: Detail<FlagValue> = match params.value_type.as_str() {
                "bool" => self
                    .client
                    .bool_variation_detail(
                        &params.user,
                        &params.flag_key,
                        params
                            .default_value
                            .as_bool()
                            .expect("Should not fail to convert"),
                    )
                    .map(|v| v.into()),
                "int" => self
                    .client
                    .int_variation_detail(
                        &params.user,
                        &params.flag_key,
                        params
                            .default_value
                            .as_int()
                            .expect("Should not fail to convert"),
                    )
                    .map(|v| v.into()),
                "double" => self
                    .client
                    .float_variation_detail(
                        &params.user,
                        &params.flag_key,
                        params
                            .default_value
                            .as_float()
                            .expect("Should not fail to convert"),
                    )
                    .map(|v| v.into()),
                "string" => self
                    .client
                    .str_variation_detail(
                        &params.user,
                        &params.flag_key,
                        params
                            .default_value
                            .as_string()
                            .expect("Should not fail to convert"),
                    )
                    .map(|v| v.into()),
                _ => self
                    .client
                    .json_variation_detail(
                        &params.user,
                        &params.flag_key,
                        params
                            .default_value
                            .as_json()
                            .expect("Any type should be valid JSON"),
                    )
                    .map(|v| v.into()),
            };

            return EvaluateFlagResponse {
                value: detail.value,
                variation_index: detail.variation_index,
                reason: Some(detail.reason),
            };
        }

        let result: FlagValue = match params.value_type.as_str() {
            "bool" => self
                .client
                .bool_variation(
                    &params.user,
                    &params.flag_key,
                    params
                        .default_value
                        .as_bool()
                        .expect("Should not fail to convert"),
                )
                .into(),
            "int" => self
                .client
                .int_variation(
                    &params.user,
                    &params.flag_key,
                    params
                        .default_value
                        .as_int()
                        .expect("Should not fail to convert"),
                )
                .into(),
            "double" => self
                .client
                .float_variation(
                    &params.user,
                    &params.flag_key,
                    params
                        .default_value
                        .as_float()
                        .expect("Should not fail to convert"),
                )
                .into(),
            "string" => self
                .client
                .str_variation(
                    &params.user,
                    &params.flag_key,
                    params
                        .default_value
                        .as_string()
                        .expect("Should not fail to convert"),
                )
                .into(),
            _ => self
                .client
                .json_variation(
                    &params.user,
                    &params.flag_key,
                    params
                        .default_value
                        .as_json()
                        .expect("Any type should be valid JSON"),
                )
                .into(),
        };

        EvaluateFlagResponse {
            value: Some(result),
            variation_index: None,
            reason: None,
        }
    }

    fn evaluate_all(&self, params: EvaluateAllFlagsParams) -> EvaluateAllFlagsResponse {
        let mut config = FlagDetailConfig::new();

        if params.with_reasons {
            config.with_reasons();
        }

        if params.client_side_only {
            config.client_side_only();
        }

        if params.details_only_for_tracked_flags {
            config.details_only_for_tracked_flags();
        }

        let all_flags = self.client.all_flags_detail(&params.user, config);

        EvaluateAllFlagsResponse { state: all_flags }
    }
}

impl Drop for ClientEntity {
    fn drop(&mut self) {
        self.client.close();
    }
}
