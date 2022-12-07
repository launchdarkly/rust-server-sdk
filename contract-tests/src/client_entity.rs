use eventsource_client::HttpsConnector;
use launchdarkly_server_sdk::{Context, ContextBuilder, MultiContextBuilder, Reference};
use std::time::Duration;

const DEFAULT_POLLING_BASE_URL: &str = "https://sdk.launchdarkly.com";
const DEFAULT_STREAM_BASE_URL: &str = "https://stream.launchdarkly.com";
const DEFAULT_EVENTS_BASE_URL: &str = "https://events.launchdarkly.com";

use launchdarkly_server_sdk::{
    ApplicationInfo, BuildError, Client, ConfigBuilder, Detail, EventProcessorBuilder,
    FlagDetailConfig, FlagValue, NullEventProcessorBuilder, PollingDataSourceBuilder,
    ServiceEndpointsBuilder, StreamingDataSourceBuilder,
};

use crate::command_params::{
    ContextBuildParams, ContextConvertParams, ContextParam, ContextResponse,
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
    pub async fn new(
        create_instance_params: CreateInstanceParams,
        connector: &HttpsConnector,
    ) -> Result<Self, BuildError> {
        let mut config_builder =
            ConfigBuilder::new(&create_instance_params.configuration.credential);

        let mut application_info = ApplicationInfo::new();
        if let Some(tags) = create_instance_params.configuration.tags {
            if let Some(id) = tags.application_id {
                application_info.application_identifier(id);
            }

            if let Some(version) = tags.application_version {
                application_info.application_version(version);
            }
        }

        config_builder = config_builder.application_info(application_info);

        let mut service_endpoints_builder = ServiceEndpointsBuilder::new();
        service_endpoints_builder.streaming_base_url(DEFAULT_STREAM_BASE_URL);
        service_endpoints_builder.polling_base_url(DEFAULT_POLLING_BASE_URL);
        service_endpoints_builder.events_base_url(DEFAULT_EVENTS_BASE_URL);

        if let Some(endpoints) = create_instance_params.configuration.service_endpoints {
            if let Some(streaming) = endpoints.streaming {
                service_endpoints_builder.streaming_base_url(&streaming);
            }
            if let Some(polling) = endpoints.polling {
                service_endpoints_builder.polling_base_url(&polling);
            }
            if let Some(events) = endpoints.events {
                service_endpoints_builder.events_base_url(&events);
            }
        }

        if let Some(streaming) = create_instance_params.configuration.streaming {
            if let Some(base_uri) = streaming.base_uri {
                service_endpoints_builder.streaming_base_url(&base_uri);
            }

            let mut streaming_builder = StreamingDataSourceBuilder::new();
            if let Some(delay) = streaming.initial_retry_delay_ms {
                streaming_builder.initial_reconnect_delay(Duration::from_millis(delay));
            }
            streaming_builder.https_connector(connector.clone());

            config_builder = config_builder.data_source(&streaming_builder);
        } else if let Some(polling) = create_instance_params.configuration.polling {
            if let Some(base_uri) = polling.base_uri {
                service_endpoints_builder.polling_base_url(&base_uri);
            }

            let mut polling_builder = PollingDataSourceBuilder::new();
            if let Some(delay) = polling.poll_interval_ms {
                polling_builder.poll_interval(Duration::from_millis(delay));
            }

            config_builder = config_builder.data_source(&polling_builder);
        }

        config_builder = if let Some(events) = create_instance_params.configuration.events {
            if let Some(base_uri) = events.base_uri {
                service_endpoints_builder.events_base_url(&base_uri);
            }

            let mut processor_builder = EventProcessorBuilder::new();
            if let Some(capacity) = events.capacity {
                processor_builder.capacity(capacity);
            }
            processor_builder.all_attributes_private(events.all_attributes_private);

            if let Some(interval) = events.flush_interval_ms {
                processor_builder.flush_interval(Duration::from_millis(interval));
            }

            if let Some(attributes) = events.global_private_attributes {
                processor_builder.private_attributes(attributes);
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
                        params.context,
                        params.event_key,
                        mv,
                        params
                            .data
                            .unwrap_or_else(|| serde_json::Value::Null.into()),
                    ),
                    None if params.data.is_some() => {
                        let _ = self.client.track_data(
                            params.context,
                            params.event_key,
                            params
                                .data
                                .unwrap_or_else(|| serde_json::Value::Null.into()),
                        );
                    }
                    None => self.client.track_event(params.context, params.event_key),
                };

                Ok(None)
            }
            "identifyEvent" => {
                self.client.identify(
                    command
                        .identify_event
                        .ok_or("Identify params should be set")?
                        .context,
                );
                Ok(None)
            }
            "flushEvents" => {
                self.client.flush();
                Ok(None)
            }
            "contextBuild" => {
                let params = command
                    .context_build
                    .ok_or("ContextBuild params should be set")?;
                Ok(Some(CommandResponse::ContextBuildOrConvert(
                    ContextResponse::from(Self::context_build(params)),
                )))
            }
            "contextConvert" => {
                let params = command
                    .context_convert
                    .ok_or("ContextConvert params should be set")?;
                Ok(Some(CommandResponse::ContextBuildOrConvert(
                    ContextResponse::from(Self::context_convert(params)),
                )))
            }
            command => Err(format!("Invalid command requested: {}", command)),
        }
    }

    fn context_build_single(single: ContextParam) -> Result<Context, String> {
        let mut builder = ContextBuilder::new(single.key);
        if let Some(kind) = single.kind {
            builder.kind(kind);
        }
        if let Some(name) = single.name {
            builder.name(name);
        }
        if let Some(anonymous) = single.anonymous {
            builder.anonymous(anonymous);
        }
        if let Some(attribute_references) = single.private {
            for attribute in attribute_references {
                builder.add_private_attribute(Reference::new(attribute));
            }
        }
        if let Some(attributes) = single.custom {
            for (k, v) in attributes {
                builder.set_value(k.as_str(), v);
            }
        }
        builder.build()
    }

    fn build_context_from_params(params: ContextBuildParams) -> Result<String, String> {
        if params.single.is_none() && params.multi.is_none() {
            return Err("either 'single' or 'multi' required for contextBuild command".to_string());
        }

        if let Some(single) = params.single {
            let context = Self::context_build_single(single)?;
            return serde_json::to_string(&context).map_err(|e| e.to_string());
        }

        if let Some(multi) = params.multi {
            let mut multi_builder = MultiContextBuilder::new();
            for single in multi {
                let c = Self::context_build_single(single)?;
                multi_builder.add_context(c);
            }
            let context = multi_builder.build()?;
            return serde_json::to_string(&context).map_err(|e| e.to_string());
        }

        unreachable!()
    }

    fn context_build(params: ContextBuildParams) -> Result<String, String> {
        Self::build_context_from_params(params)
    }

    fn context_convert(params: ContextConvertParams) -> Result<String, String> {
        serde_json::from_str::<Context>(&params.input)
            .map_err(|e| e.to_string())
            .and_then(|context| serde_json::to_string(&context).map_err(|e| e.to_string()))
    }

    fn evaluate(&self, params: EvaluateFlagParams) -> EvaluateFlagResponse {
        if params.detail {
            let detail: Detail<FlagValue> = match params.value_type.as_str() {
                "bool" => self
                    .client
                    .bool_variation_detail(
                        &params.context,
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
                        &params.context,
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
                        &params.context,
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
                        &params.context,
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
                        &params.context,
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
                    &params.context,
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
                    &params.context,
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
                    &params.context,
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
                    &params.context,
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
                    &params.context,
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

        let all_flags = self.client.all_flags_detail(&params.context, config);

        EvaluateAllFlagsResponse { state: all_flags }
    }
}

impl Drop for ClientEntity {
    fn drop(&mut self) {
        self.client.close();
    }
}
