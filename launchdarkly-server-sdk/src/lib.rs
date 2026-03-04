//! launchdarkly-server-sdk is the main crate for the LaunchDarkly SDK.
//!
//! This package contains the types and methods for the SDK client [Client] and its overall
//! configuration.
//!
//! For more information and code examples, see the Rust SDK Reference:
//! <https://docs.launchdarkly.com/sdk/server-side/rust>

#![deny(rustdoc::missing_crate_level_docs)]
#![deny(missing_docs)]

#[macro_use]
extern crate log;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

use http::HeaderValue;
pub use launchdarkly_server_sdk_evaluation::Error as EvalError;
pub use launchdarkly_server_sdk_evaluation::{
    AttributeValue, Context, ContextBuilder, Detail, FlagValue, Kind, MultiContextBuilder, Reason,
    Reference,
};
use std::sync::LazyLock;

pub use client::Client;

// Re-export
pub use client::{BuildError, StartError};
pub use config::{ApplicationInfo, BuildError as ConfigBuildError, Config, ConfigBuilder};
pub use data_source_builders::{
    BuildError as DataSourceBuildError, PollingDataSourceBuilder, StreamingDataSourceBuilder,
};
pub use evaluation::{FlagDetail, FlagDetailConfig, FlagFilter};
pub use events::event::MigrationOpEvent;
pub use events::processor::EventProcessor;
pub use events::processor_builders::{
    BuildError as EventProcessorBuildError, EventProcessorBuilder, NullEventProcessorBuilder,
};
pub use feature_requester_builders::{
    BuildError as FeatureRequestBuilderError, FeatureRequesterFactory,
};
pub use launchdarkly_server_sdk_evaluation::{Flag, FlagBuilder, RuleBuilder, Segment, Versioned};
pub use migrations::{
    ExecutionOrder, MigrationOpTracker, Migrator, MigratorBuilder, Operation, Origin, Stage,
};
pub use service_endpoints::ServiceEndpointsBuilder;
pub use stores::persistent_store::{PersistentDataStore, PersistentStoreError};
pub use stores::persistent_store_builders::{
    PersistentDataStoreBuilder, PersistentDataStoreFactory,
};
pub use stores::store_types::{AllData, DataKind, SerializedItem, StorageItem};
pub use test_data::TestData;
pub use version::version_string;

mod client;
mod config;
mod data_source;
mod data_source_builders;
mod evaluation;
mod events;
mod feature_requester;
mod feature_requester_builders;
mod migrations;
mod reqwest;
mod sampler;
mod service_endpoints;
mod stores;
mod test_common;
mod test_data;
mod version;

static LAUNCHDARKLY_EVENT_SCHEMA_HEADER: &str = "x-launchdarkly-event-schema";
static LAUNCHDARKLY_PAYLOAD_ID_HEADER: &str = "x-launchdarkly-payload-id";
static LAUNCHDARKLY_TAGS_HEADER: &str = "x-launchdarkly-tags";
static CURRENT_EVENT_SCHEMA: &str = "4";

static USER_AGENT: LazyLock<String> =
    LazyLock::new(|| format!("RustServerClient/{}", version_string()));

static EMPTY_HEADER: LazyLock<HeaderValue> = LazyLock::new(|| HeaderValue::from_static(""));

#[cfg(test)]
mod tests {
    use test_case::test_case;

    #[test_case("130.65331632653061", 130.65331632653061)]
    #[test_case("130.65331632653062", 130.65331632653061)]
    #[test_case("130.65331632653063", 130.65331632653064)]
    fn json_float_serialization_matches_go(float_as_string: &str, expected: f64) {
        let parsed: f64 = serde_json::from_str(float_as_string).unwrap();
        assert_eq!(expected, parsed);
    }
}
