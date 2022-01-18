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
extern crate spectral;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

pub use launchdarkly_server_sdk_evaluation::Error as EvalError;
pub use launchdarkly_server_sdk_evaluation::{
    AttributeValue, Detail, FlagValue, Reason, TypeError, User, UserBuilder,
};
use lazy_static::lazy_static;

pub use client::Client;

// Re-export
pub use client::{BuildError, StartError};
pub use config::{Config, ConfigBuilder};
pub use data_source_builders::{
    BuildError as DataSourceBuildError, PollingDataSourceBuilder, StreamingDataSourceBuilder,
};
pub use evaluation::FlagDetailConfig;
pub use events::processor::EventProcessor;
pub use events::processor_builders::{
    BuildError as EventProcessorBuildError, EventProcessorBuilder,
};
pub use feature_requester_builders::{
    BuildError as FeatureRequestBuilderError, FeatureRequesterFactory,
};
pub use service_endpoints::ServiceEndpointsBuilder;
pub use version::version_string;

mod client;
mod config;
mod data_source;
mod data_source_builders;
mod data_store;
mod data_store_builders;
mod evaluation;
mod events;
mod feature_requester;
mod feature_requester_builders;
mod reqwest;
mod service_endpoints;
mod test_common;
mod version;

lazy_static! {
    pub(crate) static ref USER_AGENT: String =
        "RustServerClient/".to_owned() + built_info::PKG_VERSION;
}

#[allow(dead_code)]
mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

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
