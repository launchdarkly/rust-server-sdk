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
pub use client::{BuildError, FlushError, StartError};
pub use config::{Config, ConfigBuilder};
pub use data_source_builders::{BuildError as DataSourceBuildError, StreamingDataSourceBuilder};
pub use evaluation::FlagDetailConfig;
pub use event_processor::EventProcessor;
pub use event_processor_builders::{BuildError as EventProcessorBuildError, EventProcessorBuilder};
pub use service_endpoints::ServiceEndpointsBuilder;
pub use version::version_string;

mod client;
mod config;
mod data_source;
mod data_source_builders;
mod data_store;
mod data_store_builders;
mod evaluation;
mod event_processor;
mod event_processor_builders;
mod event_sink;
mod events;
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
