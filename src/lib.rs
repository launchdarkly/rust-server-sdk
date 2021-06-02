#[macro_use]
extern crate log;

#[cfg(test)]
extern crate spectral;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

mod client;
mod event_processor;
mod event_sink;
mod events;
mod store;
mod test_common;
mod update_processor;

use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref USER_AGENT: String =
        "RustServerClient/".to_owned() + built_info::PKG_VERSION;
}

#[allow(dead_code)]
mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

// Re-export
pub use client::Error as ClientError;
pub use client::{Client, ClientBuilder};
pub use rust_server_sdk_evaluation::Error as EvalError;
pub use rust_server_sdk_evaluation::{
    AttributeValue, Detail, FlagValue, Reason, TypeError, User, UserBuilder,
};
