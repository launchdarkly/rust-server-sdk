#[macro_use]
extern crate log;

#[cfg(test)]
extern crate spectral;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

pub mod client;
mod event_processor;
mod event_sink;
pub mod events;
pub mod store; // TODO no, move flagvalue into types or something instead
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
pub use rust_server_sdk_evaluation::User;
