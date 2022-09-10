#[macro_use]
extern crate log;
#[macro_use]
extern crate maplit;

use std::env;

use eventsource_client::HttpsConnector;
use launchdarkly_server_sdk::{
    Client, ConfigBuilder, ServiceEndpointsBuilder, StreamingDataSourceBuilder, User,
};

use env_logger::Env;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(Env::new().default_filter_or("info"));

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let mut data_source = StreamingDataSourceBuilder::new();
    data_source.https_connector(HttpsConnector::with_native_roots());
    let config = ConfigBuilder::new(&sdk_key)
        .data_source(&data_source)
        .build();
    let client = Client::build(config).expect("failed to start client");
}
