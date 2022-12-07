#[macro_use]
extern crate log;

use std::env;
use std::process::exit;
use std::time::Duration;

use launchdarkly_server_sdk::{Client, ConfigBuilder, ContextBuilder, ServiceEndpointsBuilder};

use env_logger::Env;
use tokio::time;

#[tokio::main]
async fn main() {
    env_logger::init_from_env(Env::new().default_filter_or("info"));

    info!("Connecting...");

    let flags: Vec<String> = env::args().skip(1).collect();
    let mut bool_flags = Vec::<String>::new();
    let mut str_flags = Vec::<String>::new();
    for flag in flags {
        let bits: Vec<&str> = flag.splitn(2, ':').collect();
        let bits = bits.as_slice();
        if let ["bool", name] = bits {
            bool_flags.push(name.to_string());
        } else if let ["str", name] = bits {
            str_flags.push(name.to_string());
        } else if let [flag_type, _] = bits {
            error!("Unsupported flag type {} in {}", flag_type, flag);
            exit(2);
        } else if let [name] = bits {
            bool_flags.push(name.to_string());
        } else {
            unreachable!();
        }
    }
    if bool_flags.is_empty() && str_flags.is_empty() {
        error!("Please list some flags to watch.");
        exit(1);
    }

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let stream_url_opt = env::var("LAUNCHDARKLY_STREAM_URL");
    let events_url_opt = env::var("LAUNCHDARKLY_EVENTS_URL");
    let polling_url_opt = env::var("LAUNCHDARKLY_POLLING_URL");

    let alice = ContextBuilder::new("alice")
        .set_value("team", "Avengers".into())
        .build()
        .expect("Failed to create context");
    let bob = ContextBuilder::new("bob")
        .build()
        .expect("Failed to create context");

    let mut config_builder = ConfigBuilder::new(&sdk_key);
    match (stream_url_opt, events_url_opt, polling_url_opt) {
        (Ok(stream_url_opt), Ok(events_url_opt), Ok(polling_url_opt)) => {
            config_builder = config_builder.service_endpoints(
                ServiceEndpointsBuilder::new()
                    .polling_base_url(&polling_url_opt)
                    .events_base_url(&events_url_opt)
                    .streaming_base_url(&stream_url_opt),
            );
        }
        // If none of them are set, then that is fine and we default.
        (Err(_), Err(_), Err(_)) => {}
        _ => {
            error!(
                "Please specify all URLs LAUNCHDARKLY_STREAM_URL,\
             LAUNCHDARKLY_EVENTS_URL, and LAUNCHDARKLY_POLLING_URL"
            );
        }
    }

    let config = config_builder.build();
    let client = Client::build(config).expect("failed to start client");
    client.start_with_default_executor();

    let mut interval = time::interval(Duration::from_secs(5));

    let initialized = client.initialized_async().await;

    if !initialized {
        error!("The client failed to initialize!");
    }

    loop {
        interval.tick().await;

        for context in &[&alice, &bob] {
            for flag_key in &bool_flags {
                let flag_detail = client.bool_variation_detail(context, flag_key, false);
                info!(
                    "context {:?}, flag {}: {:?}",
                    context.key(),
                    flag_key,
                    flag_detail
                );
            }
            for flag_key in &str_flags {
                let flag_detail =
                    client.str_variation_detail(context, flag_key, "default".to_string());
                info!(
                    "context {:?}, flag {}: {:?}",
                    context.key(),
                    flag_key,
                    flag_detail
                );
            }
        }
    }
}
