#[macro_use]
extern crate log;

use std::{
    env,
    io::{self, Write},
    process::exit,
    thread,
    time::Duration,
};

use launchdarkly_server_sdk::{Client, ConfigBuilder, ContextBuilder, ServiceEndpointsBuilder};

const MAX_PROGRESS: usize = 100;

// example "app" that just counts and prints progress to stdout
struct ProgressCounter {
    count: usize,
}

impl ProgressCounter {
    fn start(&self) {
        println!("{}", "=".repeat(MAX_PROGRESS));
    }
    fn inc(&mut self) {
        self.count += 1;
        print!(".");
        io::stdout().flush().unwrap();
    }
    fn finish(&self) {
        println!();
        self.start();
    }
}

fn main() {
    env_logger::init();

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let stream_url_opt = env::var("LAUNCHDARKLY_STREAM_URL");
    let events_url_opt = env::var("LAUNCHDARKLY_EVENTS_URL");
    let polling_url_opt = env::var("LAUNCHDARKLY_POLLING_URL");

    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 1 {
        eprintln!("Please enter a username on the command line.");
        exit(1);
    }
    let context = ContextBuilder::new(args[0].clone())
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

    let client = Client::build(config_builder.build()).expect("failed to build client");
    client.start_with_runtime().expect("failed to start");

    let mut counter = ProgressCounter { count: 0 };

    counter.start();
    while counter.count < 20 {
        thread::sleep(Duration::from_millis(20));
        counter.inc();
    }
    while counter.count < 100 {
        let millis = client.int_variation(&context, "progress-delay", 100);
        thread::sleep(Duration::from_millis(millis as u64));

        let increase = client.bool_variation(&context, "make-progress", false);
        if increase {
            counter.inc();
        }
    }

    client.close();
    counter.finish();
}
