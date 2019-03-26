use std::env;
use std::time::Duration;

use futures::future::lazy;
use futures::stream::Stream;
use tokio::timer::Interval;

fn main() {
    println!("Connecting...");

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let stream_url_opt = env::var("LAUNCHDARKLY_STREAM_URL");

    let mut client_builder = ldclient::client::Client::configure();
    let _ = stream_url_opt.map(|url| {
        client_builder.base_url(&url);
    });
    let mut client = client_builder.build(&sdk_key).unwrap();

    tokio::run(lazy(move || {
        client.start();

        Interval::new_interval(Duration::from_secs(5))
            .map_err(|_| ())
            .for_each(move |_| {
                let large_indirect_flag = client.bool_variation("large-indirect-flag");
                println!("large_indirect_flag flag: {}", large_indirect_flag);
                Ok(())
            })
    }));
}
