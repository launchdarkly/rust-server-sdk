use std::env;
use std::time::Duration;

use futures::future::lazy;
use futures::stream::Stream;
use tokio::timer::Interval;

fn main() {
    println!("Connecting...");

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");

    let mut client = ldclient::client::Client::configure()
        .base_url("https://stream-stg.launchdarkly.com")
        .build(&sdk_key);

    tokio::run(lazy(move || {
        client.start();

        Interval::new_interval(Duration::from_secs(5))
            .map_err(|_| ())
            .for_each(move |_| {
                let awesomeness = client.bool_variation("awesomeness");
                println!("awesomeness flag: {}", awesomeness);
                Ok(())
            })
    }));
}
