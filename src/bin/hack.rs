use std::env;

use futures::future::Future;
use futures::stream::Stream;

use ldclient::eventsource::Client as EV;

fn main() {
    println!("Connecting...");

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");

    let client = EV::for_url("https://stream-stg.launchdarkly.com/all")
        .header("Authorization", &sdk_key)
        .build();

    let event_stream = client.stream();

    tokio::run(
        event_stream
            .for_each(|event| {
                println!("got a event: {:?}", event);
                Ok(())
            })
            .map_err(|e| println!("error! {:?}", e)),
    );
}
