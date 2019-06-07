#[macro_use]
extern crate log;
#[macro_use]
extern crate maplit;
extern crate simplelog;

use std::env;
use std::process::exit;
use std::time::Duration;

use ldclient::client::Client;
use ldclient::users::User;

use futures::future::lazy;
use futures::stream::Stream;
use simplelog::{Config, LevelFilter, TermLogger};
use tokio::timer::Interval;

fn main() {
    TermLogger::init(LevelFilter::Debug, Config::default()).unwrap();

    info!("Connecting...");

    let flags: Vec<String> = env::args().skip(1).collect();
    if flags.is_empty() {
        error!("Please list some flags to watch.");
        exit(1);
    }

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let stream_url_opt = env::var("LAUNCHDARKLY_STREAM_URL");

    let alice = User::new_with_custom("alice", hashmap! { "team".into() => "Avengers".into() });
    let bob = User::new("bob");

    let mut client_builder = Client::configure();
    let _ = stream_url_opt.map(|url| {
        client_builder.base_url(&url);
    });
    let mut client = client_builder.build(&sdk_key).unwrap();

    tokio::run(lazy(move || {
        client.start();

        Interval::new_interval(Duration::from_secs(5))
            .map_err(|_| ())
            .for_each(move |_| {
                for user in vec![&alice, &bob] {
                    for flag_key in &flags {
                        let flag_detail = client.bool_variation_detail(user, &flag_key, false);
                        info!("user {}, flag {}: {:?}", user.key, flag_key, flag_detail);
                    }
                }
                Ok(())
            })
    }));
}
