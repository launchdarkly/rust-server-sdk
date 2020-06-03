#[macro_use]
extern crate log;
#[macro_use]
extern crate maplit;

use std::env;
use std::process::exit;
use std::time::Duration;

use ldclient::client::Client;
use ldclient::users::User;

use env_logger::Env;
use futures::future::lazy;
use futures::stream::Stream;
use tokio::timer::Interval;

fn main() {
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
            assert!(false, "impossible");
        }
    }
    if bool_flags.is_empty() && str_flags.is_empty() {
        error!("Please list some flags to watch.");
        exit(1);
    }

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let stream_url_opt = env::var("LAUNCHDARKLY_STREAM_URL");
    let events_url_opt = env::var("LAUNCHDARKLY_EVENTS_URL");

    let alice = User::with_key("alice")
        .custom(hashmap! { "team".into() => "Avengers".into() })
        .build();
    let bob = User::with_key("bob").build();

    let mut client_builder = Client::configure();
    if let Ok(url) = stream_url_opt {
        client_builder.stream_base_url(&url);
    }
    if let Ok(url) = events_url_opt {
        client_builder.events_base_url(&url);
    }

    tokio::run(lazy(move || {
        let client = client_builder
            .start_with_default_executor(&sdk_key)
            .expect("failed to start client");

        Interval::new_interval(Duration::from_secs(5))
            .map_err(|_| ())
            .for_each(move |_| {
                for user in vec![&alice, &bob] {
                    for flag_key in &bool_flags {
                        let flag_detail = client.bool_variation_detail(user, flag_key, false);
                        info!(
                            "user {:?}, flag {}: {:?}",
                            user.key(),
                            flag_key,
                            flag_detail
                        );
                    }
                    for flag_key in &str_flags {
                        let flag_detail = client.str_variation_detail(user, flag_key, "default");
                        info!(
                            "user {:?}, flag {}: {:?}",
                            user.key(),
                            flag_key,
                            flag_detail
                        );
                    }
                }
                Ok(())
            })
    }));
}
