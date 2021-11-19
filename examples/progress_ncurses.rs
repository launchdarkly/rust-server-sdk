#[macro_use]
extern crate log;

use std::env;
use std::process::exit;
use std::thread;
use std::time::Duration;

use launchdarkly_server_sdk::{Client, ConfigBuilder, ServiceEndpointsBuilder, User};

use cursive::traits::Boxable;
use cursive::utils::Counter;
use cursive::views::{Dialog, ProgressBar};
use cursive::Cursive;

fn main() {
    env_logger::init();

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let stream_url_opt = env::var("LAUNCHDARKLY_STREAM_URL");
    let events_url_opt = env::var("LAUNCHDARKLY_EVENTS_URL");
    let polling_url_opt = env::var("LAUNCHDARKLY_POLLING_URL");

    let flags: Vec<String> = env::args().skip(1).collect();
    if flags.len() != 1 {
        error!("Please enter your username on the command line.");
        exit(1);
    }
    let user = User::with_key(flags[0].clone()).build();

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

    let mut cursive = Cursive::default();

    cursive.add_global_callback('q', Cursive::quit);

    let cb = cursive.cb_sink().clone();

    let progress = ProgressBar::new()
        .with_task(move |counter| {
            fake_load(&client, user, counter);

            cb.send(Box::new(display_done)).unwrap();
        })
        .full_width();

    cursive.add_layer(progress);

    cursive.set_autorefresh(true);
    cursive.run();
}

fn fake_load(client: &Client, mut user: User, counter: Counter) {
    while counter.get() < 20 {
        thread::sleep(Duration::from_millis(20));
        counter.tick(1);
    }

    while counter.get() < 100 {
        user.attribute("progress", counter.get() as i64).unwrap();

        let millis = client.int_variation(&user, "progress-delay", 100);
        thread::sleep(Duration::from_millis(millis as u64));

        let increase = client.bool_variation(&user, "make-progress", false);

        if increase {
            counter.tick(1);
        }
    }
}

fn display_done(cursive: &mut Cursive) {
    cursive.set_autorefresh(false);
    cursive.pop_layer();
    cursive.add_layer(
        Dialog::new()
            .title("It's alive!!!!!!!")
            .button("Mwahahahaa", |s| s.quit())
            .full_width(),
    );
}
