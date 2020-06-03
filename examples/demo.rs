#[macro_use]
extern crate log;

use std::env;
use std::process::exit;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use ldclient::client::Client;
use ldclient::users::User;

use cursive::traits::Boxable;
use cursive::utils::Counter;
use cursive::views::{Dialog, ProgressBar};
use cursive::Cursive;

fn main() {
    env_logger::init();

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");
    let stream_url_opt = env::var("LAUNCHDARKLY_STREAM_URL");
    let events_url_opt = env::var("LAUNCHDARKLY_EVENTS_URL");

    let flags: Vec<String> = env::args().skip(1).collect();
    if flags.len() != 1 {
        error!("Please enter your username on the command line.");
        exit(1);
    }
    let user = User::with_key(flags[0].clone()).build();

    let mut client_builder = Client::configure();
    if let Ok(url) = stream_url_opt {
        client_builder.stream_base_url(&url);
    }
    if let Ok(url) = events_url_opt {
        client_builder.events_base_url(&url);
    }
    let client = client_builder
        .start(&sdk_key)
        .expect("failed to start client");

    let mut cursive = Cursive::default();

    cursive.add_global_callback('q', Cursive::quit);

    let cb = cursive.cb_sink().clone();

    let progress = ProgressBar::new()
        .with_task(move |counter| {
            fake_load(client, user, counter);

            cb.send(Box::new(display_done)).unwrap();
        })
        .full_width();

    cursive.add_layer(progress);

    cursive.set_autorefresh(true);
    cursive.run();
}

fn fake_load(ld: Arc<RwLock<Client>>, mut user: User, counter: Counter) {
    let ld = ld.read().unwrap();

    while counter.get() < 20 {
        thread::sleep(Duration::from_millis(20));
        counter.tick(1);
    }

    while counter.get() < 100 {
        user.attribute("progress", counter.get() as i64);

        let millis = ld.int_variation_detail(&user, "progress-delay", 100);
        thread::sleep(Duration::from_millis(millis.value.unwrap() as u64));

        let increase = ld.bool_variation_detail(&user, "make-progress", false);

        if increase.value.unwrap() {
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
