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
use futures::future::lazy;
use simplelog::{Config, LevelFilter, TermLogger};

fn main() {
    TermLogger::init(LevelFilter::Info, Config::default()).unwrap();

    let sdk_key = env::var("LAUNCHDARKLY_SDK_KEY").expect("Please set LAUNCHDARKLY_SDK_KEY");

    let flags: Vec<String> = env::args().skip(1).collect();
    if flags.len() != 1 {
        error!("Please enter your username on the command line.");
        exit(1);
    }
    let user = User::new(&flags[0]);

    // TODO move this wrapper into Client type
    let ld = Arc::new(RwLock::new(Client::new(&sdk_key)));
    let ld_w = ld.clone();

    let mut rt = tokio::runtime::Runtime::new().expect("failed to get runtime");
    let executor = rt.executor();

    // TODO move this into a Client method
    thread::spawn(|| {
        rt.spawn(lazy(move || {
            {
                ld_w.write().unwrap().start();
            }

            futures::future::ok(())
        }));

        rt.shutdown_on_idle()
    });

    let mut cursive = Cursive::default();

    cursive.add_global_callback('q', Cursive::quit);

    let cb = cursive.cb_sink().clone();

    let bar = ProgressBar::new()
        .with_task(move |counter| {
            fake_load(executor, ld, user, counter);

            cb.send(Box::new(display_done)).unwrap();
        })
        .full_width();

    cursive.add_layer(bar);

    cursive.set_autorefresh(true);
    cursive.run();
}

fn fake_load(
    mut executor: impl tokio_executor::Executor,
    ld: Arc<RwLock<Client>>,
    user: User,
    counter: Counter,
) {
    let mut entered = tokio_executor::enter().expect("enter");

    tokio_executor::with_default(&mut executor, &mut entered, |_| {
        let ld = ld.read().unwrap();

        while counter.get() < 80 {
            thread::sleep(Duration::from_millis(20));
            counter.tick(1);
        }

        while counter.get() < 100 {
            thread::sleep(Duration::from_millis(100));

            let increase = ld.bool_variation_detail(&user, "test-demo", false);

            if increase.value.unwrap() {
                counter.tick(1);
            }
        }
    });
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
