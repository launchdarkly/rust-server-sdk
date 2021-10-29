use std::{
    env,
    io::{self, Write},
    process::exit,
    thread,
    time::Duration,
};

use launchdarkly_server_sdk as ld;

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

    let args: Vec<String> = env::args().skip(1).collect();
    if args.len() != 1 {
        eprintln!("Please enter a username on the command line.");
        exit(1);
    }
    let mut user = ld::User::with_key(args[0].clone()).build();

    let mut client_builder = ld::Client::configure();
    if let Ok(url) = stream_url_opt {
        client_builder.stream_base_url(&url);
    }
    if let Ok(url) = events_url_opt {
        client_builder.events_base_url(&url);
    }
    let client = client_builder
        .start(&sdk_key)
        .expect("failed to start client");

    let ld = client.read().unwrap();

    let mut counter = ProgressCounter { count: 0 };

    counter.start();
    while counter.count < 20 {
        thread::sleep(Duration::from_millis(20));
        counter.inc();
    }
    while counter.count < 100 {
        user.attribute("progress", counter.count as f64).unwrap();

        let millis = ld.int_variation(&user, "progress-delay", 100);
        thread::sleep(Duration::from_millis(millis as u64));

        let increase = ld.bool_variation(&user, "make-progress", false);
        if increase {
            counter.inc();
        }
    }
    counter.finish();
}
