use std::sync::{Arc, Mutex};

use super::store::FeatureStore;
use super::update_processor::StreamingUpdateProcessor;

const DEFAULT_BASE_URL: &'static str = "https://stream.launchdarkly.com/all";

pub type Error = String; // TODO enum

pub struct Client {
    sdk_key: String,
    config: Config,
    //eventProcessor: EventProcessor,
    update_processor: StreamingUpdateProcessor,
    store: Arc<Mutex<FeatureStore>>,
}

pub struct Config {}

pub struct ConfigBuilder {
    base_url: String,
}

impl ConfigBuilder {
    pub fn base_url<'a>(&'a mut self, url: &str) -> &'a mut ConfigBuilder {
        let mut url = url;
        while url.ends_with("/") {
            print!("trimming base url: {}", url);
            url = &url[..url.len() - 1];
            println!(" -> {}", url);
        }

        self.base_url = url.to_owned();
        self
    }

    pub fn build(&self, sdk_key: &str) -> Client {
        let config = Config {};
        let store = Arc::new(Mutex::new(FeatureStore::new()));
        Client {
            sdk_key: sdk_key.to_owned(),
            config,
            update_processor: StreamingUpdateProcessor::new(&self.base_url, sdk_key, &store),
            store: store,
        }
    }
}

impl Client {
    pub fn new(sdk_key: &str) -> Client {
        Client::configure().build(sdk_key)
    }

    pub fn configure() -> ConfigBuilder {
        ConfigBuilder {
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }

    pub fn start(&mut self) {
        self.update_processor.subscribe()
    }

    pub fn get_all_the_data_all_of_it(&self) -> Option<serde_json::Value> {
        let store = self.store.lock().unwrap();
        store.data.clone()
    }
}
