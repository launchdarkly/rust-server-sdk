use std::sync::{Arc, Mutex};

use super::store::FeatureStore;
use super::update_processor::StreamingUpdateProcessor;

const DEFAULT_BASE_URL: &'static str = "https://stream.launchdarkly.com/all";

pub type Error = String; // TODO enum

pub struct Client {
    //sdk_key: String,
    //config: Config,
    //eventProcessor: EventProcessor,
    update_processor: StreamingUpdateProcessor,
    store: Arc<Mutex<FeatureStore>>,
}

//pub struct Config {}

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
        let store = Arc::new(Mutex::new(FeatureStore::new()));
        Client {
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

    pub fn bool_variation(&self, /*TODO user, */ flag_name: &str) -> bool {
        self.evaluate(flag_name)
            .and_then(|val| val.as_bool().ok_or("flag is not boolean".to_string()))
            .unwrap_or_else(|e| {
                println!("couldn't evaluate flag {:?}: {}", flag_name, e);
                false
            })
    }

    pub fn evaluate(
        &self,
        /*TODO user, */ flag_name: &str,
    ) -> Result<serde_json::Value, Error> {
        let store = self.store.lock().unwrap();
        let flag = store.flag(flag_name).ok_or("no such flag")?;
        flag.get("on")
            .ok_or("flag missing 'on' property".to_string())
            .map(|v| v.clone())
    }
}
