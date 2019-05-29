use std::sync::{Arc, Mutex};

use super::store::{self, FeatureStore};
use super::update_processor::{self as up, StreamingUpdateProcessor};

const DEFAULT_BASE_URL: &'static str = "https://stream.launchdarkly.com";

#[derive(Debug)]
pub enum Error {
    FlagWrongType(String, String),
    InvalidConfig(up::Error),
    EvaluationError(store::Error),
    NoSuchFlag(String),
}

pub type Result<T> = std::result::Result<T, Error>;

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
            let untrimmed_url = url;
            url = &url[..url.len() - 1];
            debug!("trimming base url: {} -> {}", untrimmed_url, url);
        }

        self.base_url = url.to_owned();
        self
    }

    pub fn build(&self, sdk_key: &str) -> Result<Client> {
        let store = Arc::new(Mutex::new(FeatureStore::new()));
        let update_processor = StreamingUpdateProcessor::new(&self.base_url, sdk_key, &store)
            .map_err(|e| Error::InvalidConfig(e))?;
        Ok(Client {
            update_processor: update_processor,
            store: store,
        })
    }
}

impl Client {
    pub fn new(sdk_key: &str) -> Client {
        Client::configure().build(sdk_key).unwrap()
    }

    pub fn configure() -> ConfigBuilder {
        ConfigBuilder {
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }

    pub fn start(&mut self) {
        self.update_processor.subscribe()
    }

    pub fn bool_variation(&self, /*TODO user, */ flag_name: &str, default: bool) -> bool {
        self.evaluate(flag_name)
            .and_then(|val| {
                val.as_bool().ok_or(Error::FlagWrongType(
                    flag_name.to_string(),
                    "not bool".to_string(),
                ))
            })
            .unwrap_or_else(|e| {
                warn!("couldn't evaluate flag {:?}: {:?}", flag_name, e);
                default
            })
    }

    /*
     * TODO don't expose JSON types
     */
    pub fn evaluate(&self, /*TODO user, */ flag_name: &str) -> Result<serde_json::Value> {
        let store = self.store.lock().unwrap();
        let flag = store
            .flag(flag_name)
            .ok_or(Error::NoSuchFlag(flag_name.to_string()))?;
        flag.evaluate()
            .map_err(|e| Error::EvaluationError(e))
            .map(|v| v.clone())
    }
}
