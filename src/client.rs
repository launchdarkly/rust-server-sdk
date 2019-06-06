use std::sync::{Arc, Mutex};

use super::eval::{self, Detail};
use super::store::{FeatureStore, FlagValue};
use super::update_processor::{self as up, StreamingUpdateProcessor};
use super::users::User;

const DEFAULT_BASE_URL: &'static str = "https://stream.launchdarkly.com";

#[derive(Debug)]
pub enum Error {
    FlagWrongType(String, String),
    InvalidConfig(up::Error),
    EvaluationError(eval::Error),
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

    pub fn bool_variation_detail(
        &self,
        user: &User,
        flag_name: &str,
        default: bool,
    ) -> Detail<bool> {
        self.evaluate_detail(user, flag_name)
            .try_map(|val| val.as_bool(), eval::Error::Exception)
            .or(default)
    }

    pub fn evaluate_detail(&self, user: &User, flag_name: &str) -> Detail<FlagValue> {
        let store = self.store.lock().unwrap();
        let flag = store.flag(flag_name);
        if flag.is_none() {
            return Detail::err(eval::Error::FlagNotFound);
        }
        // TODO can we avoid the clone here?
        flag.unwrap().evaluate(user).map(|v| v.clone())
    }
}
