use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use super::eval::{self, Detail};
use super::event_processor::EventProcessor;
use super::event_sink::{self, EventSink};
use super::events::{BaseEvent, Event, MaybeInlinedUser};
use super::store::{FeatureStore, FlagValue};
use super::update_processor::StreamingUpdateProcessor;
use super::users::User;

const DEFAULT_STREAM_BASE_URL: &str = "https://stream.launchdarkly.com";
const DEFAULT_EVENTS_BASE_URL: &str = "https://events.launchdarkly.com";

#[derive(Debug)]
pub enum Error {
    FlagWrongType(String, String),
    InvalidConfig(Box<dyn std::fmt::Debug>),
    EvaluationError(eval::Error),
    NoSuchFlag(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Client {
    //sdk_key: String,
    config: Config,
    event_processor: EventProcessor,
    update_processor: StreamingUpdateProcessor,
    store: Arc<Mutex<FeatureStore>>,
}

#[derive(Clone, Copy)]
pub struct Config {
    inline_users_in_events: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            inline_users_in_events: false,
        }
    }
}

pub struct ClientBuilder {
    stream_base_url: String,
    events_base_url: String,
    config: Config,
    event_sink: Option<Arc<RwLock<dyn EventSink>>>,
}

impl ClientBuilder {
    pub fn stream_base_url<'a>(&'a mut self, url: &str) -> &'a mut ClientBuilder {
        let url = trim_base_url(url);
        self.stream_base_url = url.to_owned();
        self
    }

    pub fn events_base_url<'a>(&'a mut self, url: &str) -> &'a mut ClientBuilder {
        let url = trim_base_url(url);
        self.events_base_url = url.to_owned();
        self
    }

    pub fn inline_users_in_events(&mut self, inline: bool) -> &mut ClientBuilder {
        self.config.inline_users_in_events = inline;
        self
    }

    #[cfg(test)]
    fn event_sink(&mut self, sink: Arc<RwLock<dyn EventSink>>) -> &mut ClientBuilder {
        self.event_sink = Some(sink);
        self
    }

    pub fn build(&self, sdk_key: &str) -> Result<Client> {
        let store = Arc::new(Mutex::new(FeatureStore::new()));
        let event_processor = match &self.event_sink {
            None => reqwest::Url::parse(&self.events_base_url)
                .map_err(|e| Error::InvalidConfig(Box::new(e)))
                .map(|base_url| EventProcessor::new(base_url, sdk_key))?,
            Some(sink) => {
                // clone sink Arc so we don't have to consume builder
                EventProcessor::new_with_sink(sink.clone())
            }
        };
        let update_processor =
            StreamingUpdateProcessor::new(&self.stream_base_url, sdk_key, &store)
                .map_err(|e| Error::InvalidConfig(Box::new(e)))?;
        Ok(Client {
            config: self.config,
            event_processor,
            update_processor,
            store,
        })
    }
}

impl Client {
    pub fn new(sdk_key: &str) -> Client {
        Client::configure().build(sdk_key).unwrap()
    }

    pub fn configure() -> ClientBuilder {
        ClientBuilder {
            config: Config::default(),
            stream_base_url: DEFAULT_STREAM_BASE_URL.to_string(),
            events_base_url: DEFAULT_EVENTS_BASE_URL.to_string(),
            event_sink: None,
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
        self.evaluate_detail(user, flag_name, default.into())
            .try_map(|val| val.as_bool(), eval::Error::Exception)
    }

    pub fn str_variation_detail(
        &self,
        user: &User,
        flag_name: &str,
        default: &str,
    ) -> Detail<String> {
        self.evaluate_detail(user, flag_name, default.to_string().into())
            .try_map(|val| val.as_string(), eval::Error::Exception)
    }

    pub fn float_variation_detail(
        &self,
        user: &User,
        flag_name: &str,
        default: f64,
    ) -> Detail<f64> {
        self.evaluate_detail(user, flag_name, default.into())
            .try_map(|val| val.as_float(), eval::Error::Exception)
    }

    pub fn int_variation_detail(&self, user: &User, flag_name: &str, default: i64) -> Detail<i64> {
        self.evaluate_detail(user, flag_name, default.into())
            .try_map(|val| val.as_int(), eval::Error::Exception)
    }

    pub fn all_flags_detail(&self, user: &User) -> HashMap<String, Detail<FlagValue>> {
        let store = self.store.lock().unwrap();
        let flags = store.all_flags();
        let evals = flags.iter().map(|(key, flag)| {
            // TODO don't send events
            let val = flag.evaluate(user).map(|v| v.clone());
            (key.clone(), val)
        });
        evals.collect()
    }

    pub fn evaluate_detail(
        &self,
        user: &User,
        flag_name: &str,
        default: FlagValue,
    ) -> Detail<FlagValue> {
        let store = self.store.lock().unwrap();
        let flag = match store.flag(flag_name) {
            Some(flag) => flag,
            None => return Detail::err_default(eval::Error::FlagNotFound, default),
        };

        if user.key().is_none() {
            // TODO still send event in this case
            return Detail::err_default(eval::Error::UserNotSpecified, default);
        }

        let default_for_event = default.clone();

        // TODO can we avoid the clone here if this returns a &FlagValue instead?
        let result = flag.evaluate(user).map(|v| v.clone()).or(default);

        let event = Event::FeatureRequest {
            base: BaseEvent {
                creation_date: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                user: MaybeInlinedUser::new(self.config.inline_users_in_events, user.clone()),
            },
            user_key: user.key().cloned(),
            key: flag.key.clone(),
            default: default_for_event,
            reason: result.reason,
            value: result.value.clone().unwrap(),
            variation: result.variation_index,
            version: flag.version,
            prereq_of: None,
        };
        self.event_processor.send(event);

        result
    }
}

fn trim_base_url(mut url: &str) -> &str {
    while url.ends_with('/') {
        let untrimmed_url = url;
        url = &url[..url.len() - 1];
        debug!("trimming base url: {} -> {}", untrimmed_url, url);
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    use spectral::prelude::*;

    #[test]
    fn test_trim_base_url() {
        assert_eq!(trim_base_url("localhost"), "localhost");
        assert_eq!(trim_base_url("http://localhost"), "http://localhost");

        assert_eq!(trim_base_url("localhost/"), "localhost");
        assert_eq!(trim_base_url("http://localhost/"), "http://localhost");

        assert_eq!(trim_base_url("localhost////////"), "localhost");
    }

    #[test]
    fn can_construct_client_and_override_event_sink() {
        let jsons = Arc::new(RwLock::new(event_sink::TestSink::new()));

        let client = Client::configure()
            .event_sink(jsons.clone())
            .build("dummy_key")
            .unwrap();

        let result = client.bool_variation_detail(
            &User::with_key("foo".to_string()).build(),
            "someFlag",
            false,
        );

        assert_that!(result.value).contains_value(false);

        let jsons = jsons.read().unwrap();
        assert_that!(*jsons).is_empty();
    }
}
