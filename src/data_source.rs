use parking_lot::RwLock;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};

use eventsource_client as es;
use eventsource_client::ReconnectOptionsBuilder;
use futures::StreamExt;
use launchdarkly_server_sdk_evaluation::{Flag, Segment};
use serde::Deserialize;

use crate::data_store::UpdateError;
use crate::feature_requester::FeatureRequesterError;
use crate::feature_requester_builders::FeatureRequesterFactory;

use super::data_store::{AllData, DataStore, PatchTarget};

#[derive(Debug)]
pub enum Error {
    InvalidEventData {
        event_type: String,
        error: Box<dyn std::error::Error + Send>,
    },
    InvalidPutPath(String),
    InvalidUpdate(UpdateError),
    InvalidEventType(String),
    MissingEventField(String, String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
pub(crate) struct PutData {
    #[serde(default = "String::default")]
    path: String,

    data: AllData<Flag, Segment>,
}

#[derive(Deserialize)]
pub(crate) struct PatchData {
    pub path: String,
    pub data: PatchTarget,
}

#[derive(Deserialize)]
pub(crate) struct DeleteData {
    path: String,
    version: u64,
}

/// Trait for the component that obtains feature flag data in some way and passes it to a data
/// store. The built-in implementations of this are the client's standard streaming or polling
/// behavior.
pub trait DataSource: Send + Sync {
    fn subscribe(
        &self,
        data_store: Arc<RwLock<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    );
}

pub struct StreamingDataSource {
    es_client: es::Client<es::HttpsConnector>,
}

impl StreamingDataSource {
    pub fn new(
        base_url: &str,
        sdk_key: &str,
        initial_reconnect_delay: Duration,
    ) -> std::result::Result<Self, es::Error> {
        let stream_url = format!("{}/all", base_url);
        let client_builder = es::Client::for_url(&stream_url)?;
        let es_client = client_builder
            .reconnect(
                ReconnectOptionsBuilder::new(true)
                    .retry_initial(true)
                    .delay(initial_reconnect_delay)
                    .build(),
            )
            .header("Authorization", sdk_key)?
            .header("User-Agent", &*crate::USER_AGENT)?
            .build();

        Ok(Self { es_client })
    }
}

impl DataSource for StreamingDataSource {
    fn subscribe(
        &self,
        data_store: Arc<RwLock<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    ) {
        let mut event_stream = Box::pin(self.es_client.stream()).fuse();

        tokio::spawn(async move {
            let shutdown_stream = BroadcastStream::new(shutdown_receiver);
            let mut shutdown_future = shutdown_stream.into_future();
            let notify_init = Once::new();
            let mut init_success = true;

            loop {
                futures::select! {
                    _ = shutdown_future => break,
                    event = event_stream.next() => {
                        let event = match event {
                            Some(Ok(event)) => event,
                            Some(Err(e)) => {
                                error!("error on event stream: {:?}", e);
                                // TODO reconnect?
                                break;
                            },
                            None => {
                                error!("unexpected end of event stream");
                                break;
                            }
                        };

                        let data_store = data_store.clone();
                        let mut data_store = data_store.write();

                        debug!("data source got an event: {}", event.event_type);

                        let stored = match event.event_type.as_str() {
                            "put" => process_put(&mut *data_store, event),
                            "patch" => process_patch(&mut *data_store, event),
                            "delete" => process_delete(&mut *data_store, event),
                            _ => Err(Error::InvalidEventType(event.event_type)),
                        };
                        if let Err(e) = stored {
                            init_success = false;
                            error!("error processing update: {:?}", e);
                        }

                        // Only want to notify for the first event.
                        // TODO: When error handling is added this should happen once we are successful,
                        // or if we have encountered an unrecoverable error.
                        notify_init.call_once(|| (init_complete)(init_success));
                    },
                };
            }
        });
    }
}

pub struct PollingDataSource {
    feature_requester_factory: Arc<Mutex<Box<dyn FeatureRequesterFactory>>>,
    poll_interval: Duration,
}

impl PollingDataSource {
    pub fn new(
        feature_requester_factory: Arc<Mutex<Box<dyn FeatureRequesterFactory>>>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            feature_requester_factory,
            poll_interval,
        }
    }
}

impl DataSource for PollingDataSource {
    fn subscribe(
        &self,
        data_store: Arc<RwLock<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    ) {
        let mut feature_requester = match self.feature_requester_factory.lock() {
            Ok(factory) => match factory.build() {
                Ok(requester) => requester,
                Err(e) => {
                    error!("{:?}", e);
                    return;
                }
            },
            Err(e) => {
                error!("{:?}", e);
                return;
            }
        };

        let poll_interval = self.poll_interval;
        tokio::spawn(async move {
            let notify_init = Once::new();

            let mut interval = IntervalStream::new(time::interval(poll_interval)).fuse();

            let shutdown_stream = BroadcastStream::new(shutdown_receiver);
            let mut shutdown_future = shutdown_stream.into_future();

            loop {
                futures::select! {
                    _ = interval.next() => {
                        match feature_requester.get_all() {
                            Ok(all_data) => {
                                let mut data_store = data_store.write();
                                data_store.init(all_data);
                                notify_init.call_once(|| init_complete(true));
                            }
                            Err(FeatureRequesterError::Temporary) => {
                                notify_init.call_once(|| init_complete(false))
                            }
                            Err(FeatureRequesterError::Permanent) => {
                                notify_init.call_once(|| init_complete(false));
                                break;
                            }
                        };
                    },
                    _ = shutdown_future => break
                };
            }
        });
    }
}

pub struct NullDataSource {}

impl NullDataSource {
    pub fn new() -> Self {
        Self {}
    }
}

impl DataSource for NullDataSource {
    fn subscribe(
        &self,
        _datastore: Arc<RwLock<dyn DataStore>>,
        _init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        _shutdown_receiver: broadcast::Receiver<()>,
    ) {
    }
}

#[cfg(test)]
pub(crate) struct MockDataSource {
    delay_init: u64,
}

#[cfg(test)]
impl MockDataSource {
    pub fn new_with_init_delay(delay_init: u64) -> Self {
        return MockDataSource { delay_init };
    }
}

#[cfg(test)]
impl DataSource for MockDataSource {
    fn subscribe(
        &self,
        _datastore: Arc<RwLock<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        _shutdown_receiver: broadcast::Receiver<()>,
    ) {
        let delay_init = self.delay_init;
        if self.delay_init != 0 {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay_init)).await;
                (init_complete)(true);
            });
        } else {
            (init_complete)(true);
        }
    }
}

fn event_field<'a>(event: &'a es::Event, field: &'a str) -> Result<&'a [u8]> {
    event
        .field(field)
        .ok_or_else(|| Error::MissingEventField(event.event_type.clone(), field.to_string()))
}

fn parse_event_data<'a, T: Deserialize<'a>>(event: &'a es::Event) -> Result<T> {
    let data = event_field(event, "data")?;
    serde_json::from_slice(data).map_err(|e| Error::InvalidEventData {
        event_type: event.event_type.clone(),
        error: Box::new(e),
    })
}

fn process_put(data_store: &mut dyn DataStore, event: es::Event) -> Result<()> {
    let put: PutData = parse_event_data(&event)?;
    if put.path == "/" || put.path.is_empty() {
        data_store.init(put.data);
        Ok(())
    } else {
        Err(Error::InvalidPutPath(put.path))
    }
}

fn process_patch(data_store: &mut dyn DataStore, event: es::Event) -> Result<()> {
    let patch: PatchData = parse_event_data(&event)?;
    data_store
        .patch(&patch.path, patch.data)
        .map_err(Error::InvalidUpdate)
}

fn process_delete(data_store: &mut dyn DataStore, event: es::Event) -> Result<()> {
    let delete: DeleteData = parse_event_data(&event)?;
    data_store
        .delete(&delete.path, delete.version)
        .map_err(Error::InvalidUpdate)
}
