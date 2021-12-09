use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventsource_client as es;
use eventsource_client::ReconnectOptionsBuilder;
use futures::TryStreamExt;
use rust_server_sdk_evaluation::{Flag, Segment};
use serde::Deserialize;

use super::data_store::{AllData, DataStore, PatchTarget};

#[derive(Debug)]
pub enum Error {
    InvalidEventData {
        event_type: String,
        error: Box<dyn std::error::Error + Send>,
    },
    InvalidPutPath(String),
    InvalidUpdate(String),
    InvalidEventType(String),
    MissingEventField(String, String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
pub(crate) struct PutData {
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
pub trait DataSource: Send {
    fn subscribe(
        &mut self,
        data_store: Arc<Mutex<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
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
        &mut self,
        data_store: Arc<Mutex<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
    ) {
        let mut event_stream = Box::pin(self.es_client.stream());

        tokio::spawn(async move {
            let mut first_pass = true;
            let mut init_success = true;
            loop {
                let event = match event_stream.try_next().await {
                    Ok(Some(event)) => event,
                    Ok(None) => {
                        error!("unexpected end of event stream");
                        break;
                    }
                    Err(e) => {
                        error!("error on event stream: {:?}", e);
                        // TODO reconnect?
                        break;
                    }
                };

                let mut data_store = data_store.lock().unwrap();

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
                if first_pass {
                    (init_complete)(init_success);
                    first_pass = false;
                }
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
    fn subscribe(&mut self, _: Arc<Mutex<dyn DataStore>>, _: Arc<dyn Fn(bool) + Send + Sync>) {}
}

#[cfg(test)]
pub(crate) struct MockDataSource {
    data_store: Option<Arc<Mutex<dyn DataStore>>>,
    delay_init: u64,
}

#[cfg(test)]
impl MockDataSource {
    pub fn new_with_init_delay(delay_init: u64) -> Self {
        return MockDataSource {
            data_store: None,
            delay_init,
        };
    }

    pub fn patch(&self, patch: PatchData) -> Result<()> {
        self.data_store
            .as_ref()
            .expect("not subscribed")
            .lock()
            .unwrap()
            .patch(&patch.path, patch.data)
            .map_err(Error::InvalidUpdate)
    }
}

#[cfg(test)]
impl DataSource for MockDataSource {
    fn subscribe(
        &mut self,
        datastore: Arc<Mutex<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
    ) {
        self.data_store = Some(datastore);
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
    if put.path == "/" {
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
