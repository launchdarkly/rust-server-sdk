use std::sync::{Arc, Mutex};

use eventsource_client as es;
use futures::future::{lazy, Future};
use futures::stream::Stream;
use serde::Deserialize;

use super::store::{AllData, FeatureStore, PatchTarget};

#[derive(Debug)]
pub enum Error {
    EventSource(es::Error),
    InvalidEventData(String, Box<dyn std::error::Error + Send>),
    InvalidEventPath(String, String),
    InvalidEventType(String),
    MissingEventField(String, String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
struct PutData {
    path: String,
    data: AllData,
}

#[derive(Deserialize)]
struct PatchData {
    path: String,
    data: PatchTarget,
}

#[derive(Deserialize)]
struct DeleteData {
    path: String,
    // TODO care about version
}

pub struct StreamingUpdateProcessor {
    es_client: es::Client,
    store: Arc<Mutex<FeatureStore>>,
}

impl StreamingUpdateProcessor {
    pub fn new(
        base_url: &str,
        sdk_key: &str,
        store: &Arc<Mutex<FeatureStore>>,
    ) -> Result<StreamingUpdateProcessor> {
        let stream_url = format!("{}/all", base_url);
        let client_builder = es::Client::for_url(&stream_url).map_err(|e| Error::EventSource(e))?;
        let es_client = client_builder
            .header("Authorization", sdk_key)
            .unwrap()
            .build();
        Ok(StreamingUpdateProcessor {
            es_client,
            store: store.clone(),
        })
    }

    pub fn subscribe(&mut self) {
        let store = self.store.clone();
        let event_stream = self.es_client.stream();

        tokio::spawn(lazy(move || {
            event_stream
                .map_err(|e| Error::EventSource(e))
                .for_each(move |event| {
                    let mut store = store.lock().unwrap();

                    debug!("update processor got an event: {}", event.event_type);

                    match event.event_type.as_str() {
                        "put" => process_put(&mut *store, event),
                        "patch" => process_patch(&mut *store, event),
                        "delete" => process_delete(&mut *store, event),
                        _ => Err(Error::InvalidEventType(event.event_type)),
                    }
                })
                .map_err(|e| error!("update processor got an error: {:?}", e))
        }));
    }
}

fn event_field<'a>(event: &'a es::Event, field: &'a str) -> Result<&'a [u8]> {
    event.field(field).ok_or(Error::MissingEventField(
        event.event_type.clone(),
        field.to_string(),
    ))
}

fn parse_event_data<'a, T: Deserialize<'a>>(event: &'a es::Event) -> Result<T> {
    let data = event_field(&event, "data")?;
    serde_json::from_slice(data)
        .map_err(|e| Error::InvalidEventData(event.event_type.clone(), Box::new(e)))
}

fn process_put(store: &mut FeatureStore, event: es::Event) -> Result<()> {
    let put: PutData = parse_event_data(&event)?;
    if put.path == "/" {
        Ok(store.init(put.data))
    } else {
        Err(Error::InvalidEventPath(
            event.event_type,
            put.path.to_string(),
        ))
    }
}

fn process_patch(store: &mut FeatureStore, event: es::Event) -> Result<()> {
    let patch: PatchData = parse_event_data(&event)?;
    Ok(store.patch(&patch.path, patch.data))
}

fn process_delete(store: &mut FeatureStore, event: es::Event) -> Result<()> {
    let delete: DeleteData = parse_event_data(&event)?;
    Ok(store.delete(&delete.path))
}
