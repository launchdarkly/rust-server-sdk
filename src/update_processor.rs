use std::sync::{Arc, Mutex};

use futures::future::{lazy, Future};
use futures::stream::Stream;
use serde::Deserialize;

use super::eventsource;
use super::store::{AllData, FeatureFlag, FeatureStore};

type Error = String; // TODO enum

#[derive(Deserialize)]
struct PutData {
    path: String,
    data: AllData,
}

#[derive(Deserialize)]
struct PatchData {
    path: String,
    // TODO support segments too
    data: FeatureFlag,
}

pub struct StreamingUpdateProcessor {
    es_client: eventsource::Client,
    store: Arc<Mutex<FeatureStore>>,
}

impl StreamingUpdateProcessor {
    pub fn new(
        base_url: &str,
        sdk_key: &str,
        store: &Arc<Mutex<FeatureStore>>,
    ) -> StreamingUpdateProcessor {
        let stream_url = format!("{}/all", base_url);
        let es_client = eventsource::Client::for_url(&stream_url)
            .header("Authorization", sdk_key)
            .build();
        StreamingUpdateProcessor {
            es_client,
            store: store.clone(),
        }
    }

    pub fn subscribe(&mut self) {
        let store = self.store.clone();
        let event_stream = self.es_client.stream();

        tokio::spawn(lazy(move || {
            event_stream
                .for_each(move |event| {
                    let mut store = store.lock().unwrap();

                    println!("update processor got an event: {}", event.event_type);

                    match event.event_type.as_str() {
                        "put" => process_put(&mut *store, event),
                        "patch" => process_patch(&mut *store, event),
                        typ => Err(format!("oh dear, can't handle event {:?}", typ)),
                    }
                })
                .map_err(|e| println!("update processor got an error: {:?}", e))
        }));
    }
}

fn process_put(store: &mut FeatureStore, event: eventsource::Event) -> Result<(), Error> {
    let put: PutData = serde_json::from_slice(&event["data"])
        .map_err(|e| format!("couldn't parse put event data: {}", e).to_string())?;
    if put.path == "/" {
        Ok(store.init(put.data))
    } else {
        Err(format!("unexpected put to path {}", put.path))
    }
}

fn process_patch(store: &mut FeatureStore, event: eventsource::Event) -> Result<(), Error> {
    let patch: PatchData = serde_json::from_slice(&event["data"])
        .map_err(|e| format!("couldn't parse patch event data: {}", e).to_string())?;
    Ok(store.patch(&patch.path, patch.data))
}
