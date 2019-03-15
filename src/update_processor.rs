use std::sync::{Arc, Mutex};

use futures::future::{lazy, Future};
use futures::stream::Stream;

use super::eventsource;
use super::store::FeatureStore;

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

                    println!("update processor got an event: {:?}", event);

                    let json: serde_json::Value = serde_json::from_slice(&event["data"])
                        .map_err(|e| format!("bad json in event: {}", e).to_string())?;

                    store.data = Some(json);

                    Ok(())
                })
                .map_err(|e| println!("update processor got an error: {:?}", e))
        }));
    }
}
