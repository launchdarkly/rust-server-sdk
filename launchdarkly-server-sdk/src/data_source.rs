use es::{Client, ClientBuilder, HttpsConnector, ReconnectOptionsBuilder};
use eventsource_client as es;
use futures::StreamExt;
use launchdarkly_server_sdk_evaluation::{Flag, Segment};
use parking_lot::RwLock;
use serde::Deserialize;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};

use super::stores::store_types::{AllData, DataKind, PatchTarget, StorageItem};
use crate::feature_requester::FeatureRequesterError;
use crate::feature_requester_builders::FeatureRequesterFactory;
use crate::stores::store::{DataStore, UpdateError};
use crate::LAUNCHDARKLY_TAGS_HEADER;

const FLAGS_PREFIX: &str = "/flags/";
const SEGMENTS_PREFIX: &str = "/segments/";

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    InvalidEventData {
        event_type: String,
        error: Box<dyn std::error::Error + Send>,
    },
    InvalidPath(String),
    InvalidUpdate(UpdateError),
    InvalidEventType(String),
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
    es_client: Box<dyn Client>,
}

impl StreamingDataSource {
    fn new_builder(
        base_url: &str,
        sdk_key: &str,
        initial_reconnect_delay: Duration,
        tags: &Option<String>,
    ) -> es::Result<ClientBuilder> {
        let stream_url = format!("{}/all", base_url);

        let client_builder = ClientBuilder::for_url(&stream_url)?;
        let mut client_builder = client_builder
            .reconnect(
                ReconnectOptionsBuilder::new(true)
                    .retry_initial(true)
                    .delay(initial_reconnect_delay)
                    .build(),
            )
            .header("Authorization", sdk_key)?
            .header("User-Agent", &crate::USER_AGENT)?;

        if let Some(tags) = tags {
            client_builder = client_builder.header(LAUNCHDARKLY_TAGS_HEADER, tags)?;
        }

        Ok(client_builder)
    }

    pub fn new(
        base_url: &str,
        sdk_key: &str,
        initial_reconnect_delay: Duration,
        tags: &Option<String>,
    ) -> std::result::Result<Self, es::Error> {
        let client_builder =
            StreamingDataSource::new_builder(base_url, sdk_key, initial_reconnect_delay, tags)?;

        Ok(Self {
            es_client: Box::new(client_builder.build()),
        })
    }

    pub fn new_with_connector(
        base_url: &str,
        sdk_key: &str,
        initial_reconnect_delay: Duration,
        tags: &Option<String>,
        connector: HttpsConnector,
    ) -> std::result::Result<Self, es::Error> {
        let client_builder =
            StreamingDataSource::new_builder(base_url, sdk_key, initial_reconnect_delay, tags)?;

        Ok(Self {
            es_client: Box::new(client_builder.build_with_conn(connector)),
        })
    }
}

impl DataSource for StreamingDataSource {
    fn subscribe(
        &self,
        data_store: Arc<RwLock<dyn DataStore>>,
        init_complete: Arc<dyn Fn(bool) + Send + Sync>,
        shutdown_receiver: broadcast::Receiver<()>,
    ) {
        let mut event_stream = self.es_client.stream().fuse();

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
                            Some(Ok(event)) => match event {
                                es::SSE::Comment(str)=> {
                                    debug!("data source got a comment: {}", str);
                                    continue;
                                },
                                es::SSE::Event(ev) => ev,
                            },
                            Some(Err(e)) => {
                                error!("error on event stream: {:?}", e);

                                match e {
                                    es::Error::Eof => {
                                        continue;
                                    }
                                    _ => {
                                        debug!("unhandled error; break");
                                        break;
                                    }
                                }
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
    tags: Option<String>,
}

impl PollingDataSource {
    pub fn new(
        feature_requester_factory: Arc<Mutex<Box<dyn FeatureRequesterFactory>>>,
        poll_interval: Duration,
        tags: Option<String>,
    ) -> Self {
        Self {
            feature_requester_factory,
            poll_interval,
            tags,
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
            Ok(factory) => match factory.build(self.tags.clone()) {
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
                                warn!("feature requester has returned a temporary failure");
                            }
                            Err(FeatureRequesterError::Permanent) => {
                                error!("feature requester has returned a permanent failure");
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

fn parse_event_data<'a, T: Deserialize<'a>>(event: &'a es::Event) -> Result<T> {
    serde_json::from_slice(event.data.as_ref()).map_err(|e| Error::InvalidEventData {
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
        Err(Error::InvalidPath(put.path))
    }
}

fn process_patch(data_store: &mut dyn DataStore, event: es::Event) -> Result<()> {
    let patch: PatchData = parse_event_data(&event)?;
    let (_, key) = path_to_key(&patch.path)?;

    data_store
        .upsert(key, patch.data)
        .map_err(Error::InvalidUpdate)
}

fn process_delete(data_store: &mut dyn DataStore, event: es::Event) -> Result<()> {
    let delete: DeleteData = parse_event_data(&event)?;
    let (kind, key) = path_to_key(&delete.path)?;
    let target = match kind {
        DataKind::Flag => PatchTarget::Flag(StorageItem::Tombstone(delete.version)),
        DataKind::Segment => PatchTarget::Segment(StorageItem::Tombstone(delete.version)),
    };

    data_store.upsert(key, target).map_err(Error::InvalidUpdate)
}

fn path_to_key(path: &str) -> Result<(DataKind, &str)> {
    if let Some(flag_key) = path.strip_prefix(FLAGS_PREFIX) {
        Ok((DataKind::Flag, flag_key))
    } else if let Some(segment_key) = path.strip_prefix(SEGMENTS_PREFIX) {
        Ok((DataKind::Segment, segment_key))
    } else {
        Err(Error::InvalidPath(path.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use mockito::{mock, Matcher};
    use parking_lot::RwLock;
    use test_case::test_case;
    use tokio::sync::broadcast;

    use super::{DataSource, PollingDataSource, StreamingDataSource};
    use crate::{
        feature_requester_builders::ReqwestFeatureRequesterBuilder,
        stores::store::InMemoryDataStore, LAUNCHDARKLY_TAGS_HEADER,
    };

    #[test_case(Some("application-id/abc:application-sha/xyz".into()), "application-id/abc:application-sha/xyz")]
    #[test_case(None, Matcher::Missing)]
    #[tokio::test(flavor = "multi_thread")]
    async fn streaming_source_passes_along_tags_header(
        tag: Option<String>,
        matcher: impl Into<Matcher>,
    ) {
        let mock_endpoint = mock("GET", "/all")
            .with_status(200)
            .with_body("event:one\ndata:One\n\n")
            .expect_at_least(1)
            .match_header(LAUNCHDARKLY_TAGS_HEADER, matcher)
            .create();

        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let initialized = Arc::new(AtomicBool::new(false));

        let streaming = StreamingDataSource::new(
            &mockito::server_url(),
            "sdk-key",
            Duration::from_secs(0),
            &tag,
        )
        .unwrap();

        let data_store = Arc::new(RwLock::new(InMemoryDataStore::new()));

        let init_state = initialized.clone();
        streaming.subscribe(
            data_store,
            Arc::new(move |success| init_state.store(success, Ordering::SeqCst)),
            shutdown_tx.subscribe(),
        );

        let mut attempts = 0;
        loop {
            if initialized.load(Ordering::SeqCst) {
                break;
            }

            attempts += 1;
            if attempts > 10 {
                break;
            }

            std::thread::sleep(Duration::from_millis(100));
        }

        let _ = shutdown_tx.send(());

        mock_endpoint.assert();
    }

    #[test_case(Some("application-id/abc:application-sha/xyz".into()), "application-id/abc:application-sha/xyz")]
    #[test_case(None, Matcher::Missing)]
    #[tokio::test(flavor = "multi_thread")]
    async fn polling_source_passes_along_tags_header(
        tag: Option<String>,
        matcher: impl Into<Matcher>,
    ) {
        let mock_endpoint = mock("GET", "/sdk/latest-all")
            .with_status(200)
            .with_body("{}")
            .expect_at_least(1)
            .match_header(LAUNCHDARKLY_TAGS_HEADER, matcher)
            .create();

        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let initialized = Arc::new(AtomicBool::new(false));

        let reqwest_builder =
            ReqwestFeatureRequesterBuilder::new(&mockito::server_url(), "sdk-key");

        let polling = PollingDataSource::new(
            Arc::new(Mutex::new(Box::new(reqwest_builder))),
            Duration::from_secs(10),
            tag,
        );

        let data_store = Arc::new(RwLock::new(InMemoryDataStore::new()));

        let init_state = initialized.clone();
        polling.subscribe(
            data_store,
            Arc::new(move |success| init_state.store(success, Ordering::SeqCst)),
            shutdown_tx.subscribe(),
        );

        let mut attempts = 0;
        loop {
            if initialized.load(Ordering::SeqCst) {
                break;
            }

            attempts += 1;
            if attempts > 10 {
                break;
            }

            std::thread::sleep(Duration::from_millis(100));
        }

        let _ = shutdown_tx.send(());

        mock_endpoint.assert();
    }
}
