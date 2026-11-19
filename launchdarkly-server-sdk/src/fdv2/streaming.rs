use std::pin::Pin;
use std::time::Duration;

use eventsource_client::{
    self as eventsource, Client, ClientBuilder, ReconnectOptionsBuilder, SSE,
};
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use http::Uri;
use launchdarkly_sdk_transport::HttpTransport;
use rand::Rng;
use tokio::sync::watch;
use tokio::time::{sleep_until, Instant};

use super::data_system::SynchronizerFactory;
use super::model::Selector;
use super::protocol::{FDv2ProtocolHandler, ProtocolError, ProtocolResult};
use super::request_headers::RequestHeaders;
use super::source::{
    read_fallback_directive, ErrorInfo, ErrorKind, FDv1FallbackDirective, FDv2SourceEvent,
    FDv2SourceResult, Synchronizer,
};
use super::url::build_fdv2_url;
use crate::reqwest::is_http_error_recoverable;
use crate::stores::change_set::ChangeSet;

const STREAM_ENDPOINT: &str = "sdk/stream";
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(30);
/// A stream that stays healthy for this long sets the restart delay back to the initial delay.
const RESTART_BACKOFF_RESET_INTERVAL: Duration = Duration::from_secs(60);

/// Computes the delay before the synchronizer restarts the stream after invalid data. The
/// eventsource client applies its backoff only to the reconnects that it starts itself, so the
/// synchronizer applies this backoff to the restarts that it starts.
struct RestartBackoff {
    initial_delay: Duration,
    attempts: u32,
    healthy_since: Option<Instant>,
}

impl RestartBackoff {
    fn new(initial_delay: Duration) -> Self {
        Self {
            initial_delay,
            attempts: 0,
            healthy_since: None,
        }
    }

    /// Records a valid result on the current stream.
    fn record_valid_result(&mut self, now: Instant) {
        self.healthy_since.get_or_insert(now);
    }

    /// Records that the synchronizer dropped the stream. If the stream was healthy for
    /// `RESTART_BACKOFF_RESET_INTERVAL` or longer, the next delay is the initial delay.
    fn record_stream_end(&mut self, now: Instant) {
        if let Some(since) = self.healthy_since.take() {
            if now.duration_since(since) >= RESTART_BACKOFF_RESET_INTERVAL {
                self.attempts = 0;
            }
        }
    }

    /// Returns the delay before the next restart. The delay starts at the initial delay and
    /// doubles with each restart, up to `RECONNECT_DELAY_MAX`. Jitter removes up to half of it.
    fn next_delay(&mut self) -> Duration {
        let delay = self
            .initial_delay
            .saturating_mul(2u32.saturating_pow(self.attempts))
            .min(RECONNECT_DELAY_MAX);
        self.attempts = self.attempts.saturating_add(1);
        delay.mul_f64(rand::rng().random_range(0.5..=1.0))
    }
}

fn interrupted(
    kind: ErrorKind,
    message: impl Into<String>,
    fallback: Option<FDv1FallbackDirective>,
) -> FDv2SourceEvent {
    FDv2SourceEvent {
        result: FDv2SourceResult::Interrupted(ErrorInfo {
            kind,
            message: message.into(),
        }),
        fdv1_fallback: fallback,
    }
}

type EventStream = Pin<Box<dyn Stream<Item = eventsource::Result<SSE>> + Send>>;

pub(crate) struct StreamingSynchronizer<T: HttpTransport + Clone + Send + Sync + 'static> {
    transport: T,
    base_url: String,
    headers: RequestHeaders,
    initial_reconnect_delay: Duration,

    url_sender: watch::Sender<Uri>,
    stream: Option<EventStream>,

    handler: FDv2ProtocolHandler,
    latest_fdv1_fallback: Option<FDv1FallbackDirective>,

    restart_backoff: RestartBackoff,
    /// When set, the next stream start waits until this time.
    restart_at: Option<Instant>,
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> StreamingSynchronizer<T> {
    pub(crate) fn new(
        transport: T,
        base_url: String,
        headers: RequestHeaders,
        initial_reconnect_delay: Duration,
    ) -> Self {
        let (url_sender, _) = watch::channel(Uri::default());
        Self {
            transport,
            base_url,
            headers,
            initial_reconnect_delay,
            url_sender,
            stream: None,
            handler: FDv2ProtocolHandler::new(),
            latest_fdv1_fallback: None,
            restart_backoff: RestartBackoff::new(initial_reconnect_delay),
            restart_at: None,
        }
    }

    fn build_stream_uri(&self, selector: &Selector) -> Result<Uri, http::uri::InvalidUri> {
        build_fdv2_url(&self.base_url, STREAM_ENDPOINT, selector).parse::<Uri>()
    }

    fn drop_connection(&mut self) {
        self.stream = None;
        self.handler.reset();
        self.restart_backoff.record_stream_end(Instant::now());
    }

    /// Drops the connection after invalid data. The next stream start waits for a backoff
    /// delay, so that data the SDK cannot use does not cause a tight reconnect loop.
    fn restart_after_invalid_data(&mut self) {
        self.drop_connection();
        let delay = self.restart_backoff.next_delay();
        debug!("FDv2 streaming source will restart the stream in {delay:?}");
        self.restart_at = Some(Instant::now() + delay);
    }

    fn handle_sse_event(&mut self, event: eventsource::Event) -> Option<FDv2SourceEvent> {
        // Unrecognized events are ignored without parsing their data, leaving
        // room for future protocol additions.
        if !FDv2ProtocolHandler::is_known_event(&event.event_type) {
            return None;
        }

        let data: serde_json::Value = match serde_json::from_str(&event.data) {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("could not parse FDv2 streaming event data: {e}");
                let fallback = self.latest_fdv1_fallback.clone();
                self.restart_after_invalid_data();
                return Some(interrupted(ErrorKind::InvalidData, msg, fallback));
            }
        };

        match self.handler.handle_event(&event.event_type, data) {
            ProtocolResult::None => None,
            ProtocolResult::ChangeSet(wire_cs) => match ChangeSet::try_from(wire_cs) {
                Ok(cs) => {
                    self.restart_backoff.record_valid_result(Instant::now());
                    Some(FDv2SourceEvent {
                        result: FDv2SourceResult::ChangeSet(cs),
                        fdv1_fallback: self.latest_fdv1_fallback.clone(),
                    })
                }
                Err(e) => {
                    let msg = format!("FDv2 streaming changeset could not be translated: {e}");
                    let fallback = self.latest_fdv1_fallback.clone();
                    self.restart_after_invalid_data();
                    Some(interrupted(ErrorKind::InvalidData, msg, fallback))
                }
            },
            ProtocolResult::Goodbye(g) => {
                if let Some(reason) = &g.reason {
                    info!("FDv2 streaming source received goodbye: {reason}");
                }
                // An explicit directive on the goodbye takes precedence over
                // the most recent response header.
                let fallback = g
                    .fdv1_fallback()
                    .or_else(|| self.latest_fdv1_fallback.clone());
                self.drop_connection();
                Some(FDv2SourceEvent {
                    result: FDv2SourceResult::Goodbye,
                    fdv1_fallback: fallback,
                })
            }
            ProtocolResult::Error(ProtocolError::Server(err)) => {
                let msg = format!(
                    "An issue was encountered receiving updates for payload '{}' with reason: '{}'. Automatic retry will occur.",
                    err.id.as_deref().unwrap_or(""),
                    err.reason,
                );
                Some(interrupted(
                    ErrorKind::ErrorResponse { status_code: 0 },
                    msg,
                    self.latest_fdv1_fallback.clone(),
                ))
            }
            ProtocolResult::Error(ProtocolError::Protocol(msg))
            | ProtocolResult::Error(ProtocolError::JsonParse(msg)) => {
                let fallback = self.latest_fdv1_fallback.clone();
                self.restart_after_invalid_data();
                Some(interrupted(ErrorKind::InvalidData, msg, fallback))
            }
        }
    }

    fn handle_unexpected_response(&mut self, response: &eventsource::Response) -> FDv2SourceEvent {
        let fallback =
            read_fallback_directive(|name| response.get_header_value(name).ok().flatten())
                .or_else(|| self.latest_fdv1_fallback.clone());
        let status = response.status();
        let info = ErrorInfo {
            kind: ErrorKind::ErrorResponse {
                status_code: status,
            },
            message: format!("FDv2 streaming request received HTTP status {status}"),
        };
        if is_http_error_recoverable(status) {
            FDv2SourceEvent {
                result: FDv2SourceResult::Interrupted(info),
                fdv1_fallback: fallback,
            }
        } else {
            self.drop_connection();
            FDv2SourceEvent {
                result: FDv2SourceResult::TerminalError(info),
                fdv1_fallback: fallback,
            }
        }
    }

    fn handle_sse_error(&mut self, error: eventsource::Error) -> Option<FDv2SourceEvent> {
        match error {
            eventsource::Error::Eof => None,
            eventsource::Error::UnexpectedResponse(response, _) => {
                Some(self.handle_unexpected_response(&response))
            }
            eventsource::Error::MaxRedirectLimitReached(_)
            | eventsource::Error::InvalidParameter(_)
            | eventsource::Error::MalformedLocationHeader(_) => {
                let fallback = self.latest_fdv1_fallback.clone();
                self.drop_connection();
                Some(FDv2SourceEvent {
                    result: FDv2SourceResult::TerminalError(ErrorInfo {
                        kind: ErrorKind::Unknown,
                        message: format!("FDv2 streaming connection failed: {error}"),
                    }),
                    fdv1_fallback: fallback,
                })
            }
            other => Some(interrupted(
                ErrorKind::NetworkError,
                format!("{other}"),
                self.latest_fdv1_fallback.clone(),
            )),
        }
    }

    fn start(&mut self) -> Result<(), eventsource::Error> {
        let initial_uri = self.url_sender.borrow().clone();
        let mut client_builder = ClientBuilder::for_url(&initial_uri.to_string())?
            .dynamic_url(self.url_sender.subscribe())
            .reconnect(
                ReconnectOptionsBuilder::new(true)
                    .retry_initial(true)
                    .delay(self.initial_reconnect_delay)
                    .delay_max(RECONNECT_DELAY_MAX)
                    .build(),
            );
        for (name, value) in self.headers.iter() {
            client_builder = client_builder.header(name, value)?;
        }
        let client = client_builder.build_with_transport(self.transport.clone());

        self.stream = Some(Box::pin(client.stream()));
        Ok(())
    }
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> Synchronizer for StreamingSynchronizer<T> {
    fn next(&mut self, selector: Selector) -> BoxFuture<'_, FDv2SourceEvent> {
        Box::pin(async move {
            let uri = match self.build_stream_uri(&selector) {
                Ok(u) => u,
                Err(e) => {
                    return FDv2SourceEvent {
                        result: FDv2SourceResult::TerminalError(ErrorInfo {
                            kind: ErrorKind::Unknown,
                            message: format!("could not build FDv2 streaming URL: {e}"),
                        }),
                        fdv1_fallback: None,
                    };
                }
            };

            self.url_sender.send_replace(uri);

            if self.stream.is_none() {
                // Wait for the backoff delay of a restart after invalid data. The restart time
                // stays set until the wait completes, so a cancelled call does not skip it.
                if let Some(restart_at) = self.restart_at {
                    sleep_until(restart_at).await;
                    self.restart_at = None;
                }
                if let Err(e) = self.start() {
                    return FDv2SourceEvent {
                        result: FDv2SourceResult::TerminalError(ErrorInfo {
                            kind: ErrorKind::Unknown,
                            message: format!("could not start FDv2 streaming client: {e}"),
                        }),
                        fdv1_fallback: None,
                    };
                }
            }

            loop {
                let item = self.stream.as_mut().unwrap().next().await;
                match item {
                    Some(Ok(SSE::Connected(details))) => {
                        self.latest_fdv1_fallback = read_fallback_directive(|name| {
                            details.response().get_header_value(name).ok().flatten()
                        });
                    }
                    Some(Ok(SSE::Comment(_))) => continue,
                    Some(Ok(SSE::Event(ev))) => {
                        if let Some(out) = self.handle_sse_event(ev) {
                            return out;
                        }
                    }
                    Some(Err(e)) => {
                        if let Some(out) = self.handle_sse_error(e) {
                            return out;
                        }
                    }
                    None => {
                        let fallback = self.latest_fdv1_fallback.clone();
                        self.drop_connection();
                        return FDv2SourceEvent {
                            result: FDv2SourceResult::TerminalError(ErrorInfo {
                                kind: ErrorKind::NetworkError,
                                message: "FDv2 streaming connection exhausted".into(),
                            }),
                            fdv1_fallback: fallback,
                        };
                    }
                }
            }
        })
    }

    fn name(&self) -> &str {
        "FDv2 streaming synchronizer"
    }
}

/// Builds a fresh `StreamingSynchronizer` each time the synchronizer activates.
pub(crate) struct StreamingSynchronizerFactory<T: HttpTransport + Clone + Send + Sync + 'static> {
    transport: T,
    base_url: String,
    headers: RequestHeaders,
    initial_reconnect_delay: Duration,
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> StreamingSynchronizerFactory<T> {
    pub(crate) fn new(
        transport: T,
        base_url: String,
        headers: RequestHeaders,
        initial_reconnect_delay: Duration,
    ) -> Self {
        Self {
            transport,
            base_url,
            headers,
            initial_reconnect_delay,
        }
    }
}

impl<T: HttpTransport + Clone + Send + Sync + 'static> SynchronizerFactory
    for StreamingSynchronizerFactory<T>
{
    fn create(&self) -> Box<dyn Synchronizer> {
        Box::new(StreamingSynchronizer::new(
            self.transport.clone(),
            self.base_url.clone(),
            self.headers.clone(),
            self.initial_reconnect_delay,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Request, StatusCode};
    use launchdarkly_sdk_transport::{ByteStream, ResponseFuture, TransportError};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::time::Instant;

    #[derive(Clone)]
    struct NoopTransport;
    impl HttpTransport for NoopTransport {
        fn request(&self, _: Request<Option<Bytes>>) -> ResponseFuture {
            Box::pin(async { Err(TransportError::new(std::io::Error::other("unused"))) })
        }
    }

    fn test_headers() -> RequestHeaders {
        RequestHeaders::new("sdk-key", None, "test-instance")
    }

    fn new_synchronizer() -> StreamingSynchronizer<NoopTransport> {
        StreamingSynchronizer::new(
            NoopTransport,
            "http://example.com".into(),
            test_headers(),
            Duration::ZERO,
        )
    }

    fn event(event_type: &str, data: serde_json::Value) -> eventsource::Event {
        eventsource::Event {
            event_type: event_type.into(),
            data: data.to_string(),
            id: None,
            retry: None,
        }
    }

    fn eventsource_response(status: u16, headers: &[(&str, &str)]) -> eventsource::Response {
        let mut map = HeaderMap::new();
        for (k, v) in headers {
            map.insert(
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            );
        }
        eventsource::Response::new(StatusCode::from_u16(status).unwrap(), map)
    }

    #[test]
    fn build_stream_uri_uses_stream_endpoint_and_selector() {
        let sync = new_synchronizer();
        let uri = sync.build_stream_uri(&Some("state-1".into())).unwrap();
        assert_eq!(
            uri.to_string(),
            "http://example.com/sdk/stream?basis=state-1"
        );
    }

    fn feed(
        sync: &mut StreamingSynchronizer<NoopTransport>,
        event_type: &str,
        data: serde_json::Value,
    ) -> Option<FDv2SourceEvent> {
        sync.handle_sse_event(event(event_type, data))
    }

    fn intent(code: &str) -> serde_json::Value {
        json!({"payloads": [{
            "id": "p", "target": 1, "intentCode": code, "reason": "payload-missing",
        }]})
    }

    #[test]
    fn full_payload_cycle_emits_changeset_with_latest_fallback() {
        let mut sync = new_synchronizer();
        sync.latest_fdv1_fallback = Some(FDv1FallbackDirective {
            ttl: Duration::from_secs(30),
        });
        let flag = crate::test_common::basic_flag("my-flag");
        let flag_json = serde_json::to_value(&flag).unwrap();

        assert!(feed(&mut sync, "server-intent", intent("xfer-full")).is_none());
        assert!(feed(
            &mut sync,
            "put-object",
            json!({"version": 1, "kind": "flag", "key": "my-flag", "object": flag_json}),
        )
        .is_none());
        let out = feed(&mut sync, "payload-transferred", json!({"state": "s-1"}))
            .expect("changeset produced");

        let FDv2SourceResult::ChangeSet(cs) = out.result else {
            panic!("expected ChangeSet, got {:?}", out.result);
        };
        assert_eq!(cs.changes.len(), 1);
        assert_eq!(cs.selector.as_deref(), Some("s-1"));
        assert_eq!(
            out.fdv1_fallback.map(|d| d.ttl),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn malformed_event_json_returns_interrupted() {
        let mut sync = new_synchronizer();
        let bad = eventsource::Event {
            event_type: "put-object".into(),
            data: "not json".into(),
            id: None,
            retry: None,
        };
        let out = sync.handle_sse_event(bad).expect("interrupted");
        let FDv2SourceResult::Interrupted(err) = out.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::InvalidData);
    }

    #[test]
    fn unknown_event_is_ignored_without_parsing_body() {
        let mut sync = new_synchronizer();

        // An unrecognized event with a non-JSON body must be ignored, not parsed.
        let unknown = eventsource::Event {
            event_type: "whatever".into(),
            data: "not json".into(),
            id: None,
            retry: None,
        };
        assert!(sync.handle_sse_event(unknown).is_none());

        // The stream keeps working: a following valid cycle still produces a changeset.
        let flag = crate::test_common::basic_flag("my-flag");
        let flag_json = serde_json::to_value(&flag).unwrap();
        assert!(feed(&mut sync, "server-intent", intent("xfer-full")).is_none());
        assert!(feed(
            &mut sync,
            "put-object",
            json!({"version": 1, "kind": "flag", "key": "my-flag", "object": flag_json}),
        )
        .is_none());
        assert!(feed(&mut sync, "payload-transferred", json!({"state": "s-1"})).is_some());
    }

    #[test]
    fn unknown_kind_objects_are_ignored() {
        testing_logger::setup();
        let mut sync = new_synchronizer();
        let flag = crate::test_common::basic_flag("my-flag");
        let flag_json = serde_json::to_value(&flag).unwrap();

        // Stream a payload that mixes unknown-kind objects with a flag.
        let outputs = [
            feed(&mut sync, "server-intent", intent("xfer-full")),
            feed(
                &mut sync,
                "put-object",
                json!({"version": 1, "kind": "some-future-kind", "key": "x", "object": {"foo": 1}}),
            ),
            feed(
                &mut sync,
                "put-object",
                json!({"version": 1, "kind": "flag", "key": "my-flag", "object": flag_json}),
            ),
            feed(
                &mut sync,
                "delete-object",
                json!({"version": 2, "kind": "some-future-kind", "key": "y"}),
            ),
        ];
        let out = feed(&mut sync, "payload-transferred", json!({"state": "s-1"}))
            .expect("changeset produced");

        // The payload completes with only the flag, and nothing is logged at warn or above.
        assert!(outputs.iter().all(Option::is_none));
        let FDv2SourceResult::ChangeSet(cs) = out.result else {
            panic!("expected ChangeSet, got {:?}", out.result);
        };
        assert_eq!(cs.changes.len(), 1);
        assert!(matches!(
            &cs.changes[0],
            crate::stores::change_set::ItemChange::Flag { key, .. } if key == "my-flag"
        ));
        assert_eq!(cs.selector.as_deref(), Some("s-1"));
        testing_logger::validate(|logs| {
            assert!(logs.iter().all(|l| l.level > log::Level::Warn));
        });
    }

    #[test]
    fn goodbye_ttl_takes_precedence_over_latest_fallback() {
        let mut sync = new_synchronizer();
        sync.latest_fdv1_fallback = Some(FDv1FallbackDirective {
            ttl: Duration::from_secs(30),
        });
        let out = feed(
            &mut sync,
            "goodbye",
            json!({"reason": "rotating", "protocolFallbackTTL": 90}),
        )
        .expect("goodbye produced");

        assert!(matches!(out.result, FDv2SourceResult::Goodbye));
        assert_eq!(
            out.fdv1_fallback.map(|d| d.ttl),
            Some(Duration::from_secs(90))
        );
    }

    #[test]
    fn goodbye_without_ttl_falls_back_to_latest() {
        let mut sync = new_synchronizer();
        sync.latest_fdv1_fallback = Some(FDv1FallbackDirective {
            ttl: Duration::from_secs(30),
        });
        let out = feed(&mut sync, "goodbye", json!({"reason": "bye"})).expect("goodbye");
        assert_eq!(
            out.fdv1_fallback.map(|d| d.ttl),
            Some(Duration::from_secs(30))
        );
    }

    #[test]
    fn server_error_event_returns_interrupted() {
        let mut sync = new_synchronizer();
        feed(&mut sync, "server-intent", intent("xfer-full"));
        let out =
            feed(&mut sync, "error", json!({"id": "p", "reason": "bad"})).expect("interrupted");
        let FDv2SourceResult::Interrupted(err) = out.result else {
            panic!("expected Interrupted");
        };
        assert!(err.message.contains("bad"));
    }

    #[test]
    fn unknown_intent_code_returns_interrupted() {
        let mut sync = new_synchronizer();
        let out = feed(&mut sync, "server-intent", intent("brand-new")).expect("interrupted");
        let FDv2SourceResult::Interrupted(err) = out.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::InvalidData);
    }

    #[test]
    fn translation_failure_returns_interrupted() {
        let mut sync = new_synchronizer();
        feed(&mut sync, "server-intent", intent("xfer-full"));
        feed(
            &mut sync,
            "put-object",
            json!({"version": 1, "kind": "flag", "key": "bad", "object": {"garbage": true}}),
        );
        let out =
            feed(&mut sync, "payload-transferred", json!({"state": "s-1"})).expect("interrupted");
        let FDv2SourceResult::Interrupted(err) = out.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::InvalidData);
        assert!(err.message.contains("translated"));
    }

    #[test]
    fn eof_returns_none() {
        let mut sync = new_synchronizer();
        assert!(sync.handle_sse_error(eventsource::Error::Eof).is_none());
    }

    #[test]
    fn recoverable_status_returns_interrupted() {
        let mut sync = new_synchronizer();
        let out = sync.handle_unexpected_response(&eventsource_response(500, &[]));
        let FDv2SourceResult::Interrupted(err) = out.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::ErrorResponse { status_code: 500 });
    }

    #[test]
    fn unrecoverable_status_returns_terminal_error_with_fallback_header() {
        let mut sync = new_synchronizer();
        let response = eventsource_response(401, &[("X-LD-FD-Fallback", "true")]);
        let out = sync.handle_unexpected_response(&response);
        let FDv2SourceResult::TerminalError(err) = out.result else {
            panic!("expected TerminalError");
        };
        assert_eq!(err.kind, ErrorKind::ErrorResponse { status_code: 401 });
        assert!(out.fdv1_fallback.is_some());
    }

    #[test]
    fn unrecoverable_status_without_header_uses_latest_fallback() {
        let mut sync = new_synchronizer();
        sync.latest_fdv1_fallback = Some(FDv1FallbackDirective {
            ttl: Duration::from_secs(42),
        });
        let out = sync.handle_unexpected_response(&eventsource_response(401, &[]));
        assert_eq!(
            out.fdv1_fallback.map(|d| d.ttl),
            Some(Duration::from_secs(42))
        );
    }

    #[test]
    fn max_redirect_limit_returns_terminal_error() {
        let mut sync = new_synchronizer();
        let out = sync
            .handle_sse_error(eventsource::Error::MaxRedirectLimitReached(3))
            .expect("terminal");
        let FDv2SourceResult::TerminalError(err) = out.result else {
            panic!("expected TerminalError");
        };
        assert_eq!(err.kind, ErrorKind::Unknown);
    }

    #[test]
    fn invalid_line_returns_interrupted() {
        let mut sync = new_synchronizer();
        let out = sync
            .handle_sse_error(eventsource::Error::InvalidLine("bad".into()))
            .expect("interrupted");
        let FDv2SourceResult::Interrupted(err) = out.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::NetworkError);
    }

    fn sse_full_payload(flag_key: &str) -> String {
        let flag = crate::test_common::basic_flag(flag_key);
        sse_full_payload_with_object(flag_key, &serde_json::to_value(&flag).unwrap())
    }

    fn sse_full_payload_with_object(flag_key: &str, flag_json: &serde_json::Value) -> String {
        format!(
            "event: server-intent\n\
             data: {{\"payloads\":[{{\"id\":\"p\",\"target\":1,\"intentCode\":\"xfer-full\",\"reason\":\"payload-missing\"}}]}}\n\
             \n\
             event: put-object\n\
             data: {{\"version\":1,\"kind\":\"flag\",\"key\":\"{flag_key}\",\"object\":{flag_json}}}\n\
             \n\
             event: payload-transferred\n\
             data: {{\"state\":\"s-1\"}}\n\
             \n"
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_to_end_full_payload_emits_changeset() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/sdk/stream")
            .match_header("Authorization", "sdk-key")
            .with_status(200)
            .with_header("content-type", "text/event-stream")
            .with_body(sse_full_payload("f"))
            .expect_at_least(1)
            .create_async()
            .await;

        let transport = launchdarkly_sdk_transport::HyperTransport::new().expect("hyper transport");
        let mut sync = StreamingSynchronizer::new(
            transport,
            server.url(),
            test_headers(),
            Duration::from_millis(10),
        );

        let out = sync.next(None).await;
        let FDv2SourceResult::ChangeSet(cs) = out.result else {
            panic!("expected ChangeSet, got {:?}", out.result);
        };
        assert_eq!(cs.selector.as_deref(), Some("s-1"));
        assert_eq!(cs.changes.len(), 1);
        mock.assert_async().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_request_includes_selector_in_query() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                mockito::Matcher::Regex(r"/sdk/stream\?basis=state-1".into()),
            )
            .with_status(200)
            .with_header("content-type", "text/event-stream")
            .with_body(sse_full_payload("f"))
            .expect_at_least(1)
            .create_async()
            .await;

        let transport = launchdarkly_sdk_transport::HyperTransport::new().expect("hyper transport");
        let mut sync = StreamingSynchronizer::new(
            transport,
            server.url(),
            test_headers(),
            Duration::from_millis(10),
        );

        let _ = sync.next(Some("state-1".into())).await;
        mock.assert_async().await;
    }

    /// Serves one scripted response body for each stream request, and records the time of
    /// each request. A request after the script runs out gets a body that never ends.
    #[derive(Clone)]
    struct ScriptedTransport {
        bodies: Arc<Mutex<VecDeque<ByteStream>>>,
        request_times: Arc<Mutex<Vec<Instant>>>,
    }

    impl ScriptedTransport {
        fn new(bodies: Vec<ByteStream>) -> Self {
            Self {
                bodies: Arc::new(Mutex::new(bodies.into())),
                request_times: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn request_times(&self) -> Vec<Instant> {
            self.request_times.lock().unwrap().clone()
        }
    }

    impl HttpTransport for ScriptedTransport {
        fn request(&self, _: Request<Option<Bytes>>) -> ResponseFuture {
            self.request_times.lock().unwrap().push(Instant::now());
            let body = self
                .bodies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Box::pin(futures::stream::pending()));
            Box::pin(async move {
                Ok(http::Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(body)
                    .unwrap())
            })
        }
    }

    /// A response body that sends `sse` and then stays open.
    fn open_body(sse: String) -> ByteStream {
        Box::pin(futures::stream::iter([Ok(Bytes::from(sse))]).chain(futures::stream::pending()))
    }

    /// A full payload whose only flag has a property of the wrong type.
    fn sse_payload_with_wrong_type_flag() -> String {
        let mut flag_json = serde_json::to_value(crate::test_common::basic_flag("bad")).unwrap();
        flag_json["on"] = json!("not-a-boolean");
        sse_full_payload_with_object("bad", &flag_json)
    }

    fn assert_invalid_data(out: FDv2SourceEvent) {
        let FDv2SourceResult::Interrupted(err) = out.result else {
            panic!("expected Interrupted, got {:?}", out.result);
        };
        assert_eq!(err.kind, ErrorKind::InvalidData);
    }

    fn scripted_synchronizer(
        transport: &ScriptedTransport,
    ) -> StreamingSynchronizer<ScriptedTransport> {
        StreamingSynchronizer::new(
            transport.clone(),
            "http://example.com".into(),
            test_headers(),
            Duration::from_secs(1),
        )
    }

    /// Asserts that `wait` is `full` less a jitter of up to half of `full`.
    fn assert_jittered(wait: Duration, full: Duration) {
        assert!(
            wait >= full / 2 && wait <= full,
            "expected a wait between {:?} and {full:?}, got {wait:?}",
            full / 2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_payload_restarts_stream_after_growing_backoff() {
        let transport = ScriptedTransport::new(
            (0..8)
                .map(|_| open_body(sse_payload_with_wrong_type_flag()))
                .collect(),
        );
        let mut sync = scripted_synchronizer(&transport);

        // Receive the invalid payload on eight connections in turn.
        for _ in 0..8 {
            assert_invalid_data(sync.next(None).await);
        }

        // Each restart waits for a delay that starts at the initial delay and doubles, up to
        // 30 seconds.
        let times = transport.request_times();
        assert_eq!(times.len(), 8);
        for (pair, full_secs) in times.windows(2).zip([1, 2, 4, 8, 16, 30, 30]) {
            assert_jittered(pair[1] - pair[0], Duration::from_secs(full_secs));
        }
    }

    /// Makes three connections that receive an invalid payload, then a fourth connection that
    /// receives a valid payload and, `healthy_for` later, invalid data. Returns the wait before
    /// the fifth connection.
    async fn restart_wait_after_healthy_connection(healthy_for: Duration) -> Duration {
        let (healthy_sender, healthy_body) = futures::channel::mpsc::unbounded();
        let mut bodies: Vec<ByteStream> = (0..3)
            .map(|_| open_body(sse_payload_with_wrong_type_flag()))
            .collect();
        bodies.push(Box::pin(healthy_body));
        bodies.push(open_body(sse_payload_with_wrong_type_flag()));
        let transport = ScriptedTransport::new(bodies);
        let mut sync = scripted_synchronizer(&transport);

        for _ in 0..3 {
            assert_invalid_data(sync.next(None).await);
        }
        healthy_sender
            .unbounded_send(Ok(Bytes::from(sse_full_payload("good"))))
            .unwrap();
        let out = sync.next(None).await;
        assert!(matches!(out.result, FDv2SourceResult::ChangeSet(_)));
        tokio::time::advance(healthy_for).await;
        healthy_sender
            .unbounded_send(Ok(Bytes::from("event: put-object\ndata: not json\n\n")))
            .unwrap();
        assert_invalid_data(sync.next(None).await);
        let invalid_data_time = Instant::now();
        assert_invalid_data(sync.next(None).await);

        let times = transport.request_times();
        assert_eq!(times.len(), 5);
        times[4] - invalid_data_time
    }

    #[tokio::test(start_paused = true)]
    async fn stream_healthy_for_60_seconds_resets_restart_backoff() {
        // Restart after invalid data that follows 60 seconds of healthy operation.
        let wait = restart_wait_after_healthy_connection(Duration::from_secs(60)).await;

        // The restart waits for the initial delay again.
        assert_jittered(wait, Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn stream_healthy_for_less_than_60_seconds_keeps_restart_backoff() {
        // Restart after invalid data that follows 59 seconds of healthy operation.
        let wait = restart_wait_after_healthy_connection(Duration::from_secs(59)).await;

        // The delay keeps growing from the three earlier restarts.
        assert_jittered(wait, Duration::from_secs(8));
    }
}
