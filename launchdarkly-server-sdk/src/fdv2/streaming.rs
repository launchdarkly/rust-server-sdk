use std::pin::Pin;
use std::time::Duration;

use eventsource_client::{
    self as eventsource, Client, ClientBuilder, ReconnectOptionsBuilder, SSE,
};
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use http::Uri;
use launchdarkly_sdk_transport::HttpTransport;
use tokio::sync::watch;

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
        }
    }

    fn build_stream_uri(&self, selector: &Selector) -> Result<Uri, http::uri::InvalidUri> {
        build_fdv2_url(&self.base_url, STREAM_ENDPOINT, selector).parse::<Uri>()
    }

    fn drop_connection(&mut self) {
        self.stream = None;
        self.handler.reset();
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
                self.drop_connection();
                return Some(interrupted(ErrorKind::InvalidData, msg, fallback));
            }
        };

        match self.handler.handle_event(&event.event_type, data) {
            ProtocolResult::None => None,
            ProtocolResult::ChangeSet(wire_cs) => match ChangeSet::try_from(wire_cs) {
                Ok(cs) => Some(FDv2SourceEvent {
                    result: FDv2SourceResult::ChangeSet(cs),
                    fdv1_fallback: self.latest_fdv1_fallback.clone(),
                }),
                Err(e) => {
                    let msg = format!("FDv2 streaming changeset could not be translated: {e}");
                    let fallback = self.latest_fdv1_fallback.clone();
                    self.drop_connection();
                    Some(interrupted(ErrorKind::InvalidData, msg, fallback))
                }
            },
            ProtocolResult::Goodbye(g) => {
                // An explicit directive on the goodbye takes precedence over
                // the most recent response header.
                let fallback = g
                    .protocol_fallback_ttl
                    .map(|s| FDv1FallbackDirective {
                        ttl: Duration::from_secs(s),
                    })
                    .or_else(|| self.latest_fdv1_fallback.clone());
                self.drop_connection();
                Some(FDv2SourceEvent {
                    result: FDv2SourceResult::Goodbye { reason: g.reason },
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
                self.drop_connection();
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Request, StatusCode};
    use launchdarkly_sdk_transport::{ResponseFuture, TransportError};
    use serde_json::json;

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

        let FDv2SourceResult::Goodbye { reason } = out.result else {
            panic!("expected Goodbye");
        };
        assert_eq!(reason.as_deref(), Some("rotating"));
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
        let flag_json = serde_json::to_string(&flag).unwrap();
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
}
