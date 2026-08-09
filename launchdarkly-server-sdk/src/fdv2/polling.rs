use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use http::{Request, Response, StatusCode};
use launchdarkly_sdk_transport::{ByteStream, HttpTransport};
use serde::Deserialize;

use super::model::{ChangeSetKind, Selector};
use super::protocol::{FDv2ProtocolHandler, ProtocolError, ProtocolResult};
use super::source::{
    read_fallback_directive, ErrorInfo, ErrorKind, FDv1FallbackDirective, FDv2SourceEvent,
    FDv2SourceResult, RequestHeaders,
};
use super::url::build_fdv2_url;
use crate::reqwest::is_http_error_recoverable;
use crate::stores::change_set::ChangeSet;

// Minimum polling interval to avoid accidentally hammering the service.
const MINIMUM_POLL_INTERVAL: Duration = Duration::from_secs(30);

fn interrupted(kind: ErrorKind, message: impl Into<String>) -> FDv2SourceEvent {
    FDv2SourceEvent {
        result: FDv2SourceResult::Interrupted(ErrorInfo {
            kind,
            message: message.into(),
        }),
        fdv1_fallback: None,
    }
}

#[derive(Deserialize)]
struct PollEnvelope {
    events: Vec<PollEvent>,
}

#[derive(Deserialize)]
struct PollEvent {
    event: String,
    data: serde_json::Value,
}

fn parse_poll_body(body: &[u8]) -> FDv2SourceEvent {
    let envelope: PollEnvelope = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => {
            return interrupted(
                ErrorKind::InvalidData,
                "could not parse FDv2 polling response",
            );
        }
    };

    let mut handler = FDv2ProtocolHandler::new();
    for event in envelope.events {
        match handler.handle_event(&event.event, event.data) {
            ProtocolResult::None => continue,
            ProtocolResult::ChangeSet(wire_cs) => match ChangeSet::try_from(wire_cs) {
                Ok(cs) => {
                    return FDv2SourceEvent {
                        result: FDv2SourceResult::ChangeSet(cs),
                        fdv1_fallback: None,
                    };
                }
                Err(_) => {
                    return interrupted(
                        ErrorKind::InvalidData,
                        "FDv2 polling response could not be translated",
                    );
                }
            },
            ProtocolResult::Goodbye(g) => {
                return FDv2SourceEvent {
                    result: FDv2SourceResult::Goodbye { reason: g.reason },
                    fdv1_fallback: g.protocol_fallback_ttl.map(|s| FDv1FallbackDirective {
                        ttl: Duration::from_secs(s),
                    }),
                };
            }
            ProtocolResult::Error(ProtocolError::Server(err)) => {
                let msg = format!(
                    "An issue was encountered receiving updates for payload '{}' with reason: '{}'. Automatic retry will occur.",
                    err.id.as_deref().unwrap_or(""),
                    err.reason,
                );
                return interrupted(ErrorKind::ErrorResponse { status_code: 0 }, msg);
            }
            ProtocolResult::Error(ProtocolError::Protocol(msg))
            | ProtocolResult::Error(ProtocolError::JsonParse(msg)) => {
                return interrupted(ErrorKind::InvalidData, msg);
            }
        }
    }
    interrupted(
        ErrorKind::InvalidData,
        "FDv2 polling response did not contain a complete payload",
    )
}

async fn handle_response(response: Response<ByteStream>) -> FDv2SourceEvent {
    let fallback = read_fallback_directive(response.headers());
    let status = response.status();

    if status == StatusCode::NOT_MODIFIED {
        return FDv2SourceEvent {
            result: FDv2SourceResult::ChangeSet(ChangeSet {
                kind: ChangeSetKind::None,
                changes: Vec::new(),
                selector: None,
            }),
            fdv1_fallback: fallback,
        };
    }

    if status.is_success() {
        let mut body_bytes = Vec::new();
        let mut stream = response.into_body();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(b) => body_bytes.extend_from_slice(&b),
                Err(e) => {
                    return FDv2SourceEvent {
                        result: FDv2SourceResult::Interrupted(ErrorInfo {
                            kind: ErrorKind::NetworkError,
                            message: format!("could not read polling response body: {e}"),
                        }),
                        fdv1_fallback: fallback,
                    };
                }
            }
        }
        if body_bytes.is_empty() {
            return FDv2SourceEvent {
                result: FDv2SourceResult::Interrupted(ErrorInfo {
                    kind: ErrorKind::InvalidData,
                    message: "polling response contained no body".into(),
                }),
                fdv1_fallback: fallback,
            };
        }
        let mut event = parse_poll_body(&body_bytes);
        if event.fdv1_fallback.is_none() {
            event.fdv1_fallback = fallback;
        }
        return event;
    }

    let status_u16 = status.as_u16();
    let message = format!("FDv2 polling request received HTTP status {status_u16}");
    let error_info = ErrorInfo {
        kind: ErrorKind::ErrorResponse {
            status_code: status_u16,
        },
        message,
    };
    if is_http_error_recoverable(status_u16) {
        FDv2SourceEvent {
            result: FDv2SourceResult::Interrupted(error_info),
            fdv1_fallback: fallback,
        }
    } else {
        FDv2SourceEvent {
            result: FDv2SourceResult::TerminalError(error_info),
            fdv1_fallback: fallback,
        }
    }
}

async fn fetch_and_handle<T: HttpTransport>(
    transport: &T,
    request: Request<Option<Bytes>>,
) -> FDv2SourceEvent {
    match transport.request(request).await {
        Ok(response) => handle_response(response).await,
        Err(e) => interrupted(ErrorKind::NetworkError, format!("{e}")),
    }
}

pub(crate) struct PollingInitializer<T: HttpTransport> {
    transport: T,
    base_url: String,
    headers: RequestHeaders,
    selector: Selector,
}

impl<T: HttpTransport> PollingInitializer<T> {
    pub(crate) fn new(
        transport: T,
        base_url: String,
        headers: RequestHeaders,
        selector: Selector,
    ) -> Self {
        Self {
            transport,
            base_url,
            headers,
            selector,
        }
    }
}

impl<T: HttpTransport> super::source::Initializer for PollingInitializer<T> {
    fn run(&mut self) -> futures::future::BoxFuture<'_, FDv2SourceEvent> {
        Box::pin(async move {
            let request = match build_poll_request(&self.base_url, &self.headers, &self.selector) {
                Ok(r) => r,
                Err(e) => {
                    return FDv2SourceEvent {
                        result: FDv2SourceResult::TerminalError(ErrorInfo {
                            kind: ErrorKind::Unknown,
                            message: format!("could not build polling request: {e}"),
                        }),
                        fdv1_fallback: None,
                    };
                }
            };
            fetch_and_handle(&self.transport, request).await
        })
    }

    fn name(&self) -> &str {
        "FDv2 polling initializer"
    }
}

pub(crate) struct PollingSynchronizer<T: HttpTransport> {
    transport: T,
    base_url: String,
    headers: RequestHeaders,
    poll_interval: Duration,
    last_poll_start: Option<std::time::Instant>,
}

impl<T: HttpTransport> PollingSynchronizer<T> {
    pub(crate) fn new(
        transport: T,
        base_url: String,
        headers: RequestHeaders,
        poll_interval: Duration,
    ) -> Self {
        Self {
            transport,
            base_url,
            headers,
            poll_interval: std::cmp::max(poll_interval, MINIMUM_POLL_INTERVAL),
            last_poll_start: None,
        }
    }
}

impl<T: HttpTransport> super::source::Synchronizer for PollingSynchronizer<T> {
    fn next(&mut self, selector: Selector) -> futures::future::BoxFuture<'_, FDv2SourceEvent> {
        Box::pin(async move {
            // Wait for the poll interval to elapse since the previous request.
            let wait = match self.last_poll_start {
                Some(t) => self.poll_interval.saturating_sub(t.elapsed()),
                None => Duration::ZERO,
            };
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            self.last_poll_start = Some(std::time::Instant::now());

            // Build the poll request.
            let request = match build_poll_request(&self.base_url, &self.headers, &selector) {
                Ok(r) => r,
                Err(e) => {
                    return FDv2SourceEvent {
                        result: FDv2SourceResult::TerminalError(ErrorInfo {
                            kind: ErrorKind::Unknown,
                            message: format!("could not build polling request: {e}"),
                        }),
                        fdv1_fallback: None,
                    };
                }
            };

            // Fire it and interpret the response.
            fetch_and_handle(&self.transport, request).await
        })
    }

    fn name(&self) -> &str {
        "FDv2 polling synchronizer"
    }
}

fn build_poll_request(
    base_url: &str,
    headers: &RequestHeaders,
    selector: &Selector,
) -> Result<Request<Option<Bytes>>, http::Error> {
    let url = build_fdv2_url(base_url, "sdk/poll", selector);
    let mut builder = Request::builder()
        .uri(url)
        .method("GET")
        .header("Content-Type", "application/json");
    for (name, value) in headers.iter() {
        builder = builder.header(name, value);
    }
    builder.body(Some(Bytes::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_headers() -> RequestHeaders {
        RequestHeaders::new("sdk-key", None, "test-instance")
    }

    fn full_payload_body(flag_key: &str) -> Vec<u8> {
        let flag = crate::test_common::basic_flag(flag_key);
        let flag_json = serde_json::to_value(&flag).unwrap();
        let envelope = serde_json::json!({
            "events": [
                {"event": "server-intent", "data": {"payloads": [{
                    "id": "p1",
                    "target": 1,
                    "intentCode": "xfer-full",
                    "reason": "payload-missing",
                }]}},
                {"event": "put-object", "data": {
                    "version": 1,
                    "kind": "flag",
                    "key": flag_key,
                    "object": flag_json,
                }},
                {"event": "payload-transferred", "data": {"state": "s-1"}},
            ]
        });
        serde_json::to_vec(&envelope).unwrap()
    }

    #[test]
    fn parse_full_payload_returns_change_set() {
        let body = full_payload_body("my-flag");
        let event = parse_poll_body(&body);
        let FDv2SourceResult::ChangeSet(cs) = event.result else {
            panic!("expected ChangeSet, got {:?}", event.result);
        };
        assert_eq!(cs.changes.len(), 1);
        assert_eq!(cs.selector.as_deref(), Some("s-1"));
    }

    #[test]
    fn parse_malformed_json_returns_interrupted() {
        let event = parse_poll_body(b"not json");
        assert!(matches!(
            event.result,
            FDv2SourceResult::Interrupted(ErrorInfo {
                kind: ErrorKind::InvalidData,
                ..
            })
        ));
    }

    #[test]
    fn parse_incomplete_payload_returns_interrupted() {
        let envelope = serde_json::json!({
            "events": [
                {"event": "server-intent", "data": {"payloads": [{
                    "id": "p1",
                    "target": 1,
                    "intentCode": "xfer-full",
                    "reason": "payload-missing",
                }]}},
            ]
        });
        let body = serde_json::to_vec(&envelope).unwrap();
        let event = parse_poll_body(&body);
        let FDv2SourceResult::Interrupted(err) = event.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::InvalidData);
        assert!(err.message.contains("complete"), "message: {}", err.message);
    }

    #[test]
    fn parse_translator_failure_returns_interrupted() {
        let envelope = serde_json::json!({
            "events": [
                {"event": "server-intent", "data": {"payloads": [{
                    "id": "p1",
                    "target": 1,
                    "intentCode": "xfer-full",
                    "reason": "payload-missing",
                }]}},
                {"event": "put-object", "data": {
                    "version": 1,
                    "kind": "flag",
                    "key": "bad",
                    "object": {"garbage": true},
                }},
                {"event": "payload-transferred", "data": {"state": "s-1"}},
            ]
        });
        let body = serde_json::to_vec(&envelope).unwrap();
        let event = parse_poll_body(&body);
        let FDv2SourceResult::Interrupted(err) = event.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::InvalidData);
        assert!(
            err.message.contains("translated"),
            "message: {}",
            err.message
        );
    }

    fn make_response(status: u16, headers: &[(&str, &str)], body: &[u8]) -> Response<ByteStream> {
        let mut builder = Response::builder().status(status);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        let body = body.to_vec();
        let stream = futures::stream::iter(vec![Ok(Bytes::from(body))]);
        builder.body(Box::pin(stream) as ByteStream).unwrap()
    }

    #[tokio::test]
    async fn handle_304_with_header_returns_none_changeset_with_fallback() {
        let resp = make_response(304, &[("X-LD-FD-Fallback", "true")], &[]);
        let event = handle_response(resp).await;
        let FDv2SourceResult::ChangeSet(cs) = event.result else {
            panic!("expected ChangeSet");
        };
        assert_eq!(cs.kind, ChangeSetKind::None);
        assert!(event.fdv1_fallback.is_some());
    }

    #[tokio::test]
    async fn handle_recoverable_status_returns_interrupted_with_fallback() {
        let resp = make_response(400, &[("X-LD-FD-Fallback", "true")], &[]);
        let event = handle_response(resp).await;
        let FDv2SourceResult::Interrupted(err) = event.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::ErrorResponse { status_code: 400 });
        assert!(event.fdv1_fallback.is_some());
    }

    #[tokio::test]
    async fn handle_terminal_status_returns_terminal_error_with_fallback() {
        let resp = make_response(401, &[("X-LD-FD-Fallback", "true")], &[]);
        let event = handle_response(resp).await;
        let FDv2SourceResult::TerminalError(err) = event.result else {
            panic!("expected TerminalError");
        };
        assert_eq!(err.kind, ErrorKind::ErrorResponse { status_code: 401 });
        assert!(event.fdv1_fallback.is_some());
    }

    #[tokio::test]
    async fn handle_200_with_valid_body_returns_change_set() {
        let body = full_payload_body("my-flag");
        let resp = make_response(200, &[("X-LD-FD-Fallback", "true")], &body);
        let event = handle_response(resp).await;
        assert!(matches!(event.result, FDv2SourceResult::ChangeSet(_)));
        assert!(event.fdv1_fallback.is_some());
    }

    #[tokio::test]
    async fn handle_200_with_empty_body_returns_interrupted() {
        let resp = make_response(200, &[], &[]);
        let event = handle_response(resp).await;
        let FDv2SourceResult::Interrupted(err) = event.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn in_body_fallback_takes_precedence_over_header() {
        // Envelope with a goodbye event carrying protocolFallbackTTL=90, plus a
        // header saying fallback with default (3600) TTL. Body wins.
        let envelope = serde_json::json!({
            "events": [
                {"event": "goodbye", "data": {"protocolFallbackTTL": 90}},
            ]
        });
        let body = serde_json::to_vec(&envelope).unwrap();
        let resp = make_response(200, &[("X-LD-FD-Fallback", "true")], &body);
        let event = handle_response(resp).await;
        assert_eq!(
            event.fdv1_fallback.as_ref().map(|d| d.ttl),
            Some(Duration::from_secs(90))
        );
    }

    #[derive(Clone)]
    struct FailingTransport;
    impl HttpTransport for FailingTransport {
        fn request(
            &self,
            _request: Request<Option<Bytes>>,
        ) -> launchdarkly_sdk_transport::ResponseFuture {
            Box::pin(async {
                Err(launchdarkly_sdk_transport::TransportError::new(
                    std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "boom"),
                ))
            })
        }
    }

    #[derive(Clone)]
    struct CapturingTransport {
        captured_uri: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    }

    impl HttpTransport for CapturingTransport {
        fn request(
            &self,
            request: Request<Option<Bytes>>,
        ) -> launchdarkly_sdk_transport::ResponseFuture {
            *self.captured_uri.lock().unwrap() = Some(request.uri().to_string());
            Box::pin(async { Ok(make_response(200, &[], b"{\"events\":[]}")) })
        }
    }

    #[tokio::test]
    async fn initializer_run_passes_stored_selector_in_request_url() {
        use super::super::source::Initializer;
        let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
        let transport = CapturingTransport {
            captured_uri: captured.clone(),
        };
        let mut initializer = PollingInitializer::new(
            transport,
            "http://example.com".to_string(),
            test_headers(),
            Some("stored-state".to_string()),
        );
        let _ = initializer.run().await;
        let uri = captured.lock().unwrap().clone().expect("uri captured");
        assert!(uri.contains("basis=stored-state"), "uri: {uri}");
    }

    #[tokio::test]
    async fn synchronizer_next_uses_argument_selector() {
        use super::super::source::Synchronizer;
        let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
        let transport = CapturingTransport {
            captured_uri: captured.clone(),
        };
        let mut sync = PollingSynchronizer::new(
            transport,
            "http://example.com".to_string(),
            test_headers(),
            Duration::ZERO,
        );
        let _ = sync.next(Some("per-call-state".to_string())).await;
        let uri = captured.lock().unwrap().clone().expect("uri captured");
        assert!(uri.contains("basis=per-call-state"), "uri: {uri}");
    }

    #[derive(Clone)]
    struct CountingTransport {
        count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl HttpTransport for CountingTransport {
        fn request(
            &self,
            _request: Request<Option<Bytes>>,
        ) -> launchdarkly_sdk_transport::ResponseFuture {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok(make_response(200, &[], b"{\"events\":[]}")) })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn synchronizer_next_enforces_poll_interval() {
        use super::super::source::Synchronizer;
        use std::sync::atomic::Ordering;

        // Configure an interval above the floor.
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut sync = PollingSynchronizer::new(
            CountingTransport {
                count: count.clone(),
            },
            "http://example.com".to_string(),
            test_headers(),
            Duration::from_secs(60),
        );

        // Poll twice, timing the gap on the paused clock.
        let start = tokio::time::Instant::now();
        let _ = sync.next(None).await;
        let _ = sync.next(None).await;
        let elapsed = start.elapsed();

        // The second poll waited the configured 60s.
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(
            elapsed >= Duration::from_secs(59),
            "expected the second poll to wait the configured 60s, got {elapsed:?}",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn synchronizer_clamps_poll_interval_to_minimum() {
        use super::super::source::Synchronizer;
        use std::sync::atomic::Ordering;

        // Configure an interval below the floor.
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut sync = PollingSynchronizer::new(
            CountingTransport {
                count: count.clone(),
            },
            "http://example.com".to_string(),
            test_headers(),
            Duration::from_secs(1),
        );

        // Poll twice, timing the gap on the paused clock.
        let start = tokio::time::Instant::now();
        let _ = sync.next(None).await;
        let _ = sync.next(None).await;
        let elapsed = start.elapsed();

        // The second poll waited the clamped 30s minimum, not the configured 1s.
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert!(
            elapsed >= Duration::from_secs(29),
            "expected the second poll to wait the clamped 30s minimum, got {elapsed:?}",
        );
    }

    #[tokio::test]
    async fn network_error_returns_interrupted_without_fallback() {
        let transport = FailingTransport;
        let req = build_poll_request("http://example.com", &test_headers(), &None).unwrap();
        let event = fetch_and_handle(&transport, req).await;
        let FDv2SourceResult::Interrupted(err) = event.result else {
            panic!("expected Interrupted");
        };
        assert_eq!(err.kind, ErrorKind::NetworkError);
        assert!(event.fdv1_fallback.is_none());
    }

    #[test]
    fn parse_goodbye_event_returns_goodbye_with_fallback() {
        let envelope = serde_json::json!({
            "events": [
                {"event": "goodbye", "data": {
                    "reason": "rotating",
                    "protocolFallbackTTL": 90,
                }},
            ]
        });
        let body = serde_json::to_vec(&envelope).unwrap();
        let event = parse_poll_body(&body);
        let FDv2SourceResult::Goodbye { reason } = event.result else {
            panic!("expected Goodbye");
        };
        assert_eq!(reason.as_deref(), Some("rotating"));
        assert_eq!(
            event.fdv1_fallback.as_ref().map(|d| d.ttl),
            Some(Duration::from_secs(90))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn end_to_end_full_payload_emits_changeset() {
        let mut server = mockito::Server::new_async().await;
        let flag = crate::test_common::basic_flag("f");
        let flag_json = serde_json::to_value(&flag).unwrap();
        let envelope = serde_json::json!({
            "events": [
                {"event": "server-intent", "data": {"payloads": [{
                    "id": "p", "target": 1, "intentCode": "xfer-full", "reason": "payload-missing",
                }]}},
                {"event": "put-object", "data": {
                    "version": 1, "kind": "flag", "key": "f", "object": flag_json,
                }},
                {"event": "payload-transferred", "data": {"state": "s-1"}},
            ]
        });
        let mock = server
            .mock("GET", "/sdk/poll")
            .match_header("Authorization", "sdk-key")
            .with_status(200)
            .with_body(envelope.to_string())
            .expect_at_least(1)
            .create_async()
            .await;

        let transport = launchdarkly_sdk_transport::HyperTransport::new().expect("hyper transport");
        use super::super::source::Synchronizer;
        let mut sync =
            PollingSynchronizer::new(transport, server.url(), test_headers(), Duration::ZERO);

        let out = sync.next(None).await;
        let FDv2SourceResult::ChangeSet(cs) = out.result else {
            panic!("expected ChangeSet, got {:?}", out.result);
        };
        assert_eq!(cs.selector.as_deref(), Some("s-1"));
        assert_eq!(cs.changes.len(), 1);
        mock.assert_async().await;
    }
}
