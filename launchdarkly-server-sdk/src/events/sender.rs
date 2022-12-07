use crate::{
    reqwest::is_http_error_recoverable, LAUNCHDARKLY_EVENT_SCHEMA_HEADER,
    LAUNCHDARKLY_PAYLOAD_ID_HEADER,
};
use std::collections::HashMap;

use chrono::DateTime;
use crossbeam_channel::Sender;
use futures::future::BoxFuture;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use super::event::OutputEvent;

pub struct EventSenderResult {
    pub(super) time_from_server: u128,
    pub(super) success: bool,
    pub(super) must_shutdown: bool,
}

pub trait EventSender: Send + Sync {
    fn send_event_data(
        &self,
        events: Vec<OutputEvent>,
        result_tx: Sender<EventSenderResult>,
    ) -> BoxFuture<()>;
}

#[derive(Clone)]
pub struct HyperEventSender<C> {
    url: hyper::Uri,
    sdk_key: String,
    http: hyper::Client<C>,
    default_headers: HashMap<&'static str, String>,
}

impl<C> HyperEventSender<C> {
    pub fn new(
        http: hyper::Client<C>,
        url: hyper::Uri,
        sdk_key: &str,
        default_headers: HashMap<&'static str, String>,
    ) -> Self {
        Self {
            url,
            sdk_key: sdk_key.to_owned(),
            http,
            default_headers,
        }
    }

    fn get_server_time_from_response<Body>(&self, response: &hyper::Response<Body>) -> u128 {
        let date_value = response
            .headers()
            .get("date")
            .unwrap_or(&crate::EMPTY_HEADER)
            .to_str()
            .unwrap_or("")
            .to_owned();

        match DateTime::parse_from_rfc2822(&date_value) {
            Ok(date) => date.timestamp_millis() as u128,
            Err(_) => 0,
        }
    }
}

impl<C> EventSender for HyperEventSender<C>
where
    C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
{
    fn send_event_data(
        &self,
        events: Vec<OutputEvent>,
        result_tx: Sender<EventSenderResult>,
    ) -> BoxFuture<()> {
        Box::pin(async move {
            let uuid = Uuid::new_v4();

            debug!(
                "Sending ({}): {}",
                uuid,
                serde_json::to_string_pretty(&events).unwrap_or_else(|e| e.to_string())
            );

            let json = match serde_json::to_vec(&events) {
                Ok(json) => json,
                Err(e) => {
                    error!(
                        "Failed to serialize event payload. Some events were dropped: {:?}",
                        e
                    );
                    return;
                }
            };

            for attempt in 0..2 {
                if attempt == 1 {
                    sleep(Duration::from_secs(1)).await;
                }

                let mut request_builder = hyper::Request::builder()
                    .uri(self.url.clone())
                    .method("POST")
                    .header("Content-Type", "application/json")
                    .header("Authorization", self.sdk_key.clone())
                    .header("User-Agent", &*crate::USER_AGENT)
                    .header(
                        LAUNCHDARKLY_EVENT_SCHEMA_HEADER,
                        crate::CURRENT_EVENT_SCHEMA,
                    )
                    .header(LAUNCHDARKLY_PAYLOAD_ID_HEADER, uuid.to_string());

                for default_header in &self.default_headers {
                    request_builder =
                        request_builder.header(*default_header.0, default_header.1.as_str());
                }
                let request = request_builder.body(hyper::Body::from(json.clone()));

                let result = self.http.request(request.unwrap()).await;

                let response = match result {
                    Ok(response) => response,
                    Err(e) => {
                        // It appears this type of error will not be an HTTP error.
                        // It will be a closed connection, aborted write, timeout, etc.
                        error!("Failed to send events. Some events were dropped: {:?}", e);
                        result_tx
                            .send(EventSenderResult {
                                success: false,
                                time_from_server: 0,
                                must_shutdown: false,
                            })
                            .unwrap();
                        return;
                    }
                };

                if response.status().is_success() {
                    let _ = result_tx.send(EventSenderResult {
                        success: true,
                        time_from_server: self.get_server_time_from_response(&response),
                        must_shutdown: false,
                    });
                    return;
                }

                if !is_http_error_recoverable(response.status().as_u16()) {
                    result_tx
                        .send(EventSenderResult {
                            success: false,
                            time_from_server: 0,
                            must_shutdown: true,
                        })
                        .unwrap();
                    return;
                }
            }

            result_tx
                .send(EventSenderResult {
                    success: false,
                    time_from_server: 0,
                    must_shutdown: false,
                })
                .unwrap();
        })
    }
}

#[cfg(test)]
pub(crate) struct InMemoryEventSender {
    event_tx: Sender<OutputEvent>,
}

#[cfg(test)]
impl InMemoryEventSender {
    pub(crate) fn new(event_tx: Sender<OutputEvent>) -> Self {
        Self { event_tx }
    }
}

#[cfg(test)]
impl EventSender for InMemoryEventSender {
    fn send_event_data(
        &self,
        events: Vec<OutputEvent>,
        sender: Sender<EventSenderResult>,
    ) -> BoxFuture<()> {
        Box::pin(async move {
            for event in events {
                self.event_tx.send(event).unwrap();
            }

            sender
                .send(EventSenderResult {
                    time_from_server: 0,
                    success: true,
                    must_shutdown: true,
                })
                .unwrap();
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;
    use mockito::mock;
    use std::str::FromStr;
    use test_case::test_case;

    #[test_case(hyper::StatusCode::CONTINUE, true)]
    #[test_case(hyper::StatusCode::OK, true)]
    #[test_case(hyper::StatusCode::MULTIPLE_CHOICES, true)]
    #[test_case(hyper::StatusCode::BAD_REQUEST, true)]
    #[test_case(hyper::StatusCode::UNAUTHORIZED, false)]
    #[test_case(hyper::StatusCode::REQUEST_TIMEOUT, true)]
    #[test_case(hyper::StatusCode::CONFLICT, false)]
    #[test_case(hyper::StatusCode::TOO_MANY_REQUESTS, true)]
    #[test_case(hyper::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE, false)]
    #[test_case(hyper::StatusCode::INTERNAL_SERVER_ERROR, true)]
    fn can_determine_recoverable_errors(status: hyper::StatusCode, is_recoverable: bool) {
        assert_eq!(is_recoverable, is_http_error_recoverable(status.as_u16()));
    }

    #[tokio::test]
    async fn can_parse_server_time_from_response() {
        let _mock = mock("POST", "/bulk")
            .with_status(200)
            .with_header("date", "Fri, 13 Feb 2009 23:31:30 GMT")
            .create();

        let (tx, rx) = bounded::<EventSenderResult>(5);
        let event_sender = build_event_sender();

        event_sender.send_event_data(vec![], tx).await;

        let sender_result = rx.recv().unwrap();
        assert!(sender_result.success);
        assert!(!sender_result.must_shutdown);
        assert_eq!(sender_result.time_from_server, 1234567890000);
    }

    #[tokio::test]
    async fn unrecoverable_failure_requires_shutdown() {
        let _mock = mock("POST", "/bulk").with_status(401).create();

        let (tx, rx) = bounded::<EventSenderResult>(5);
        let event_sender = build_event_sender();

        event_sender.send_event_data(vec![], tx).await;

        let sender_result = rx.recv().expect("Failed to receive sender_result");
        assert!(!sender_result.success);
        assert!(sender_result.must_shutdown);
    }

    #[tokio::test]
    async fn recoverable_failures_are_attempted_multiple_times() {
        let mock = mock("POST", "/bulk").with_status(400).expect(2).create();

        let (tx, rx) = bounded::<EventSenderResult>(5);
        let event_sender = build_event_sender();

        event_sender.send_event_data(vec![], tx).await;

        let sender_result = rx.recv().expect("Failed to receive sender_result");
        assert!(!sender_result.success);
        assert!(!sender_result.must_shutdown);
        mock.assert();
    }

    #[tokio::test]
    async fn retrying_requests_can_eventually_succeed() {
        let _failed = mock("POST", "/bulk").with_status(400).create();
        let _succeed = mock("POST", "/bulk")
            .with_status(200)
            .with_header("date", "Fri, 13 Feb 2009 23:31:30 GMT")
            .create();

        let (tx, rx) = bounded::<EventSenderResult>(5);
        let event_sender = build_event_sender();

        event_sender.send_event_data(vec![], tx).await;

        let sender_result = rx.recv().expect("Failed to receive sender_result");
        assert!(sender_result.success);
        assert!(!sender_result.must_shutdown);
        assert_eq!(sender_result.time_from_server, 1234567890000);
    }

    fn build_event_sender() -> HyperEventSender<hyper::client::HttpConnector> {
        let http = hyper::Client::builder().build(hyper::client::HttpConnector::new());
        let url = format!("{}/bulk", &mockito::server_url());
        let url = hyper::Uri::from_str(&url).expect("Failed parsing the mock server url");

        HyperEventSender::new(http, url, "sdk-key", HashMap::<&str, String>::new())
    }
}
