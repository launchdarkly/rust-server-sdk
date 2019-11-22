use super::events::Event;

use futures::Future;
use reqwest as r;
use reqwest::r#async as ra;
use tokio::executor::{DefaultExecutor, Executor};

type Error = String; // TODO

pub struct EventProcessor {
    http: ra::Client,
    base_url: r::Url,
    sdk_key: String,
}

impl EventProcessor {
    pub fn new(base_url: r::Url, sdk_key: &str) -> Self {
        EventProcessor {
            http: ra::Client::new(),
            base_url,
            sdk_key: sdk_key.to_string(),
        }
    }

    pub fn send(&self, event: Event) {
        debug!("Okay, sending event: {}", event);

        self._send(event).expect("failed to send event");
    }

    fn _send(&self, event: Event) -> Result<(), Error> {
        let mut url = self.base_url.clone();
        url.set_path("/bulk");
        let json = serde_json::to_vec(&vec![&event]).map_err(|e| e.to_string())?;
        debug!(
            "Sending: {}",
            serde_json::to_string_pretty(&vec![&event]).map_err(|e| e.to_string())?,
        );
        let request = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .header("Authorization", self.sdk_key.clone())
            .body(json);
        match DefaultExecutor::current().status() {
            // TODO queue events instead of dropping them
            Err(e) => info!("skipping event send because executor is not ready: {}", e),
            Ok(_) => {
                tokio::spawn(
                    request
                        .send()
                        .map_err(|e| error!("error sending event: {}", e))
                        .map(|resp| match resp.error_for_status() {
                            Ok(resp) => debug!("sent event: {:?}", resp),
                            Err(e) => warn!("error response sending event: {}", e),
                        }),
                );
            }
        }

        match event.make_index_event() {
            Some(index) => self._send(index),
            None => Ok(()),
        }
    }
}
