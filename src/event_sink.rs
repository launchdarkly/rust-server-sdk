use super::events::Event;

use futures::Future;
use reqwest as r;
use reqwest::r#async as ra;
use tokio::executor::{DefaultExecutor, Executor};

type Error = String; // TODO

pub trait EventSink: Send + Sync {
    fn send(&mut self, event: &Event) -> Result<(), Error>;
}

/// Inefficient and hacky event sink that spawns a new task on the global tokio reactor for every
/// send.
/// TODO improve this.
pub struct OneShotTokioSink {
    pub http: ra::Client,
    pub base_url: r::Url,
    pub sdk_key: String,
}

impl EventSink for OneShotTokioSink {
    fn send(&mut self, event: &Event) -> Result<(), Error> {
        let batch = vec![event];

        debug!(
            "Sending: {}",
            serde_json::to_string_pretty(&batch).unwrap_or_else(|e| e.to_string())
        );

        let json = serde_json::to_vec(&batch).map_err(|e| e.to_string())?;

        let mut url = self.base_url.clone();
        url.set_path("/bulk");

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

        Ok(())
    }
}

#[cfg(test)]
pub type MockSink = Vec<Event>;

#[cfg(test)]
impl EventSink for MockSink {
    fn send(&mut self, event: &Event) -> Result<(), Error> {
        let event: Event = event.clone();
        self.push(event.clone());
        Ok(())
    }
}
