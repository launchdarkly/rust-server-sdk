use reqwest as r;

use super::events::Event;
use crate::built_info;

type Error = String; // TODO

pub trait EventSink: Send + Sync {
    fn send(&mut self, events: Vec<Event>) -> Result<(), Error>;
}

pub struct ReqwestSink {
    url: r::Url,
    sdk_key: String,
    http: r::Client,
}

impl ReqwestSink {
    pub fn new(base_url: &r::Url, sdk_key: &str) -> Result<Self, Error> {
        let mut url = base_url.clone();
        url.set_path("/bulk");

        let http = r::Client::builder().build().map_err(|e| e.to_string())?;

        Ok(ReqwestSink {
            http,
            url,
            sdk_key: sdk_key.to_owned(),
        })
    }
}

impl EventSink for ReqwestSink {
    fn send(&mut self, events: Vec<Event>) -> Result<(), Error> {
        debug!(
            "Sending: {}",
            serde_json::to_string_pretty(&events).unwrap_or_else(|e| e.to_string())
        );

        let json =
            serde_json::to_vec(&events).map_err(|e| format!("error serializing event: {}", e))?;

        let request = self
            .http
            .post(self.url.clone())
            .header("Content-Type", "application/json")
            .header("Authorization", self.sdk_key.clone())
            .header(
                "User-Agent",
                &("RustServerClient/".to_owned() + built_info::PKG_VERSION),
            )
            .body(json);

        let resp = request
            .send()
            .map(|resp| resp.error_for_status())
            .map_err(|e| format!("error sending event: {}", e))?;

        debug!("sent event: {:?}", resp);
        Ok(())
    }
}

#[cfg(test)]
pub type MockSink = Vec<Event>;

#[cfg(test)]
impl EventSink for MockSink {
    fn send(&mut self, events: Vec<Event>) -> Result<(), Error> {
        self.extend(events);
        Ok(())
    }
}
