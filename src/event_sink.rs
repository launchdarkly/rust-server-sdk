use chrono::DateTime;

use r::header::HeaderValue;
use reqwest as r;

use crate::events::OutputEvent;

type Error = String; // TODO(ch108607) use an error enum

pub trait EventSink: Send + Sync {
    fn send(&mut self, events: Vec<OutputEvent>) -> Result<(), Error>;
    fn last_known_time(&self) -> u128;
}

pub struct ReqwestSink {
    url: r::Url,
    sdk_key: String,
    http: r::Client,
    last_known_time: u128,
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
            last_known_time: 0,
        })
    }
}

impl EventSink for ReqwestSink {
    fn send(&mut self, events: Vec<OutputEvent>) -> Result<(), Error> {
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
            .header("User-Agent", &*crate::USER_AGENT)
            .body(json);

        let resp = request
            .send()
            .map(|resp| resp.error_for_status())
            .map_err(|e| format!("error sending event: {}", e))?;

        debug!("sent event: {:?}", resp);

        if let Ok(response) = resp {
            let date_value = response
                .headers()
                .get(r::header::DATE)
                .map_or(HeaderValue::from_static(""), |v| v.to_owned())
                .to_str()
                .unwrap_or("")
                .to_owned();

            if let Ok(date) = DateTime::parse_from_rfc2822(&date_value) {
                self.last_known_time = date.timestamp_millis() as u128;
            }
        }

        Ok(())
    }

    fn last_known_time(&self) -> u128 {
        self.last_known_time
    }
}

#[cfg(test)]
pub struct MockSink {
    pub events: Vec<OutputEvent>,
    last_known_time: u128,
}
#[cfg(test)]
impl MockSink {
    pub fn new(last_known_time: u128) -> Self {
        Self {
            events: Vec::new(),
            last_known_time,
        }
    }
}

#[cfg(test)]
impl EventSink for MockSink {
    fn send(&mut self, events: Vec<OutputEvent>) -> Result<(), Error> {
        self.events.extend(events);
        Ok(())
    }

    fn last_known_time(&self) -> u128 {
        self.last_known_time
    }
}
