use std::time::Duration;

use serde::Deserialize;

use super::source::FDv1FallbackDirective;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum IntentCode {
    None,
    XferFull,
    XferChanges,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub(super) struct ServerIntent {
    pub(super) payloads: Vec<ServerIntentPayload>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServerIntentPayload {
    // Parsed from the wire but not used; only intent_code is read.
    #[allow(dead_code)]
    pub(super) id: String,
    #[allow(dead_code)]
    pub(super) target: u64,
    pub(super) intent_code: IntentCode,
    #[allow(dead_code)]
    pub(super) reason: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct PutObject {
    #[allow(dead_code)] // Parsed from the wire but not used.
    pub(super) version: u64,
    pub(super) kind: String,
    pub(super) key: String,
    pub(super) object: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteObject {
    pub(super) version: u64,
    pub(super) kind: String,
    pub(super) key: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct PayloadTransferred {
    pub(super) state: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct Goodbye {
    pub(super) reason: Option<String>,
    // Kept as raw JSON so a malformed value still signals fallback.
    #[serde(rename = "protocolFallbackTTL")]
    pub(super) protocol_fallback_ttl: Option<serde_json::Value>,
}

impl Goodbye {
    /// A present `protocolFallbackTTL` signals FDv1 fallback. A value that is not
    /// a non-negative integer number of seconds uses the default TTL.
    pub(super) fn fdv1_fallback(&self) -> Option<FDv1FallbackDirective> {
        self.protocol_fallback_ttl
            .as_ref()
            .map(|ttl| FDv1FallbackDirective::from_ttl(ttl.as_u64().map(Duration::from_secs)))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FDv2Error {
    pub(super) id: Option<String>,
    pub(super) reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn intent_code_parses_known_and_unknown() {
        assert_eq!(
            serde_json::from_value::<IntentCode>(json!("none")).unwrap(),
            IntentCode::None,
        );
        assert_eq!(
            serde_json::from_value::<IntentCode>(json!("xfer-full")).unwrap(),
            IntentCode::XferFull,
        );
        assert_eq!(
            serde_json::from_value::<IntentCode>(json!("xfer-changes")).unwrap(),
            IntentCode::XferChanges,
        );
        assert_eq!(
            serde_json::from_value::<IntentCode>(json!("brand-new-code")).unwrap(),
            IntentCode::Unknown,
        );
    }

    #[test]
    fn server_intent_parses() {
        let payload = json!({
            "payloads": [{
                "id": "abc",
                "target": 5,
                "intentCode": "xfer-full",
                "reason": "payload-missing"
            }]
        });
        let intent: ServerIntent = serde_json::from_value(payload).unwrap();
        assert_eq!(intent.payloads.len(), 1);
        let p = &intent.payloads[0];
        assert_eq!(p.id, "abc");
        assert_eq!(p.target, 5);
        assert_eq!(p.intent_code, IntentCode::XferFull);
        assert_eq!(p.reason, "payload-missing");
    }

    #[test]
    fn put_object_keeps_object_as_json_value() {
        let payload = json!({
            "version": 42,
            "kind": "flag",
            "key": "my-flag",
            "object": {"on": true, "variations": [false, true]}
        });
        let put: PutObject = serde_json::from_value(payload).unwrap();
        assert_eq!(put.version, 42);
        assert_eq!(put.kind, "flag");
        assert_eq!(put.key, "my-flag");
        assert_eq!(put.object["on"], json!(true));
    }

    #[test]
    fn goodbye_handles_optional_fields() {
        let bare: Goodbye = serde_json::from_value(json!({})).unwrap();
        assert!(bare.reason.is_none());
        assert!(bare.protocol_fallback_ttl.is_none());

        let full: Goodbye =
            serde_json::from_value(json!({"reason": "rotation", "protocolFallbackTTL": 30}))
                .unwrap();
        assert_eq!(full.reason.as_deref(), Some("rotation"));
        assert_eq!(full.protocol_fallback_ttl, Some(json!(30)));
    }

    fn goodbye(data: serde_json::Value) -> Goodbye {
        serde_json::from_value(data).unwrap()
    }

    #[test]
    fn goodbye_without_ttl_has_no_fallback() {
        assert!(goodbye(json!({"reason": "bye"})).fdv1_fallback().is_none());
    }

    #[test]
    fn goodbye_with_valid_ttl_uses_it_as_is() {
        for secs in [1, 90, 3600] {
            let directive = goodbye(json!({"protocolFallbackTTL": secs}))
                .fdv1_fallback()
                .expect("directive");
            assert_eq!(directive.ttl, Duration::from_secs(secs));
        }
    }

    #[test]
    fn goodbye_with_invalid_ttl_uses_jittered_default() {
        for ttl in [
            json!(0),
            json!(-5),
            json!(3601),
            json!(60.5),
            json!("60"),
            json!(true),
        ] {
            let directive = goodbye(json!({"protocolFallbackTTL": ttl}))
                .fdv1_fallback()
                .unwrap_or_else(|| panic!("no directive for {ttl}"));
            assert!(
                directive.ttl >= Duration::from_secs(30 * 60)
                    && directive.ttl <= Duration::from_secs(60 * 60),
                "ttl {:?} for {ttl}",
                directive.ttl
            );
        }
    }

    #[test]
    fn fdv2_error_handles_optional_id() {
        let with_id: FDv2Error =
            serde_json::from_value(json!({"id": "payload-1", "reason": "bad"})).unwrap();
        assert_eq!(with_id.id.as_deref(), Some("payload-1"));
        assert_eq!(with_id.reason, "bad");

        let no_id: FDv2Error = serde_json::from_value(json!({"reason": "other"})).unwrap();
        assert!(no_id.id.is_none());
        assert_eq!(no_id.reason, "other");
    }
}
