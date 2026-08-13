use super::model::{ChangeSet, ChangeSetKind, FDv2Change};
use super::wire::{
    DeleteObject, FDv2Error, Goodbye, IntentCode, PayloadTransferred, PutObject, ServerIntent,
};

pub(super) enum ProtocolResult {
    None,
    ChangeSet(ChangeSet),
    Error(ProtocolError),
    Goodbye(Goodbye),
}

#[derive(Debug)]
pub(super) enum ProtocolError {
    JsonParse(String),
    Protocol(String),
    Server(FDv2Error),
}

enum State {
    Inactive,
    Full,
    Partial,
}

pub(super) struct FDv2ProtocolHandler {
    state: State,
    changes: Vec<FDv2Change>,
}

impl FDv2ProtocolHandler {
    pub(super) fn new() -> Self {
        Self {
            state: State::Inactive,
            changes: Vec::new(),
        }
    }

    pub(super) fn reset(&mut self) {
        self.state = State::Inactive;
        self.changes.clear();
    }

    /// Reports whether the FDv2 protocol handler recognizes this event type.
    pub(super) fn is_known_event(event_type: &str) -> bool {
        matches!(
            event_type,
            "server-intent"
                | "put-object"
                | "delete-object"
                | "payload-transferred"
                | "error"
                | "goodbye"
        )
    }

    pub(super) fn handle_event(
        &mut self,
        event_type: &str,
        data: serde_json::Value,
    ) -> ProtocolResult {
        match event_type {
            "server-intent" => self.handle_server_intent(data),
            "put-object" => self.handle_put_object(data),
            "delete-object" => self.handle_delete_object(data),
            "payload-transferred" => self.handle_payload_transferred(data),
            "error" => self.handle_error(data),
            "goodbye" => self.handle_goodbye(data),
            _ => ProtocolResult::None,
        }
    }

    fn handle_server_intent(&mut self, data: serde_json::Value) -> ProtocolResult {
        let intent: ServerIntent = match serde_json::from_value(data) {
            Ok(v) => v,
            Err(e) => {
                self.reset();
                return ProtocolResult::Error(ProtocolError::JsonParse(format!(
                    "could not deserialize server-intent: {e}"
                )));
            }
        };
        let Some(payload) = intent.payloads.into_iter().next() else {
            self.reset();
            return ProtocolResult::None;
        };
        self.changes.clear();
        match payload.intent_code {
            IntentCode::XferFull => {
                self.state = State::Full;
                ProtocolResult::None
            }
            IntentCode::XferChanges => {
                self.state = State::Partial;
                ProtocolResult::None
            }
            IntentCode::None => {
                self.state = State::Partial;
                ProtocolResult::ChangeSet(ChangeSet {
                    kind: ChangeSetKind::None,
                    changes: Vec::new(),
                    selector: None,
                })
            }
            IntentCode::Unknown => {
                self.reset();
                ProtocolResult::Error(ProtocolError::Protocol(
                    "server-intent had an unrecognized intent code".into(),
                ))
            }
        }
    }

    fn handle_put_object(&mut self, data: serde_json::Value) -> ProtocolResult {
        let put: PutObject = match serde_json::from_value(data) {
            Ok(v) => v,
            Err(e) => {
                self.reset();
                return ProtocolResult::Error(ProtocolError::JsonParse(format!(
                    "could not deserialize put-object: {e}"
                )));
            }
        };
        self.changes.push(FDv2Change::Put(put));
        ProtocolResult::None
    }

    fn handle_delete_object(&mut self, data: serde_json::Value) -> ProtocolResult {
        let del: DeleteObject = match serde_json::from_value(data) {
            Ok(v) => v,
            Err(e) => {
                self.reset();
                return ProtocolResult::Error(ProtocolError::JsonParse(format!(
                    "could not deserialize delete-object: {e}"
                )));
            }
        };
        self.changes.push(FDv2Change::Delete(del));
        ProtocolResult::None
    }

    fn handle_payload_transferred(&mut self, data: serde_json::Value) -> ProtocolResult {
        if matches!(self.state, State::Inactive) {
            self.reset();
            return ProtocolResult::Error(ProtocolError::Protocol(
                "payload-transferred received without an active server-intent".into(),
            ));
        }
        let transferred: PayloadTransferred = match serde_json::from_value(data) {
            Ok(v) => v,
            Err(e) => {
                self.reset();
                return ProtocolResult::Error(ProtocolError::JsonParse(format!(
                    "could not deserialize payload-transferred: {e}"
                )));
            }
        };
        let kind = match self.state {
            State::Full => ChangeSetKind::Full,
            State::Partial => ChangeSetKind::Partial,
            State::Inactive => unreachable!(),
        };
        let changeset = ChangeSet {
            kind,
            changes: std::mem::take(&mut self.changes),
            selector: Some(transferred.state),
        };
        // Subsequent put/delete + payload-transferred cycles continue as
        // partial transfers without a new server-intent.
        self.state = State::Partial;
        ProtocolResult::ChangeSet(changeset)
    }

    fn handle_error(&mut self, data: serde_json::Value) -> ProtocolResult {
        self.changes.clear();
        match serde_json::from_value::<FDv2Error>(data) {
            Ok(err) => ProtocolResult::Error(ProtocolError::Server(err)),
            Err(e) => ProtocolResult::Error(ProtocolError::JsonParse(format!(
                "could not deserialize error event: {e}"
            ))),
        }
    }

    fn handle_goodbye(&mut self, data: serde_json::Value) -> ProtocolResult {
        self.reset();
        // Parse failures are intentional: the caller rotates sources whether
        // or not the reason field is readable.
        let goodbye = serde_json::from_value::<Goodbye>(data).unwrap_or(Goodbye {
            reason: None,
            protocol_fallback_ttl: None,
        });
        ProtocolResult::Goodbye(goodbye)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn server_intent(code: &str) -> serde_json::Value {
        json!({"payloads": [{
            "id": "p",
            "target": 1,
            "intentCode": code,
            "reason": "payload-missing",
        }]})
    }

    fn put_flag(key: &str, version: u64) -> serde_json::Value {
        json!({"version": version, "kind": "flag", "key": key, "object": {"on": true}})
    }

    fn delete_segment(key: &str, version: u64) -> serde_json::Value {
        json!({"version": version, "kind": "segment", "key": key})
    }

    fn payload_transferred(state: &str) -> serde_json::Value {
        json!({"state": state})
    }

    fn expect_changeset(result: ProtocolResult) -> ChangeSet {
        match result {
            ProtocolResult::ChangeSet(cs) => cs,
            _ => panic!("expected ChangeSet"),
        }
    }

    #[test]
    fn full_cycle_emits_full_changeset() {
        let mut h = FDv2ProtocolHandler::new();
        h.handle_event("server-intent", server_intent("xfer-full"));
        h.handle_event("put-object", put_flag("a", 1));
        h.handle_event("delete-object", delete_segment("old", 2));
        let cs =
            expect_changeset(h.handle_event("payload-transferred", payload_transferred("s-1")));

        assert_eq!(cs.kind, ChangeSetKind::Full);

        let FDv2Change::Put(put) = &cs.changes[0] else {
            panic!("expected Put");
        };
        assert_eq!(put.kind, "flag");
        assert_eq!(put.key, "a");
        assert_eq!(put.version, 1);
        assert_eq!(put.object["on"], json!(true));

        let FDv2Change::Delete(del) = &cs.changes[1] else {
            panic!("expected Delete");
        };
        assert_eq!(del.kind, "segment");
        assert_eq!(del.key, "old");
        assert_eq!(del.version, 2);

        assert_eq!(cs.selector.as_deref(), Some("s-1"));
    }

    #[test]
    fn partial_cycle_after_payload_transferred_needs_no_new_intent() {
        let mut h = FDv2ProtocolHandler::new();
        h.handle_event("server-intent", server_intent("xfer-full"));
        h.handle_event("put-object", put_flag("a", 1));
        let _ = h.handle_event("payload-transferred", payload_transferred("s-1"));

        // Without a new server-intent, the next cycle is Partial.
        h.handle_event("put-object", put_flag("b", 2));
        let cs =
            expect_changeset(h.handle_event("payload-transferred", payload_transferred("s-2")));
        assert_eq!(cs.kind, ChangeSetKind::Partial);
        assert_eq!(cs.changes.len(), 1);
    }

    #[test]
    fn server_intent_none_emits_empty_changeset() {
        let mut h = FDv2ProtocolHandler::new();
        let cs = expect_changeset(h.handle_event("server-intent", server_intent("none")));
        assert_eq!(cs.kind, ChangeSetKind::None);
        assert!(cs.changes.is_empty());
        assert!(cs.selector.is_none());
    }

    #[test]
    fn unknown_intent_code_is_protocol_error() {
        let mut h = FDv2ProtocolHandler::new();
        let result = h.handle_event("server-intent", server_intent("brand-new"));
        let ProtocolResult::Error(ProtocolError::Protocol(msg)) = result else {
            panic!("expected Protocol error");
        };
        assert!(msg.contains("unrecognized intent code"));
    }

    #[test]
    fn payload_transferred_without_intent_is_protocol_error() {
        let mut h = FDv2ProtocolHandler::new();
        let result = h.handle_event("payload-transferred", payload_transferred("s-1"));
        assert!(matches!(
            result,
            ProtocolResult::Error(ProtocolError::Protocol(_))
        ));
    }

    #[test]
    fn error_event_clears_accumulated_changes_but_keeps_state() {
        let mut h = FDv2ProtocolHandler::new();
        h.handle_event("server-intent", server_intent("xfer-full"));
        h.handle_event("put-object", put_flag("a", 1));

        let result = h.handle_event("error", json!({"id": "p", "reason": "bad"}));
        let ProtocolResult::Error(ProtocolError::Server(err)) = result else {
            panic!("expected Server error");
        };
        assert_eq!(err.reason, "bad");

        // State preserved: a new put + payload-transferred should still emit Full.
        h.handle_event("put-object", put_flag("b", 2));
        let cs =
            expect_changeset(h.handle_event("payload-transferred", payload_transferred("s-1")));
        assert_eq!(cs.kind, ChangeSetKind::Full);
        assert_eq!(cs.changes.len(), 1, "accumulated changes were not cleared");
    }

    #[test]
    fn goodbye_resets_handler() {
        let mut h = FDv2ProtocolHandler::new();
        h.handle_event("server-intent", server_intent("xfer-full"));
        h.handle_event("put-object", put_flag("a", 1));

        let result = h.handle_event("goodbye", json!({"reason": "rotating"}));
        let ProtocolResult::Goodbye(g) = result else {
            panic!("expected Goodbye");
        };
        assert_eq!(g.reason.as_deref(), Some("rotating"));

        // After goodbye, payload-transferred should now be a protocol error
        // (state is Inactive again).
        let result = h.handle_event("payload-transferred", payload_transferred("s-1"));
        assert!(matches!(
            result,
            ProtocolResult::Error(ProtocolError::Protocol(_))
        ));
    }

    #[test]
    fn unknown_event_type_is_noop() {
        let mut h = FDv2ProtocolHandler::new();
        assert!(matches!(
            h.handle_event("heartbeat", json!({})),
            ProtocolResult::None
        ));
    }

    #[test]
    fn malformed_event_data_is_json_error() {
        let mut h = FDv2ProtocolHandler::new();
        let result = h.handle_event("server-intent", json!({"payloads": "not-an-array"}));
        let ProtocolResult::Error(ProtocolError::JsonParse(msg)) = result else {
            panic!("expected JsonParse error");
        };
        assert!(msg.contains("server-intent"));
    }
}
