//! SSE consumer for `GET /api/v1/companion/identities/{id}/stream`.
//!
//! The exact event-frame envelope is not fully specified upstream — we know the
//! event families (DM, Channel, Sent-Echo, Contact-Update, status_response) and the
//! per-message shape, but not the exact SSE `event:` names. We parse leniently:
//!   1. Dispatch by SSE `event:` field if present.
//!   2. Fall back to a structural probe: any object with `peer_pubkey_hex` + `text` is a DM-ish.
//!   3. Unknown → `SseEvent::Other`, optionally logged to a raw-log file.

use std::path::PathBuf;
use std::time::Duration;

use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::client::{ApiError, CompanionClient};

#[derive(Debug, Error)]
pub enum SseError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("api: {0}")]
    Api(#[from] ApiError),
    #[error("event parse: {0}")]
    EventParse(String),
}

/// Narrow shape extracted from a "DM received" event (echoes carry the same shape).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DmEvent {
    #[serde(default)]
    pub id: Option<String>,
    pub identity_id: Option<String>,
    pub peer_pubkey_hex: String,
    #[serde(default)]
    pub peer_name: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub payload_type: Option<String>,
    #[serde(default)]
    pub ts: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SseEvent {
    DmReceived(DmEvent),
    SentEcho(DmEvent),
    Other { kind: String, raw: Value },
    /// Keep-alive comment or unknown frame; ignored by daemon but logged.
    Comment(String),
}

pub struct SseStream {
    client: std::sync::Arc<CompanionClient>,
    identity_id: Uuid,
    raw_log: Option<PathBuf>,
}

impl SseStream {
    pub fn new(
        client: std::sync::Arc<CompanionClient>,
        identity_id: Uuid,
        raw_log: Option<PathBuf>,
    ) -> Self {
        Self {
            client,
            identity_id,
            raw_log,
        }
    }

    /// Long-running task: connect → read events → push to `tx`; reconnect with exponential
    /// backoff on errors; honour `shutdown_rx`.
    pub async fn run(
        self,
        tx: mpsc::Sender<SseEvent>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut backoff = Duration::from_secs(1);
        let backoff_cap = Duration::from_secs(60);

        let url = format!(
            "{}/api/v1/companion/identities/{}/stream",
            self.client.base_url(),
            self.identity_id
        );

        let mut raw_log_writer: Option<tokio::fs::File> = match &self.raw_log {
            Some(p) => match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .await
            {
                Ok(f) => Some(f),
                Err(e) => {
                    warn!(path = %p.display(), error = %e, "cannot open SSE raw log");
                    None
                }
            },
            None => None,
        };

        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            info!(url = %url, "connecting SSE");
            let resp = match self.client.http().get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "SSE connect failed");
                    if !sleep_with_shutdown(backoff, &mut shutdown_rx).await {
                        break;
                    }
                    backoff = (backoff * 2).min(backoff_cap);
                    continue;
                }
            };

            let status = resp.status();
            if !status.is_success() {
                warn!(%status, "SSE non-200; backing off");
                if !sleep_with_shutdown(backoff, &mut shutdown_rx).await {
                    break;
                }
                backoff = (backoff * 2).min(backoff_cap);
                continue;
            }

            backoff = Duration::from_secs(1);
            info!("SSE connected");

            let bytes_stream = resp.bytes_stream();
            let mut events = bytes_stream.eventsource();

            // Watchdog: expect *something* (event or comment) within 60s.
            let watchdog = Duration::from_secs(60);

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() { return; }
                    }
                    next = tokio::time::timeout(watchdog, events.next()) => {
                        match next {
                            Err(_) => {
                                warn!("SSE keep-alive watchdog tripped; reconnecting");
                                break;
                            }
                            Ok(None) => {
                                info!("SSE stream ended; reconnecting");
                                break;
                            }
                            Ok(Some(Err(e))) => {
                                warn!(error = %e, "SSE frame error; reconnecting");
                                break;
                            }
                            Ok(Some(Ok(event))) => {
                                if let Some(w) = &mut raw_log_writer {
                                    let line = format!(
                                        "event={} id={} data={}\n",
                                        event.event,
                                        event.id,
                                        event.data,
                                    );
                                    let _ = w.write_all(line.as_bytes()).await;
                                }
                                if event.data.is_empty() {
                                    let _ = tx.send(SseEvent::Comment(event.event.clone())).await;
                                    continue;
                                }
                                let parsed = parse_event(&event.event, &event.data);
                                if tx.send(parsed).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            if !sleep_with_shutdown(backoff, &mut shutdown_rx).await {
                break;
            }
            backoff = (backoff * 2).min(backoff_cap);
        }
    }
}

async fn sleep_with_shutdown(
    dur: Duration,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    let jitter_ms = (rand::random::<u64>() % 500) as u128;
    let total = dur + Duration::from_millis(jitter_ms as u64);
    tokio::select! {
        _ = tokio::time::sleep(total) => true,
        _ = shutdown.changed() => !*shutdown.borrow(),
    }
}

fn parse_event(event_name: &str, data: &str) -> SseEvent {
    let value: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            debug!(error = %e, head = %&data[..data.len().min(200)], "malformed SSE JSON");
            return SseEvent::Other {
                kind: event_name.to_owned(),
                raw: Value::String(data.to_owned()),
            };
        }
    };

    let lowered = event_name.to_ascii_lowercase();
    if lowered.contains("dm") && lowered.contains("recv")
        || lowered == "message"
        || lowered == "dm_received"
    {
        if let Ok(dm) = serde_json::from_value::<DmEvent>(value.clone()) {
            return SseEvent::DmReceived(dm);
        }
    }
    if lowered.contains("echo") || lowered.contains("sent") {
        if let Ok(dm) = serde_json::from_value::<DmEvent>(value.clone()) {
            return SseEvent::SentEcho(dm);
        }
    }

    // Structural fallback: try DmEvent shape on any unknown.
    if let Ok(dm) = serde_json::from_value::<DmEvent>(value.clone()) {
        if dm.direction.as_deref() == Some("in") {
            return SseEvent::DmReceived(dm);
        }
        if dm.direction.as_deref() == Some("out") {
            return SseEvent::SentEcho(dm);
        }
    }

    SseEvent::Other {
        kind: event_name.to_owned(),
        raw: value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inbound_dm_by_structure() {
        let data = r#"{"id":"1","peer_pubkey_hex":"ab","text":"hi","direction":"in"}"#;
        match parse_event("", data) {
            SseEvent::DmReceived(dm) => {
                assert_eq!(dm.peer_pubkey_hex, "ab");
                assert_eq!(dm.text.as_deref(), Some("hi"));
            }
            other => panic!("expected DmReceived, got {other:?}"),
        }
    }

    #[test]
    fn parses_echo_when_direction_out() {
        let data = r#"{"id":"1","peer_pubkey_hex":"ab","text":"hi","direction":"out"}"#;
        match parse_event("", data) {
            SseEvent::SentEcho(_) => {}
            other => panic!("expected SentEcho, got {other:?}"),
        }
    }

    #[test]
    fn parses_dm_by_event_name() {
        let data = r#"{"peer_pubkey_hex":"cd","text":"yo"}"#;
        match parse_event("dm_received", data) {
            SseEvent::DmReceived(dm) => assert_eq!(dm.peer_pubkey_hex, "cd"),
            other => panic!("expected DmReceived, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_other_for_unknown() {
        let data = r#"{"foo":"bar"}"#;
        match parse_event("contact_update", data) {
            SseEvent::Other { kind, .. } => assert_eq!(kind, "contact_update"),
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[test]
    fn malformed_json_becomes_other_string() {
        match parse_event("dm", "not json") {
            SseEvent::Other { raw, .. } => {
                assert_eq!(raw, Value::String("not json".to_string()));
            }
            other => panic!("expected Other, got {other:?}"),
        }
    }
}
