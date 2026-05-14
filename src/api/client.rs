//! REST client for the MeshCore companion API.
//!
//! Auth: Bearer token (per-companion-identity). 32-char base32 string, sent as
//! `Authorization: Bearer <token>` on every request. The token locks the daemon to
//! one Identity; calls against any other Identity yield 403.

use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("HTTP transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("unauthorized (401) — token unknown, expired or revoked")]
    Unauthorized,
    #[error("forbidden (403): {0}")]
    Forbidden(String),
    #[error("not found (404)")]
    NotFound,
    #[error("conflict (409) — companion not loaded?")]
    Conflict,
    #[error("service unavailable (503)")]
    ServiceUnavailable,
    #[error("unexpected status {status}: {body}")]
    Unexpected { status: StatusCode, body: String },
    #[error("invalid token: {0}")]
    InvalidToken(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// 32-char base32 bearer token tied to a single companion identity.
#[derive(Clone)]
pub struct Token(String);

impl Token {
    pub fn new(raw: impl Into<String>) -> Result<Self, ApiError> {
        let t = raw.into().trim().to_string();
        if t.is_empty() {
            return Err(ApiError::InvalidToken("empty".into()));
        }
        if t.len() < 16 || t.len() > 128 {
            return Err(ApiError::InvalidToken(format!(
                "unexpected length {}",
                t.len()
            )));
        }
        // Server format is 32 base32 chars but we don't hard-enforce in case the
        // server policy changes; just sanity-check the chars are non-whitespace ASCII.
        if !t.chars().all(|c| c.is_ascii_graphic()) {
            return Err(ApiError::InvalidToken("non-graphic ASCII".into()));
        }
        Ok(Self(t))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let head: String = self.0.chars().take(4).collect();
        write!(f, "Token(\"{head}…\")")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Identity {
    pub id: Uuid,
    pub name: String,
    pub scope: String,
    #[serde(default)]
    pub pubkey_hex: Option<String>,
    #[serde(default)]
    pub archived_at: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct SendDmForm<'a> {
    identity_id: &'a str,
    peer_pubkey_hex: &'a str,
    text: &'a str,
}

pub struct CompanionClient {
    base_url: String,
    http: Client,
}

impl CompanionClient {
    pub fn new(base_url: impl Into<String>, token: Token) -> Result<Self, ApiError> {
        let mut auth_value = HeaderValue::from_str(&format!("Bearer {}", token.as_str()))
            .map_err(|e| ApiError::InvalidToken(e.to_string()))?;
        auth_value.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, auth_value);

        let http = Client::builder()
            .default_headers(headers)
            .user_agent(concat!("loraterm/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Probe that the token is valid + the companion is loaded. Returns the list
    /// of identities the token can see (with bearer auth this is exactly one).
    pub async fn list_identities(&self) -> Result<Vec<Identity>, ApiError> {
        let resp = self
            .http
            .get(self.url("/api/v1/companion/identities"))
            .send()
            .await?;
        let status = resp.status();
        match status {
            s if s.is_success() => {
                let bytes = resp.bytes().await?;
                Ok(serde_json::from_slice(&bytes)?)
            }
            other => Err(map_status(other, resp).await),
        }
    }

    pub async fn send_dm(
        &self,
        identity_id: Uuid,
        peer_pubkey_hex: &str,
        text: &str,
    ) -> Result<(), ApiError> {
        let id_str = identity_id.to_string();
        let form = SendDmForm {
            identity_id: &id_str,
            peer_pubkey_hex,
            text,
        };
        let resp = self
            .http
            .post(self.url("/api/v1/companion/messages/dm"))
            .form(&form)
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            debug!(peer = %peer_pubkey_hex, "dm sent");
            Ok(())
        } else {
            Err(map_status(status, resp).await)
        }
    }

    /// Direct access for streaming endpoints (SSE) which need the bearer header
    /// (carried automatically as a default header).
    pub fn http(&self) -> &Client {
        &self.http
    }
}

async fn map_status(status: StatusCode, resp: reqwest::Response) -> ApiError {
    match status {
        StatusCode::UNAUTHORIZED => ApiError::Unauthorized,
        StatusCode::FORBIDDEN => {
            let body = resp.text().await.unwrap_or_default();
            ApiError::Forbidden(body)
        }
        StatusCode::NOT_FOUND => ApiError::NotFound,
        StatusCode::CONFLICT => ApiError::Conflict,
        StatusCode::SERVICE_UNAVAILABLE => ApiError::ServiceUnavailable,
        other => ApiError::Unexpected {
            status: other,
            body: resp.text().await.unwrap_or_default(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{bearer_token, header_exists, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn token() -> Token {
        Token::new("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567").unwrap()
    }

    #[tokio::test]
    async fn list_identities_sends_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/companion/identities"))
            .and(bearer_token("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "shell@test",
                "scope": "public"
            }])))
            .mount(&server)
            .await;

        let client = CompanionClient::new(server.uri(), token()).unwrap();
        let ids = client.list_identities().await.unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].name, "shell@test");
    }

    #[tokio::test]
    async fn unauthorized_maps_cleanly() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v1/companion/identities"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let client = CompanionClient::new(server.uri(), token()).unwrap();
        assert!(matches!(
            client.list_identities().await.unwrap_err(),
            ApiError::Unauthorized
        ));
    }

    #[tokio::test]
    async fn send_dm_form_encoded_with_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/companion/messages/dm"))
            .and(header_exists("authorization"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})),
            )
            .mount(&server)
            .await;
        let client = CompanionClient::new(server.uri(), token()).unwrap();
        let id = Uuid::new_v4();
        let pk = "ab".repeat(32);
        client.send_dm(id, &pk, "hello").await.unwrap();
    }

    #[test]
    fn token_rejects_empty() {
        assert!(Token::new("").is_err());
        assert!(Token::new("   ").is_err());
    }

    #[test]
    fn token_debug_redacts() {
        let t = token();
        let s = format!("{t:?}");
        assert!(s.contains("ABCD"));
        assert!(!s.contains("EFGH"), "got: {s}");
    }
}
