//! REST client for the MeshCore companion API.
//!
//! Auth model: session cookie set by `POST /login` (form-urlencoded). The cookie jar is
//! persisted to disk so the daemon survives restarts inside the bridge's idle window
//! (default 30 days).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::{Client, StatusCode};
use reqwest_cookie_store::CookieStoreMutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("HTTP transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("unauthorized (401)")]
    Unauthorized,
    #[error("forbidden (403)")]
    Forbidden,
    #[error("not found (404)")]
    NotFound,
    #[error("conflict (409) — companion not loaded?")]
    Conflict,
    #[error("service unavailable (503)")]
    ServiceUnavailable,
    #[error("unexpected status {status}: {body}")]
    Unexpected { status: StatusCode, body: String },
    #[error("login rate-limited; try again after {0:?}")]
    LoginRateLimited(Duration),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct Credentials {
    pub email: String,
    pub password: String,
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

#[derive(Debug, Serialize)]
struct LoginForm<'a> {
    email: &'a str,
    password: &'a str,
}

#[derive(Debug, Serialize)]
struct CreateIdentityForm<'a> {
    name: &'a str,
    scope: &'a str,
}

#[derive(Debug, Serialize)]
struct SendDmForm<'a> {
    identity_id: &'a str,
    peer_pubkey_hex: &'a str,
    text: &'a str,
}

/// Persistent JSON wrapper for the cookie jar.
fn load_cookie_store(path: &Path) -> Arc<CookieStoreMutex> {
    if let Ok(file) = std::fs::File::open(path) {
        match cookie_store::serde::json::load(std::io::BufReader::new(file)) {
            Ok(store) => return Arc::new(CookieStoreMutex::new(store)),
            Err(e) => warn!(path = %path.display(), error = %e, "cookie jar parse failed; starting empty"),
        }
    }
    Arc::new(CookieStoreMutex::new(cookie_store::CookieStore::default()))
}

fn save_cookie_store(path: &Path, store: &CookieStoreMutex) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        let guard = store.lock().expect("cookie jar poisoned");
        cookie_store::serde::json::save_incl_expired_and_nonpersistent(&guard, &mut file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub struct CompanionClient {
    base_url: String,
    http: Client,
    cookie_store: Arc<CookieStoreMutex>,
    cookie_path: PathBuf,
    credentials: Credentials,
    last_login_attempt: Mutex<Option<Instant>>,
}

impl CompanionClient {
    pub fn new(
        base_url: impl Into<String>,
        cookie_path: PathBuf,
        credentials: Credentials,
    ) -> Result<Self, ApiError> {
        let cookie_store = load_cookie_store(&cookie_path);
        let http = Client::builder()
            .cookie_provider(cookie_store.clone())
            .user_agent(concat!("loraterm/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            cookie_store,
            cookie_path,
            credentials,
            last_login_attempt: Mutex::new(None),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub async fn persist_cookies(&self) {
        let path = self.cookie_path.clone();
        let store = self.cookie_store.clone();
        if let Err(e) = tokio::task::spawn_blocking(move || save_cookie_store(&path, &store))
            .await
            .unwrap_or_else(|_| Ok(()))
        {
            warn!(error = %e, "failed to persist cookie jar");
        }
    }

    /// Probe `/api/v1/companion/identities`; on 401 → login + retry once.
    pub async fn ensure_session(&self) -> Result<(), ApiError> {
        match self.get_identities_raw().await {
            Ok(_) => Ok(()),
            Err(ApiError::Unauthorized) => {
                self.login().await?;
                self.get_identities_raw().await.map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    /// Login via form-urlencoded POST. Rate-limited to one attempt per 5s.
    pub async fn login(&self) -> Result<(), ApiError> {
        {
            let mut last = self.last_login_attempt.lock().await;
            if let Some(t) = *last {
                let elapsed = t.elapsed();
                if elapsed < Duration::from_secs(5) {
                    return Err(ApiError::LoginRateLimited(Duration::from_secs(5) - elapsed));
                }
            }
            *last = Some(Instant::now());
        }

        let form = LoginForm {
            email: &self.credentials.email,
            password: &self.credentials.password,
        };
        let resp = self
            .http
            .post(self.url("/login"))
            .form(&form)
            // The API returns 303 on success; we don't want reqwest to auto-follow into /dashboard
            // because the cookie is the only thing we want.
            .send()
            .await?;

        let status = resp.status();
        if status.is_success() || status.is_redirection() {
            info!(status = %status, "login OK");
            self.persist_cookies().await;
            Ok(())
        } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            Err(ApiError::Unauthorized)
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(ApiError::Unexpected { status, body })
        }
    }

    async fn get_identities_raw(&self) -> Result<Vec<Identity>, ApiError> {
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
            StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
            StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
            StatusCode::SERVICE_UNAVAILABLE => Err(ApiError::ServiceUnavailable),
            StatusCode::CONFLICT => Err(ApiError::Conflict),
            other => Err(ApiError::Unexpected {
                status: other,
                body: resp.text().await.unwrap_or_default(),
            }),
        }
    }

    pub async fn list_identities(&self) -> Result<Vec<Identity>, ApiError> {
        match self.get_identities_raw().await {
            Ok(v) => Ok(v),
            Err(ApiError::Unauthorized) => {
                debug!("got 401 on list_identities; re-logging in");
                self.login().await?;
                self.get_identities_raw().await
            }
            Err(e) => Err(e),
        }
    }

    pub async fn create_identity(&self, name: &str, scope: &str) -> Result<Identity, ApiError> {
        let form = CreateIdentityForm { name, scope };
        let mut attempts = 0u8;
        loop {
            let resp = self
                .http
                .post(self.url("/api/v1/companion/identities"))
                .form(&form)
                .send()
                .await?;
            let status = resp.status();
            if status.is_success() {
                return Ok(resp.json().await?);
            }
            if status == StatusCode::UNAUTHORIZED && attempts == 0 {
                attempts += 1;
                self.login().await?;
                continue;
            }
            return map_status_err(status, resp).await;
        }
    }

    pub async fn advert(&self, identity_id: Uuid) -> Result<(), ApiError> {
        let mut attempts = 0u8;
        loop {
            let resp = self
                .http
                .post(self.url(&format!(
                    "/api/v1/companion/identities/{identity_id}/advert"
                )))
                .send()
                .await?;
            let status = resp.status();
            if status.is_success() {
                return Ok(());
            }
            if status == StatusCode::UNAUTHORIZED && attempts == 0 {
                attempts += 1;
                self.login().await?;
                continue;
            }
            return map_status_err::<()>(status, resp).await;
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
        let mut attempts = 0u8;
        loop {
            let resp = self
                .http
                .post(self.url("/api/v1/companion/messages/dm"))
                .form(&form)
                .send()
                .await?;
            let status = resp.status();
            if status.is_success() {
                debug!(peer = %peer_pubkey_hex, "dm sent");
                return Ok(());
            }
            if status == StatusCode::UNAUTHORIZED && attempts == 0 {
                attempts += 1;
                self.login().await?;
                continue;
            }
            return map_status_err::<()>(status, resp).await;
        }
    }

    /// Direct access to the cookie-aware HTTP client for streaming endpoints (SSE).
    pub fn http(&self) -> &Client {
        &self.http
    }
}

async fn map_status_err<T>(
    status: StatusCode,
    resp: reqwest::Response,
) -> Result<T, ApiError> {
    match status {
        StatusCode::UNAUTHORIZED => Err(ApiError::Unauthorized),
        StatusCode::FORBIDDEN => Err(ApiError::Forbidden),
        StatusCode::NOT_FOUND => Err(ApiError::NotFound),
        StatusCode::CONFLICT => Err(ApiError::Conflict),
        StatusCode::SERVICE_UNAVAILABLE => Err(ApiError::ServiceUnavailable),
        other => Err(ApiError::Unexpected {
            status: other,
            body: resp.text().await.unwrap_or_default(),
        }),
    }
}

// Allow `&form` cloning for retry path.
impl<'a> Clone for SendDmForm<'a> {
    fn clone(&self) -> Self {
        Self {
            identity_id: self.identity_id,
            peer_pubkey_hex: self.peer_pubkey_hex,
            text: self.text,
        }
    }
}
impl<'a> Clone for CreateIdentityForm<'a> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            scope: self.scope,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn creds() -> Credentials {
        Credentials {
            email: "test@example.com".into(),
            password: "hunter2hunter2".into(),
        }
    }

    #[tokio::test]
    async fn login_sets_cookie_and_persists() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(
                ResponseTemplate::new(303)
                    .insert_header("Set-Cookie", "mc_session=abc123; Path=/; HttpOnly"),
            )
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let cookie_path = dir.path().join("cookies.json");
        let client = CompanionClient::new(server.uri(), cookie_path.clone(), creds()).unwrap();
        client.login().await.unwrap();
        assert!(cookie_path.exists(), "cookies should have been persisted");
    }

    #[tokio::test]
    async fn list_identities_relogs_on_401() {
        let server = MockServer::start().await;

        // First call to identities → 401
        Mock::given(method("GET"))
            .and(path("/api/v1/companion/identities"))
            .respond_with(ResponseTemplate::new(401))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        // Login → 303 with cookie
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(
                ResponseTemplate::new(303)
                    .insert_header("Set-Cookie", "mc_session=abc; Path=/; HttpOnly"),
            )
            .mount(&server)
            .await;

        // Retry → 200
        Mock::given(method("GET"))
            .and(path("/api/v1/companion/identities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let client =
            CompanionClient::new(server.uri(), dir.path().join("cookies.json"), creds()).unwrap();
        let ids = client.list_identities().await.unwrap();
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn send_dm_form_encoded() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/companion/messages/dm"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let client =
            CompanionClient::new(server.uri(), dir.path().join("cookies.json"), creds()).unwrap();
        let id = Uuid::new_v4();
        client.send_dm(id, "deadbeef".repeat(8).as_str(), "hello").await.unwrap();
    }

    #[tokio::test]
    async fn login_rate_limited_within_5s() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/login"))
            .respond_with(ResponseTemplate::new(303).insert_header("Set-Cookie", "mc_session=x"))
            .mount(&server)
            .await;
        let dir = tempdir().unwrap();
        let client =
            CompanionClient::new(server.uri(), dir.path().join("cookies.json"), creds()).unwrap();
        client.login().await.unwrap();
        let err = client.login().await.unwrap_err();
        assert!(matches!(err, ApiError::LoginRateLimited(_)));
    }
}
