//! Configuration types for loraterm.
//!
//! Two files: `loraterm.toml` (daemon-wide settings, credentials reference, identity) and
//! `whitelist.toml` (peer pubkey list, hot-reloaded on SIGHUP).

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid pubkey in {path} (entry #{index}): {reason}")]
    InvalidPubkey {
        path: PathBuf,
        index: usize,
        reason: String,
    },
    #[error("{path} has insecure permissions (mode {mode:#o}); must be 0600")]
    InsecurePermissions { path: PathBuf, mode: u32 },
    #[error("duplicate pubkey in {path}: {pubkey}")]
    DuplicatePubkey { path: PathBuf, pubkey: String },
}

/// Top-level config — passed at startup, generally not hot-reloaded for connectivity fields.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bridge: BridgeConfig,
    pub identity: IdentityConfig,
    pub paths: PathsConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub chunking: ChunkingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BridgeConfig {
    /// e.g. "https://meshcore.dumke.me"
    pub base_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdentityConfig {
    /// Name of the dedicated companion identity to use / create on startup.
    pub name: String,
    /// "public" or "pool:<uuid>". Only used at bootstrap.
    #[serde(default = "default_identity_scope")]
    pub scope: String,
    /// Whether to send an initial advert on bootstrap.
    #[serde(default = "default_true")]
    pub advert_on_bootstrap: bool,
}

fn default_identity_scope() -> String {
    "public".to_string()
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct PathsConfig {
    pub whitelist: PathBuf,
    pub state_dir: PathBuf,
    pub peer_home_root: PathBuf,
    /// Optional fallback when systemd LoadCredential is not used.
    #[serde(default)]
    pub credentials_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LimitsConfig {
    /// Per-peer outbound DM rate; one DM per `dm_period`, with `dm_burst` initial budget.
    #[serde(default = "default_per_peer_period", with = "humantime_serde")]
    pub per_peer_period: Duration,
    #[serde(default = "default_per_peer_burst")]
    pub per_peer_burst: u32,
    #[serde(default = "default_global_period", with = "humantime_serde")]
    pub global_period: Duration,
    #[serde(default = "default_global_burst")]
    pub global_burst: u32,
    #[serde(default = "default_idle_timeout", with = "humantime_serde")]
    pub idle_timeout: Duration,
    /// Hard cap on chunks per single PTY output burst (post chunker).
    #[serde(default = "default_burst_chunk_cap")]
    pub burst_chunk_cap: usize,
    /// Inbound (DM → PTY) channel depth, lines per peer.
    #[serde(default = "default_inbound_cap")]
    pub inbound_channel_depth: usize,
    /// Outbound (chunker → sender) channel depth, chunks per peer.
    #[serde(default = "default_outbound_cap")]
    pub outbound_channel_depth: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            per_peer_period: default_per_peer_period(),
            per_peer_burst: default_per_peer_burst(),
            global_period: default_global_period(),
            global_burst: default_global_burst(),
            idle_timeout: default_idle_timeout(),
            burst_chunk_cap: default_burst_chunk_cap(),
            inbound_channel_depth: default_inbound_cap(),
            outbound_channel_depth: default_outbound_cap(),
        }
    }
}

fn default_per_peer_period() -> Duration {
    Duration::from_secs(4)
}
fn default_per_peer_burst() -> u32 {
    2
}
fn default_global_period() -> Duration {
    Duration::from_secs(1)
}
fn default_global_burst() -> u32 {
    5
}
fn default_idle_timeout() -> Duration {
    Duration::from_secs(1800)
}
fn default_burst_chunk_cap() -> usize {
    50
}
fn default_inbound_cap() -> usize {
    32
}
fn default_outbound_cap() -> usize {
    64
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkingConfig {
    #[serde(default = "default_dm_max_chars")]
    pub dm_max_chars: usize,
    /// If `true`, prepend `[i/N] ` to multi-chunk outputs.
    #[serde(default = "default_true")]
    pub number_chunks: bool,
    /// Flush silence threshold; after this much quiet PTY output, flush the current buffer.
    #[serde(default = "default_flush_silence", with = "humantime_serde")]
    pub flush_silence: Duration,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            dm_max_chars: default_dm_max_chars(),
            number_chunks: true,
            flush_silence: default_flush_silence(),
        }
    }
}

fn default_dm_max_chars() -> usize {
    120
}
fn default_flush_silence() -> Duration {
    Duration::from_millis(600)
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }
}

// ---------- Whitelist ----------

/// Whitelist file shape.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct WhitelistFile {
    #[serde(default, with = "humantime_serde::option")]
    pub default_idle_timeout: Option<Duration>,
    #[serde(default)]
    pub default_dm_max_chars: Option<usize>,
    #[serde(default, rename = "peer")]
    pub peers: Vec<PeerEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerEntry {
    pub pubkey: String,
    #[serde(default)]
    pub nickname: Option<String>,
    #[serde(default, with = "humantime_serde::option")]
    pub idle_timeout: Option<Duration>,
    #[serde(default)]
    pub dm_max_chars: Option<usize>,
}

/// Validated, in-memory whitelist. Pubkeys are stored as canonical lower-hex 64-char strings.
#[derive(Debug, Clone, Default)]
pub struct Whitelist {
    pub entries: Vec<ValidatedPeer>,
}

#[derive(Debug, Clone)]
pub struct ValidatedPeer {
    pub pubkey: String,
    pub nickname: Option<String>,
    pub idle_timeout: Option<Duration>,
    pub dm_max_chars: Option<usize>,
}

impl PartialEq for ValidatedPeer {
    fn eq(&self, other: &Self) -> bool {
        self.pubkey == other.pubkey
            && self.nickname == other.nickname
            && self.idle_timeout == other.idle_timeout
            && self.dm_max_chars == other.dm_max_chars
    }
}

impl Whitelist {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let file: WhitelistFile = toml::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;

        let mut seen: HashSet<String> = HashSet::new();
        let mut entries = Vec::with_capacity(file.peers.len());
        for (index, peer) in file.peers.into_iter().enumerate() {
            let canon = validate_pubkey(&peer.pubkey).map_err(|reason| ConfigError::InvalidPubkey {
                path: path.to_path_buf(),
                index,
                reason,
            })?;
            if !seen.insert(canon.clone()) {
                return Err(ConfigError::DuplicatePubkey {
                    path: path.to_path_buf(),
                    pubkey: canon,
                });
            }
            entries.push(ValidatedPeer {
                pubkey: canon,
                nickname: peer.nickname,
                idle_timeout: peer.idle_timeout.or(file.default_idle_timeout),
                dm_max_chars: peer.dm_max_chars.or(file.default_dm_max_chars),
            });
        }
        Ok(Self { entries })
    }

    pub fn contains(&self, pubkey: &str) -> bool {
        self.entries.iter().any(|e| e.pubkey == pubkey)
    }

    pub fn get(&self, pubkey: &str) -> Option<&ValidatedPeer> {
        self.entries.iter().find(|e| e.pubkey == pubkey)
    }
}

/// Diff between two whitelists. All pubkeys are canonical 64-char lower-hex.
#[derive(Debug, Default)]
pub struct WhitelistDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

pub fn diff_whitelist(old: &Whitelist, new: &Whitelist) -> WhitelistDiff {
    let old_keys: HashSet<&str> = old.entries.iter().map(|e| e.pubkey.as_str()).collect();
    let new_keys: HashSet<&str> = new.entries.iter().map(|e| e.pubkey.as_str()).collect();

    let added = new_keys
        .difference(&old_keys)
        .map(|s| (*s).to_owned())
        .collect();
    let removed = old_keys
        .difference(&new_keys)
        .map(|s| (*s).to_owned())
        .collect();

    let mut changed = Vec::new();
    for new_entry in &new.entries {
        if let Some(old_entry) = old.get(&new_entry.pubkey) {
            if old_entry != new_entry {
                changed.push(new_entry.pubkey.clone());
            }
        }
    }

    WhitelistDiff {
        added,
        removed,
        changed,
    }
}

fn validate_pubkey(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", trimmed.len()));
    }
    if !trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("non-hex characters".to_string());
    }
    Ok(trimmed.to_ascii_lowercase())
}

// ---------- Credentials ----------

#[derive(Debug, Clone, Deserialize)]
pub struct CredentialsFile {
    pub email: String,
    pub password: String,
}

/// Read credentials from a file, refusing any mode != 0600. Returns `(email, password)`.
pub fn load_credentials_file(path: &Path) -> Result<CredentialsFile, ConfigError> {
    let meta = fs::metadata(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let mode = meta.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(ConfigError::InsecurePermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    let raw = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&raw).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const VALID_HEX: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const VALID_HEX_2: &str =
        "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    fn write_toml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn whitelist_roundtrip() {
        let f = write_toml(&format!(
            r#"
            default_idle_timeout = "10m"
            [[peer]]
            pubkey = "{VALID_HEX}"
            nickname = "phone"
            [[peer]]
            pubkey = "{VALID_HEX_2}"
            nickname = "laptop"
            idle_timeout = "2h"
            "#
        ));
        let wl = Whitelist::load(f.path()).unwrap();
        assert_eq!(wl.entries.len(), 2);
        assert_eq!(wl.entries[0].pubkey, VALID_HEX);
        assert_eq!(wl.entries[0].nickname.as_deref(), Some("phone"));
        assert_eq!(wl.entries[0].idle_timeout, Some(Duration::from_secs(600)));
        assert_eq!(wl.entries[1].idle_timeout, Some(Duration::from_secs(7200)));
    }

    #[test]
    fn whitelist_uppercase_hex_is_normalized() {
        let f = write_toml(&format!(
            r#"
            [[peer]]
            pubkey = "{}"
            "#,
            VALID_HEX.to_uppercase()
        ));
        let wl = Whitelist::load(f.path()).unwrap();
        assert_eq!(wl.entries[0].pubkey, VALID_HEX);
    }

    #[test]
    fn whitelist_rejects_short_pubkey() {
        let f = write_toml(r#"[[peer]]
pubkey = "abcd"
"#);
        let err = Whitelist::load(f.path()).unwrap_err();
        match err {
            ConfigError::InvalidPubkey { index, reason, .. } => {
                assert_eq!(index, 0);
                assert!(reason.contains("64 hex chars"), "got: {reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn whitelist_rejects_nonhex() {
        let bad = "z".repeat(64);
        let f = write_toml(&format!(r#"[[peer]]
pubkey = "{bad}"
"#));
        let err = Whitelist::load(f.path()).unwrap_err();
        match err {
            ConfigError::InvalidPubkey { reason, .. } => {
                assert!(reason.contains("non-hex"), "got: {reason}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn whitelist_rejects_duplicate() {
        let f = write_toml(&format!(
            r#"
            [[peer]]
            pubkey = "{VALID_HEX}"
            [[peer]]
            pubkey = "{VALID_HEX}"
            "#
        ));
        assert!(matches!(
            Whitelist::load(f.path()).unwrap_err(),
            ConfigError::DuplicatePubkey { .. }
        ));
    }

    #[test]
    fn diff_detects_added_removed_changed() {
        let old = Whitelist {
            entries: vec![
                ValidatedPeer {
                    pubkey: VALID_HEX.to_owned(),
                    nickname: Some("phone".into()),
                    idle_timeout: None,
                    dm_max_chars: None,
                },
                ValidatedPeer {
                    pubkey: VALID_HEX_2.to_owned(),
                    nickname: None,
                    idle_timeout: None,
                    dm_max_chars: None,
                },
            ],
        };
        let new = Whitelist {
            entries: vec![
                ValidatedPeer {
                    pubkey: VALID_HEX.to_owned(),
                    nickname: Some("handheld".into()), // changed nickname
                    idle_timeout: None,
                    dm_max_chars: None,
                },
                // VALID_HEX_2 removed
                ValidatedPeer {
                    pubkey: "1".repeat(64),
                    nickname: None,
                    idle_timeout: None,
                    dm_max_chars: None,
                },
            ],
        };
        let diff = diff_whitelist(&old, &new);
        assert_eq!(diff.added, vec!["1".repeat(64)]);
        assert_eq!(diff.removed, vec![VALID_HEX_2.to_string()]);
        assert_eq!(diff.changed, vec![VALID_HEX.to_string()]);
    }

    #[test]
    fn credentials_file_rejects_world_readable() {
        let f = write_toml(r#"
email = "x@y.z"
password = "hunter2hunter2"
"#);
        // default tempfile mode is usually 0600 but be explicit
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            load_credentials_file(f.path()).unwrap_err(),
            ConfigError::InsecurePermissions { .. }
        ));
    }

    #[test]
    fn credentials_file_accepts_0600() {
        let f = write_toml(r#"
email = "x@y.z"
password = "hunter2hunter2"
"#);
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o600)).unwrap();
        let c = load_credentials_file(f.path()).unwrap();
        assert_eq!(c.email, "x@y.z");
    }
}
