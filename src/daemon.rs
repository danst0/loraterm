//! Top-level orchestrator. Builds the API client, resolves the bound companion identity,
//! starts the SSE consumer and signal task, owns the peer registry + ArcSwap<Whitelist>,
//! and dispatches inbound events to PeerActors.

use std::collections::HashMap;
use std::env;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arc_swap::ArcSwap;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::api::{CompanionClient, SseEvent, SseStream, Token};
use crate::config::{diff_whitelist, load_token_file, Config, ConfigError, Whitelist};
use crate::peer::{spawn_peer, PeerHandle, PeerSpawn};
use crate::signals::{spawn_signal_task, Signal};

pub struct DaemonArgs {
    pub config_path: PathBuf,
    pub whitelist_override: Option<PathBuf>,
    pub token_override: Option<PathBuf>,
    pub sse_raw_log: Option<PathBuf>,
}

pub async fn run(args: DaemonArgs) -> Result<()> {
    let config = Config::load(&args.config_path)
        .with_context(|| format!("loading config {}", args.config_path.display()))?;

    let whitelist_path = args
        .whitelist_override
        .clone()
        .unwrap_or_else(|| config.paths.whitelist.clone());

    let whitelist = Whitelist::load(&whitelist_path)
        .with_context(|| format!("loading whitelist {}", whitelist_path.display()))?;

    let whitelist = Arc::new(ArcSwap::from_pointee(whitelist));

    let token_raw = load_token(&config, args.token_override.as_deref())?;
    let token = Token::new(token_raw).context("validating token")?;

    tokio::fs::create_dir_all(&config.paths.state_dir).await.ok();

    let client = Arc::new(CompanionClient::new(config.bridge.base_url.clone(), token)?);

    // Resolve identity: either configured UUID or auto-resolve via list_identities().
    let identity_id = resolve_identity(&client, &config).await?;
    info!(%identity_id, name = %config.identity.name, "identity resolved");

    // Global rate limiter.
    let global_period = config.limits.global_period.max(Duration::from_millis(100));
    let global_burst = NonZeroU32::new(config.limits.global_burst.max(1)).unwrap_or(nonzero!(1u32));
    let global_quota = Quota::with_period(global_period)
        .unwrap_or_else(|| Quota::per_second(nonzero!(1u32)))
        .allow_burst(global_burst);
    let global_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>> =
        Arc::new(RateLimiter::direct(global_quota));

    // Peer registry.
    let peers: Arc<RwLock<HashMap<String, PeerHandle>>> = Arc::new(RwLock::new(HashMap::new()));

    // Shutdown coordinator
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // SSE event channel
    let (sse_tx, mut sse_rx) = mpsc::channel::<SseEvent>(256);
    let sse_stream = SseStream::new(client.clone(), identity_id, args.sse_raw_log.clone());
    let sse_shutdown_rx = shutdown_rx.clone();
    let sse_task = tokio::spawn(async move { sse_stream.run(sse_tx, sse_shutdown_rx).await });

    // Signal channel
    let (sig_tx, mut sig_rx) = mpsc::channel::<Signal>(4);
    spawn_signal_task(sig_tx);

    info!("loraterm running; waiting for events");

    loop {
        tokio::select! {
            biased;
            sig = sig_rx.recv() => {
                match sig {
                    Some(Signal::Reload) => {
                        if let Err(e) = reload_whitelist(
                            &whitelist_path,
                            &whitelist,
                            &peers,
                        ).await {
                            warn!(error = %e, "whitelist reload failed; keeping previous");
                        }
                    }
                    Some(Signal::Shutdown) => {
                        info!("shutting down");
                        break;
                    }
                    None => break,
                }
            }
            event = sse_rx.recv() => {
                let Some(event) = event else { break };
                handle_sse_event(
                    event,
                    &whitelist,
                    &peers,
                    &config,
                    identity_id,
                    &client,
                    &global_limiter,
                ).await;
            }
        }
    }

    // Graceful shutdown
    let _ = shutdown_tx.send(true);

    // Drop all peers (their actors will send farewells)
    {
        let mut guard = peers.write().await;
        for (_pk, handle) in guard.iter_mut() {
            handle.shutdown("shutdown");
        }
        guard.clear();
    }

    let _ = tokio::time::timeout(Duration::from_secs(10), sse_task).await;
    Ok(())
}

async fn handle_sse_event(
    event: SseEvent,
    whitelist: &Arc<ArcSwap<Whitelist>>,
    peers: &Arc<RwLock<HashMap<String, PeerHandle>>>,
    config: &Config,
    identity_id: Uuid,
    client: &Arc<CompanionClient>,
    global_limiter: &Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
) {
    let dm = match event {
        SseEvent::DmReceived(dm) => dm,
        SseEvent::SentEcho(_) => return,
        SseEvent::Other { kind, .. } => {
            debug!(%kind, "ignoring non-DM event");
            return;
        }
        SseEvent::Comment(_) => return,
    };

    let Some(text) = dm.text.as_ref() else {
        return;
    };
    let pubkey = dm.peer_pubkey_hex.to_ascii_lowercase();
    if pubkey.len() != 64 || !pubkey.chars().all(|c| c.is_ascii_hexdigit()) {
        debug!(%pubkey, "ignoring DM with invalid pubkey");
        return;
    }

    let wl = whitelist.load_full();
    let Some(peer_cfg) = wl.get(&pubkey) else {
        debug!(peer = %&pubkey[..8.min(pubkey.len())], "dropping DM from non-whitelisted peer");
        return;
    };
    let peer_cfg = peer_cfg.clone();

    // Ensure peer exists and is alive; spawn (or respawn) if not.
    // A `PeerHandle` stays in the registry after its actor exits (idle timeout,
    // PTY EOF, …), so the existence check must also verify the input channel
    // is still open — otherwise the next DM gets silently dropped.
    {
        let mut guard = peers.write().await;
        let stale = guard
            .get(&pubkey)
            .map_or(false, |h| h.input_tx.is_closed());
        if stale {
            info!(peer = %&pubkey[..8], "previous session ended; respawning");
            guard.remove(&pubkey);
        }
        if !guard.contains_key(&pubkey) {
            info!(peer = %&pubkey[..8], "spawning new PTY session");
            let peer_home = config.paths.peer_home_root.join(&pubkey[..16]);
            match spawn_peer(PeerSpawn {
                peer: peer_cfg,
                identity_id,
                client: client.clone(),
                peer_home,
                limits: config.limits.clone(),
                chunking: config.chunking.clone(),
                global_limiter: global_limiter.clone(),
            }) {
                Ok(handle) => {
                    guard.insert(pubkey.clone(), handle);
                }
                Err(e) => {
                    warn!(error = %e, "spawn_peer failed");
                    return;
                }
            }
        }
    }

    // Send the line to the peer's input channel.
    let g = peers.read().await;
    if let Some(handle) = g.get(&pubkey) {
        handle.touch();
        if let Err(e) = handle.input_tx.try_send(text.clone()) {
            warn!(error = %e, peer = %&pubkey[..8], "inbound channel full / closed");
        }
    }
}

async fn resolve_identity(client: &Arc<CompanionClient>, config: &Config) -> Result<Uuid> {
    let identities = client
        .list_identities()
        .await
        .context("listing identities (token valid?)")?;
    if let Some(explicit) = config.identity.id {
        if identities.iter().any(|i| i.id == explicit) {
            return Ok(explicit);
        }
        bail!(
            "configured identity.id {} not visible to this token; saw: {:?}",
            explicit,
            identities.iter().map(|i| (i.id, &i.name)).collect::<Vec<_>>()
        );
    }
    match identities.len() {
        0 => bail!("token-locked identity could not be resolved (empty list)"),
        1 => {
            let i = &identities[0];
            if i.name != config.identity.name {
                warn!(
                    expected = %config.identity.name,
                    actual = %i.name,
                    "identity.name in config doesn't match token-resolved identity (proceeding with token's identity)"
                );
            }
            Ok(i.id)
        }
        _ => bail!(
            "expected exactly one identity for token, got {}; set identity.id explicitly",
            identities.len()
        ),
    }
}

async fn reload_whitelist(
    path: &std::path::Path,
    current: &Arc<ArcSwap<Whitelist>>,
    peers: &Arc<RwLock<HashMap<String, PeerHandle>>>,
) -> Result<(), ConfigError> {
    let new = Whitelist::load(path)?;
    let old = current.load_full();
    let diff = diff_whitelist(&old, &new);
    info!(
        added = diff.added.len(),
        removed = diff.removed.len(),
        changed = diff.changed.len(),
        "whitelist reloaded"
    );
    current.store(Arc::new(new));
    let mut guard = peers.write().await;
    for pk in &diff.removed {
        if let Some(handle) = guard.get_mut(pk) {
            handle.shutdown("revoked");
        }
        guard.remove(pk);
    }
    Ok(())
}

fn load_token(config: &Config, override_path: Option<&std::path::Path>) -> Result<String> {
    // 1. systemd LoadCredential
    if let Ok(dir) = env::var("CREDENTIALS_DIRECTORY") {
        let path = std::path::Path::new(&dir).join("loraterm-token");
        if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            return Ok(raw.trim().to_owned());
        }
    }
    // 2. CLI override
    if let Some(p) = override_path {
        return Ok(load_token_file(p)?);
    }
    // 3. Env var
    if let Ok(v) = env::var("LORATERM_TOKEN") {
        let v = v.trim().to_string();
        if !v.is_empty() {
            return Ok(v);
        }
    }
    // 4. config-file fallback
    if let Some(p) = &config.paths.token_file {
        return Ok(load_token_file(p)?);
    }
    bail!(
        "no token configured (try one of: systemd LoadCredential=loraterm-token, \
         --token-file, $LORATERM_TOKEN, paths.token_file)"
    );
}
