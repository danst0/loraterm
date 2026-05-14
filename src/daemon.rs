//! Top-level orchestrator. Builds the API client, bootstraps the dedicated identity,
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

use crate::api::{CompanionClient, Credentials, SseEvent, SseStream};
use crate::config::{diff_whitelist, load_credentials_file, Config, ConfigError, Whitelist};
use crate::peer::{spawn_peer, PeerHandle, PeerSpawn};
use crate::signals::{spawn_signal_task, Signal};

pub struct DaemonArgs {
    pub config_path: PathBuf,
    pub whitelist_override: Option<PathBuf>,
    pub credentials_override: Option<PathBuf>,
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

    let credentials = load_credentials(&config, args.credentials_override.as_deref())?;

    let cookie_path = config.paths.state_dir.join("cookies.json");
    let state_path = config.paths.state_dir.join("state.json");
    tokio::fs::create_dir_all(&config.paths.state_dir).await.ok();

    let client = Arc::new(CompanionClient::new(
        config.bridge.base_url.clone(),
        cookie_path,
        credentials,
    )?);

    // Authenticate + bootstrap identity.
    client
        .ensure_session()
        .await
        .context("initial bridge session")?;
    let identity_id = bootstrap_identity(&client, &config, &state_path).await?;
    info!(%identity_id, "identity bootstrapped");

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

    // Cookie-persistence ticker
    let cookie_client = client.clone();
    let cookie_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = interval.tick() => cookie_client.persist_cookies().await,
                changed = wait_shutdown(&cookie_shutdown) => {
                    if changed { return; }
                }
            }
        }
    });

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
    client.persist_cookies().await;
    Ok(())
}

async fn wait_shutdown(rx: &watch::Receiver<bool>) -> bool {
    let mut rx = rx.clone();
    let _ = rx.changed().await;
    let v = *rx.borrow();
    v
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

    // Ensure peer exists; spawn if not.
    let already_has = {
        let g = peers.read().await;
        g.contains_key(&pubkey)
    };
    if !already_has {
        info!(peer = %&pubkey[..8], "spawning new PTY session");
        let peer_home = config
            .paths
            .peer_home_root
            .join(&pubkey[..16]);
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
                peers.write().await.insert(pubkey.clone(), handle);
            }
            Err(e) => {
                warn!(error = %e, "spawn_peer failed");
                return;
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

async fn bootstrap_identity(
    client: &Arc<CompanionClient>,
    config: &Config,
    state_path: &std::path::Path,
) -> Result<Uuid> {
    let identities = client
        .list_identities()
        .await
        .context("listing identities")?;
    let existing = identities.iter().find(|i| i.name == config.identity.name);
    let id = if let Some(i) = existing {
        info!(id = %i.id, name = %i.name, "using existing identity");
        i.id
    } else {
        let created = client
            .create_identity(&config.identity.name, &config.identity.scope)
            .await
            .context("creating identity")?;
        info!(id = %created.id, name = %created.name, "created identity");
        if config.identity.advert_on_bootstrap {
            if let Err(e) = client.advert(created.id).await {
                warn!(error = %e, "initial advert failed (non-fatal)");
            }
        }
        created.id
    };

    let _ = tokio::fs::write(
        state_path,
        serde_json::to_vec_pretty(&serde_json::json!({"identity_id": id})).unwrap_or_default(),
    )
    .await;

    Ok(id)
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

fn load_credentials(
    config: &Config,
    override_path: Option<&std::path::Path>,
) -> Result<Credentials> {
    // 1. systemd LoadCredential
    if let Ok(dir) = env::var("CREDENTIALS_DIRECTORY") {
        let path = std::path::Path::new(&dir).join("loraterm-creds");
        if path.exists() {
            let raw = std::fs::read_to_string(&path)?;
            return parse_inline_credentials(&raw)
                .with_context(|| format!("parsing {}", path.display()));
        }
    }
    // 2. CLI override
    if let Some(p) = override_path {
        let cred = load_credentials_file(p)?;
        return Ok(Credentials {
            email: cred.email,
            password: cred.password,
        });
    }
    // 3. config-file fallback
    if let Some(p) = &config.paths.credentials_file {
        let cred = load_credentials_file(p)?;
        return Ok(Credentials {
            email: cred.email,
            password: cred.password,
        });
    }
    bail!("no credentials configured (set CREDENTIALS_DIRECTORY or paths.credentials_file)");
}

fn parse_inline_credentials(raw: &str) -> Result<Credentials> {
    let mut lines = raw.lines().filter(|l| !l.trim().is_empty());
    let email = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty credentials file"))?
        .trim()
        .to_string();
    let password = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing password line"))?
        .trim()
        .to_string();
    Ok(Credentials { email, password })
}
