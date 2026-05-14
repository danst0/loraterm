//! Per-peer actor: owns one PTY, one inbound DM queue (→ PTY stdin), and one outbound
//! pipeline (PTY stdout → chunker → rate-limiter → POST /messages/dm).

use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tracing::{debug, info, warn, Instrument};
use uuid::Uuid;

use crate::api::CompanionClient;
use crate::chunker::{Chunker, ChunkerOpts};
use crate::config::{ChunkingConfig, LimitsConfig, ValidatedPeer};
use crate::pty;

pub struct PeerHandle {
    pub input_tx: mpsc::Sender<String>,
    pub shutdown_tx: Option<oneshot::Sender<&'static str>>,
    pub last_activity: Arc<AtomicI64>,
}

impl PeerHandle {
    pub fn touch(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.last_activity.store(now, Ordering::Relaxed);
    }

    pub fn last_activity_secs(&self) -> i64 {
        self.last_activity.load(Ordering::Relaxed)
    }

    pub fn shutdown(&mut self, reason: &'static str) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(reason);
        }
    }
}

pub struct PeerSpawn {
    pub peer: ValidatedPeer,
    pub identity_id: Uuid,
    pub client: Arc<CompanionClient>,
    pub peer_home: PathBuf,
    pub limits: LimitsConfig,
    pub chunking: ChunkingConfig,
    pub global_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

pub fn spawn_peer(spawn: PeerSpawn) -> anyhow::Result<PeerHandle> {
    let short = &spawn.peer.pubkey[..8];
    let span = tracing::info_span!("peer", id = %short);
    let _e = span.enter();

    let pty = pty::spawn_bash(&spawn.peer_home)?;

    let (input_tx, input_rx) = mpsc::channel::<String>(spawn.limits.inbound_channel_depth);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<&'static str>();
    let last_activity = Arc::new(AtomicI64::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64,
    ));

    let actor = PeerActor {
        peer: spawn.peer.clone(),
        identity_id: spawn.identity_id,
        client: spawn.client.clone(),
        limits: spawn.limits.clone(),
        chunking: spawn.chunking.clone(),
        global_limiter: spawn.global_limiter,
        last_activity: last_activity.clone(),
    };

    tokio::spawn(actor.run(pty, input_rx, shutdown_rx).instrument(tracing::info_span!(
        "peer",
        id = %short.to_owned()
    )));

    Ok(PeerHandle {
        input_tx,
        shutdown_tx: Some(shutdown_tx),
        last_activity,
    })
}

struct PeerActor {
    peer: ValidatedPeer,
    identity_id: Uuid,
    client: Arc<CompanionClient>,
    limits: LimitsConfig,
    chunking: ChunkingConfig,
    global_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    last_activity: Arc<AtomicI64>,
}

impl PeerActor {
    async fn run(
        self,
        mut pty: pty::PtySession,
        mut input_rx: mpsc::Receiver<String>,
        mut shutdown_rx: oneshot::Receiver<&'static str>,
    ) {
        let max_chars = self
            .peer
            .dm_max_chars
            .unwrap_or(self.chunking.dm_max_chars);
        let chunker_opts = ChunkerOpts {
            max_chars,
            number_chunks: self.chunking.number_chunks,
            flush_silence: self.chunking.flush_silence,
            burst_chunk_cap: self.limits.burst_chunk_cap,
        };
        let mut chunker = Chunker::new(chunker_opts);

        let idle_timeout = self
            .peer
            .idle_timeout
            .unwrap_or(self.limits.idle_timeout);

        // Per-peer rate limiter
        let per_peer_period = self.limits.per_peer_period.max(Duration::from_millis(100));
        let per_peer_burst = NonZeroU32::new(self.limits.per_peer_burst.max(1)).unwrap_or(nonzero!(1u32));
        let quota = Quota::with_period(per_peer_period)
            .unwrap_or_else(|| Quota::per_second(nonzero!(1u32)))
            .allow_burst(per_peer_burst);
        let per_peer_limiter = Arc::new(RateLimiter::direct(quota));

        // Outbound chunk queue → sender task
        let (chunk_tx, chunk_rx) =
            mpsc::channel::<String>(self.limits.outbound_channel_depth);

        // Spawn the sender task
        let sender = SenderTask {
            client: self.client.clone(),
            identity_id: self.identity_id,
            peer_pubkey: self.peer.pubkey.clone(),
            per_peer_limiter: per_peer_limiter.clone(),
            global_limiter: self.global_limiter.clone(),
        };
        let sender_handle = tokio::spawn(sender.run(chunk_rx));

        let mut idle_tick = tokio::time::interval(Duration::from_secs(60));
        idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let shutdown_reason: &'static str = loop {
            // Compute next chunker deadline (if any) for the silence-flush.
            let next_chunker_deadline = chunker.next_deadline();
            let silence_sleep: tokio::time::Sleep = match next_chunker_deadline {
                Some(t) => tokio::time::sleep_until(t.into()),
                None => tokio::time::sleep(Duration::from_secs(3600)),
            };
            tokio::pin!(silence_sleep);

            tokio::select! {
                biased;
                reason = &mut shutdown_rx => {
                    break reason.unwrap_or("shutdown");
                }
                _ = idle_tick.tick() => {
                    let last = self.last_activity.load(Ordering::Relaxed);
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    if now - last > idle_timeout.as_secs() as i64 {
                        info!("idle timeout exceeded; closing peer session");
                        break "idle";
                    }
                }
                _ = &mut silence_sleep => {
                    let chunks = chunker.maybe_flush(Instant::now());
                    for c in chunks {
                        enqueue_chunk(&chunk_tx, c).await;
                    }
                }
                line = input_rx.recv() => {
                    let Some(line) = line else { break "input_closed"; };
                    self.touch();
                    let mut payload = line.into_bytes();
                    if !payload.ends_with(b"\n") {
                        payload.push(b'\n');
                    }
                    if pty.input_tx.send(payload).await.is_err() {
                        warn!("pty input channel closed");
                        break "pty_closed";
                    }
                }
                output = pty.output_rx.recv() => {
                    match output {
                        None => break "pty_eof",
                        Some(bytes) => {
                            self.touch();
                            let chunks = chunker.push(&bytes);
                            for c in chunks {
                                enqueue_chunk(&chunk_tx, c).await;
                            }
                        }
                    }
                }
            }
        };

        // Drain remaining chunker output (best-effort)
        for c in chunker.flush_force() {
            let _ = chunk_tx.try_send(c);
        }
        // Polite farewell
        let farewell = format!("session closed: {shutdown_reason}");
        let _ = chunk_tx.try_send(farewell);

        drop(chunk_tx);
        let _ = sender_handle.await;

        // Kill the child
        {
            let mut child = pty.child.lock().await;
            // Drop input → EOF
            drop(pty.input_tx);
            // Try graceful exit
            let _ = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    match child.try_wait() {
                        Ok(Some(_)) => break,
                        Ok(None) => sleep(Duration::from_millis(100)).await,
                        Err(_) => break,
                    }
                }
            })
            .await;
            if matches!(child.try_wait(), Ok(None)) {
                let _ = child.kill();
            }
        }

        info!(reason = shutdown_reason, "peer session ended");
    }

    fn touch(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.last_activity.store(now, Ordering::Relaxed);
    }
}

async fn enqueue_chunk(tx: &mpsc::Sender<String>, chunk: String) {
    match tx.try_send(chunk) {
        Ok(_) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Drain + replace with marker
            let mut dropped = 1usize;
            while tx.try_send("[output truncated — sender backlog]".to_string()).is_err() {
                dropped += 1;
                if dropped > 5 {
                    break;
                }
            }
            debug!(dropped, "outbound queue full; dropped chunks");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

struct SenderTask {
    client: Arc<CompanionClient>,
    identity_id: Uuid,
    peer_pubkey: String,
    per_peer_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    global_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl SenderTask {
    async fn run(self, mut rx: mpsc::Receiver<String>) {
        while let Some(chunk) = rx.recv().await {
            self.per_peer_limiter.until_ready().await;
            self.global_limiter.until_ready().await;
            match self
                .client
                .send_dm(self.identity_id, &self.peer_pubkey, &chunk)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!(error = %e, "send_dm failed; chunk dropped");
                    // No retry — bridge handles ack/forward and we don't want to flood.
                }
            }
        }
    }
}
