//! Signal handling: SIGHUP triggers a whitelist reload; SIGTERM/SIGINT trigger shutdown.

use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tracing::info;

#[derive(Debug, Clone, Copy)]
pub enum Signal {
    Reload,
    Shutdown,
}

pub fn spawn_signal_task(tx: mpsc::Sender<Signal>) {
    tokio::spawn(async move {
        let mut hup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "cannot install SIGHUP handler");
                return;
            }
        };
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "cannot install SIGTERM handler");
                return;
            }
        };
        let mut int_sig = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "cannot install SIGINT handler");
                return;
            }
        };

        loop {
            tokio::select! {
                _ = hup.recv() => {
                    info!("SIGHUP received");
                    if tx.send(Signal::Reload).await.is_err() { return; }
                }
                _ = term.recv() => {
                    info!("SIGTERM received");
                    let _ = tx.send(Signal::Shutdown).await;
                    return;
                }
                _ = int_sig.recv() => {
                    info!("SIGINT received");
                    let _ = tx.send(Signal::Shutdown).await;
                    return;
                }
            }
        }
    });
}
