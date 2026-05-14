//! PTY spawning + IO bridging.
//!
//! `portable-pty` exposes a blocking `Read`/`Write` master. We bridge those to async
//! mpsc channels via `spawn_blocking` threads — one reader thread and one writer
//! thread per PTY. That's fine at our scale (≤ a few dozen peers).

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, warn};

pub struct PtySession {
    /// Returns bytes read from the PTY master. EOF when the child exits.
    pub output_rx: mpsc::Receiver<Vec<u8>>,
    /// Send raw bytes to be written to the PTY stdin (caller responsible for newlines).
    pub input_tx: mpsc::Sender<Vec<u8>>,
    /// Used to terminate the child on shutdown.
    pub child: Arc<Mutex<Box<dyn Child + Send + Sync>>>,
    /// Holds the master fd alive for the lifetime of the session.
    /// Dropping it closes the PTY which signals EOF to bash.
    _master: Box<dyn MasterPty + Send>,
}

pub fn spawn_bash(home: &Path) -> Result<PtySession> {
    std::fs::create_dir_all(home)
        .with_context(|| format!("creating peer home {}", home.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(home, std::fs::Permissions::from_mode(0o700));
    }

    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("openpty")?;

    let mut cmd = CommandBuilder::new("/bin/bash");
    cmd.arg("-i");
    cmd.env_clear();
    cmd.env("TERM", "dumb");
    cmd.env("PS1", "$ ");
    cmd.env("HOME", home);
    cmd.env("PATH", "/usr/local/bin:/usr/bin:/bin");
    cmd.cwd(home);

    let child = pair.slave.spawn_command(cmd).context("spawn bash")?;
    let child = Arc::new(Mutex::new(child));
    drop(pair.slave);

    let mut master_reader = pair.master.try_clone_reader().context("clone reader")?;
    let mut master_writer = pair.master.take_writer().context("take writer")?;

    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>(256);
    let (in_tx, mut in_rx) = mpsc::channel::<Vec<u8>>(64);

    // Reader thread: blocking read → async channel
    let out_tx_thread = out_tx.clone();
    tokio::task::spawn_blocking(move || {
        let mut buf = [0u8; 4096];
        loop {
            match master_reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if out_tx_thread.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    debug!(error = %e, "pty reader err");
                    break;
                }
            }
        }
        debug!("pty reader thread exiting");
    });

    // Writer thread: drain async channel → blocking write
    tokio::task::spawn_blocking(move || {
        while let Some(bytes) = in_rx.blocking_recv() {
            if let Err(e) = master_writer.write_all(&bytes) {
                warn!(error = %e, "pty write failed");
                break;
            }
            if let Err(e) = master_writer.flush() {
                warn!(error = %e, "pty flush failed");
                break;
            }
        }
        debug!("pty writer thread exiting");
    });

    Ok(PtySession {
        output_rx: out_rx,
        input_tx: in_tx,
        child,
        _master: pair.master,
    })
}
