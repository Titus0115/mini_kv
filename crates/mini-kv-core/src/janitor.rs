use std::sync::Arc;
use std::time::Duration;

use crate::engine::WalEngine;
use crate::error::Result;

pub struct Janitor {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl Janitor {
    pub fn start(engine: Arc<WalEngine>, interval: Duration) -> Self {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        let purged = engine.purge_expired();
                        if purged > 0 {
                            log::debug!("janitor purged {purged} expired keys");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        log::info!("janitor shutting down");
                        break;
                    }
                }
            }
        });

        Self {
            handle: Some(handle),
            shutdown_tx,
        }
    }

    pub fn shutdown(mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        Ok(())
    }
}
