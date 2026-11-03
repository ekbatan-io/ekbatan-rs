//! `AsyncKeyedLockProvider` and `AsyncLease`: keyed locks waited on with `.await`.
//! `KeyedLockProvider` and `Lease` are their sync twins; the providers serve both.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::OwnedMutexGuard;
use tracing::warn;

use crate::keyed_lock::mysql_name;
use crate::{Connection, Dialect, Error};

/// A keyed lock, waited on with `.await`: the async twin of `KeyedLockProvider`.
pub trait AsyncKeyedLockProvider: Send + Sync {
    /// Waits until the key is free, then holds it until the lease is released, dropped, or
    /// `max_hold` passes.
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<AsyncLease, Error>>;

    /// Waits up to `max_wait` (zero: don't wait). `None` if the key stayed held. Contention is
    /// never an error; a failing backend is.
    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        max_wait: Duration,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<Option<AsyncLease>, Error>>;
}

/// What a lease releases.
pub(crate) enum Held {
    InProcess(OwnedMutexGuard<()>),
    Sql {
        connection: Connection,
        dialect: Dialect,
        hashed: i64,
        dirty: bool,
    },
}

struct LeaseState {
    key: String,
    held: tokio::sync::Mutex<Option<Held>>,
    watchdog: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// A held lock, the async twin of `Lease`. `release().await` frees it; dropping it frees it in
/// the background.
pub struct AsyncLease {
    state: Arc<LeaseState>,
}

impl AsyncLease {
    pub(crate) fn new(key: &str, held: Held, max_hold: Duration) -> AsyncLease {
        let state = Arc::new(LeaseState {
            key: key.to_owned(),
            held: tokio::sync::Mutex::new(Some(held)),
            watchdog: Mutex::new(None),
        });
        let watched = Arc::downgrade(&state);
        let watchdog = tokio::spawn(async move {
            tokio::time::sleep(max_hold).await;
            if let Some(state) = watched.upgrade()
                && release(&state).await
            {
                warn!(
                    "Lock '{}' force-released: held longer than its max hold",
                    state.key
                );
            }
        });
        *state.watchdog.lock().unwrap_or_else(|p| p.into_inner()) = Some(watchdog);
        AsyncLease { state }
    }

    pub fn key(&self) -> &str {
        &self.state.key
    }

    /// Whether this lease still holds the lock: false after `release()` or once `max_hold`
    /// passed.
    pub async fn is_held(&self) -> bool {
        self.state.held.lock().await.is_some()
    }

    /// Releases the lock. Releasing twice is harmless.
    pub async fn release(self) {
        release(&self.state).await;
    }
}

impl Drop for AsyncLease {
    fn drop(&mut self) {
        let state = self.state.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                release(&state).await;
            });
        }
        // Without a runtime the lock can't be released by SQL; the connection drops with the
        // lease and the database frees its session lock when it closes.
    }
}

/// Frees the lock if still held. True if this call freed it.
async fn release(state: &LeaseState) -> bool {
    let Some(held) = state.held.lock().await.take() else {
        return false;
    };
    if let Some(watchdog) = state
        .watchdog
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take()
        && tokio::task::try_id() != Some(watchdog.id())
    {
        watchdog.abort();
    }
    match held {
        Held::InProcess(guard) => drop(guard),
        Held::Sql {
            mut connection,
            dialect,
            hashed,
            dirty,
        } => {
            let sql = match dialect {
                Dialect::Postgres => format!("SELECT pg_advisory_unlock({hashed})::int::int8"),
                Dialect::MySql | Dialect::MariaDb => {
                    format!("SELECT RELEASE_LOCK('{}')", mysql_name(hashed))
                }
            };
            match connection.scalar_generated(sql).await {
                Ok(Some(1)) if !dirty => {}
                Ok(_) | Err(_) => {
                    // Not released cleanly, or the session is left changed: close it, so the
                    // database drops whatever it still holds.
                    connection.evict();
                }
            }
        }
    }
    true
}
