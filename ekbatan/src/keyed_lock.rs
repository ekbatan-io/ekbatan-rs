//! Keyed locks: Java's `io.ekbatan.core.concurrent`. Mutual exclusion on a text key: different
//! keys are independent, the same key serializes. Derive keys per type (`"wallet:" + id`).
//!
//! - `InProcessKeyedLockProvider`: within one process.
//! - `PostgresKeyedLockProvider`: `pg_advisory_lock`, across instances.
//! - `MySqlKeyedLockProvider` / `MariaDbKeyedLockProvider`: `GET_LOCK`, across instances.
//!
//! Every lease has a `max_hold`: when it passes, the lock is released even if the holder never
//! released it - a safety net against a holder that hangs or forgets.
//!
//! Differences from Java: **not re-entrant** - Java's leases are re-entrant per thread, and an
//! async task has no thread of its own; acquiring a key the same task already holds waits for it
//! like any other caller. And a SQL acquire dropped part-way closes its connection rather than
//! returning it to the pool, so a lock granted late can't leak into the pool.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use sha2::{Digest, Sha256};
use tokio::sync::OwnedMutexGuard;
use tracing::warn;

use crate::{Connection, ConnectionProvider, Dialect, Error};

/// How long one SQL wait lasts before it is asked again: Java's five-second segments.
pub use crate::sync_api::keyed_lock::{KeyedLockProvider, Lease};

const SEGMENT: Duration = Duration::from_secs(5);

/// A keyed lock: Java's `KeyedLockProvider`.
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

/// Java's `LockKeyHash`: the first 8 bytes of the key's SHA-256, as a signed 64-bit number.
pub fn lock_key_hash(key: &str) -> i64 {
    let digest = Sha256::digest(key.as_bytes());
    i64::from_be_bytes(digest[..8].try_into().expect("8 bytes"))
}

/// What a lease releases.
enum Held {
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

/// A held lock: Java's `Lease`. `release()` frees it; dropping it frees it in the background.
pub struct AsyncLease {
    state: Arc<LeaseState>,
}

impl AsyncLease {
    fn new(key: &str, held: Held, max_hold: Duration) -> AsyncLease {
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

/// MySQL's lock name: Java's hex of the hash (lock names are limited to 64 characters).
fn mysql_name(hashed: i64) -> String {
    format!("{:x}", hashed as u64)
}

/// Within one process: Java's in-process `KeyedLockProvider`. FIFO per key.
#[derive(Default)]
pub struct InProcessKeyedLockProvider {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl InProcessKeyedLockProvider {
    pub fn new() -> Self {
        InProcessKeyedLockProvider::default()
    }

    fn mutex(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
        // Forget keys no one holds or waits for, so the map doesn't grow without end.
        locks.retain(|_, m| Arc::strong_count(m) > 1 || m.try_lock().is_err());
        locks.entry(key.to_owned()).or_default().clone()
    }
}

impl AsyncKeyedLockProvider for InProcessKeyedLockProvider {
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<AsyncLease, Error>> {
        Box::pin(async move {
            let guard = self.mutex(key).lock_owned().await;
            Ok(AsyncLease::new(key, Held::InProcess(guard), max_hold))
        })
    }

    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        max_wait: Duration,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<Option<AsyncLease>, Error>> {
        Box::pin(async move {
            let mutex = self.mutex(key);
            let guard = if max_wait.is_zero() {
                mutex.try_lock_owned().ok()
            } else {
                tokio::time::timeout(max_wait, mutex.lock_owned())
                    .await
                    .ok()
            };
            Ok(guard.map(|g| AsyncLease::new(key, Held::InProcess(g), max_hold)))
        })
    }
}

/// Across instances, through the database: each held lock keeps one connection of `provider`
/// (Java's `lockConfig` pool), so size it for the locks held at once.
pub struct SqlKeyedLockProvider {
    provider: ConnectionProvider,
}

/// Java's `PostgresKeyedLockProvider`: `pg_advisory_lock`.
pub type PostgresKeyedLockProvider = SqlKeyedLockProvider;
/// Java's `MySQLKeyedLockProvider`: `GET_LOCK`.
pub type MySqlKeyedLockProvider = SqlKeyedLockProvider;
/// Java's `MariaDBKeyedLockProvider`: `GET_LOCK`.
pub type MariaDbKeyedLockProvider = SqlKeyedLockProvider;

/// Closes the connection if the acquire is dropped before it hands the connection on.
struct Pending {
    connection: Option<Connection>,
}

impl Pending {
    fn connection(&mut self) -> &mut Connection {
        self.connection.as_mut().expect("taken only at the end")
    }

    fn take(mut self) -> Connection {
        self.connection.take().expect("taken once")
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            connection.evict();
        }
    }
}

enum Attempt {
    Acquired { dirty: bool },
    TimedOut,
}

impl SqlKeyedLockProvider {
    pub fn new(provider: ConnectionProvider) -> Self {
        SqlKeyedLockProvider { provider }
    }

    async fn attempt(
        connection: &mut Connection,
        hashed: i64,
        wait: Duration,
    ) -> Result<Attempt, Error> {
        match connection.dialect() {
            Dialect::Postgres => {
                if wait.is_zero() {
                    let got = connection
                        .scalar_generated(format!(
                            "SELECT pg_try_advisory_lock({hashed})::int::int8"
                        ))
                        .await?;
                    return Ok(if got == Some(1) {
                        Attempt::Acquired { dirty: false }
                    } else {
                        Attempt::TimedOut
                    });
                }
                let millis = wait.as_millis().max(1);
                connection
                    .run_generated(format!("SET lock_timeout = {millis}"))
                    .await?;
                let outcome = connection
                    .run_generated(format!("SELECT pg_advisory_lock({hashed})"))
                    .await;
                let dirty = connection
                    .run_generated("SET lock_timeout = 0".to_owned())
                    .await
                    .is_err();
                match outcome {
                    Ok(()) => Ok(Attempt::Acquired { dirty }),
                    Err(Error::Database(sqlx::Error::Database(e)))
                        if e.code().as_deref() == Some("55P03") =>
                    {
                        Ok(Attempt::TimedOut)
                    }
                    Err(e) => Err(e),
                }
            }
            Dialect::MySql | Dialect::MariaDb => {
                // GET_LOCK takes seconds and rounds up to whole ones.
                let seconds = if wait.is_zero() {
                    0.0
                } else {
                    wait.as_millis().max(1) as f64 / 1000.0
                };
                let got = connection
                    .scalar_generated(format!(
                        "SELECT GET_LOCK('{}', {seconds})",
                        mysql_name(hashed)
                    ))
                    .await?;
                match got {
                    Some(1) => Ok(Attempt::Acquired { dirty: false }),
                    Some(_) => Ok(Attempt::TimedOut),
                    None => Err(Error::Unexpected(
                        "GET_LOCK returned NULL (server-side error)".to_owned(),
                    )),
                }
            }
        }
    }

    async fn lock(
        &self,
        key: &str,
        max_wait: Option<Duration>,
        max_hold: Duration,
    ) -> Result<Option<AsyncLease>, Error> {
        let failed =
            |e: Error| Error::Unexpected(format!("lock '{key}' could not be acquired: {e}"));
        let mut pending = Pending {
            connection: Some(self.provider.acquire().await.map_err(failed)?),
        };
        let hashed = lock_key_hash(key);
        let deadline = max_wait.map(|w| tokio::time::Instant::now() + w);
        loop {
            let wait = match deadline {
                Some(deadline) => deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .min(SEGMENT),
                None => SEGMENT,
            };
            match Self::attempt(pending.connection(), hashed, wait)
                .await
                .map_err(failed)?
            {
                Attempt::Acquired { dirty } => {
                    let dialect = pending.connection().dialect();
                    let held = Held::Sql {
                        connection: pending.take(),
                        dialect,
                        hashed,
                        dirty,
                    };
                    return Ok(Some(AsyncLease::new(key, held, max_hold)));
                }
                Attempt::TimedOut => {
                    if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                        // Nothing held: the connection goes back to the pool.
                        drop(pending.take());
                        return Ok(None);
                    }
                }
            }
        }
    }
}

impl AsyncKeyedLockProvider for SqlKeyedLockProvider {
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<AsyncLease, Error>> {
        Box::pin(async move {
            Ok(self
                .lock(key, None, max_hold)
                .await?
                .expect("an unbounded wait ends acquired"))
        })
    }

    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        max_wait: Duration,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<Option<AsyncLease>, Error>> {
        Box::pin(self.lock(key, Some(max_wait), max_hold))
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncKeyedLockProvider, InProcessKeyedLockProvider, lock_key_hash, mysql_name};
    use sha2::{Digest, Sha256};
    use std::time::Duration;

    #[test]
    fn keys_hash_as_javas_lock_key_hash() {
        // SHA-256("wallet:1") starts 0x3f6f...; the first 8 bytes, big-endian, signed.
        let digest = Sha256::digest(b"wallet:1");
        let expected = i64::from_be_bytes(digest[..8].try_into().unwrap());
        assert_eq!(lock_key_hash("wallet:1"), expected);
        assert_eq!(mysql_name(-1), "ffffffffffffffff");
    }

    #[tokio::test]
    async fn in_process_locks_serialize_one_key_and_free_on_max_hold() {
        let locks = InProcessKeyedLockProvider::new();
        let held = locks.acquire("a", Duration::from_secs(60)).await.unwrap();
        assert!(
            locks
                .try_acquire("a", Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            locks
                .try_acquire("b", Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap()
                .is_some()
        );
        held.release().await;
        let short = locks.acquire("a", Duration::from_millis(50)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!short.is_held().await);
        assert!(
            locks
                .try_acquire("a", Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap()
                .is_some()
        );
    }
}
