//! `SqlKeyedLockProvider`: keyed locks across instances, through the database
//! (`pg_advisory_lock` on PostgreSQL, `GET_LOCK` on MySQL and MariaDB).

use std::time::Duration;

use futures_util::future::BoxFuture;

use super::aio::Held;
use super::{lock_key_hash, mysql_name};
use crate::aio::{AsyncKeyedLockProvider, AsyncLease};
use crate::{Connection, ConnectionProvider, Dialect, Error};

/// How long one SQL wait lasts before it is asked again: Java's five-second segments.
const SEGMENT: Duration = Duration::from_secs(5);

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
