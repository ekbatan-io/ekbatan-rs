use std::fmt;
use std::str::FromStr;

use sqlx::mysql::{MySql, MySqlConnectOptions, MySqlPool};
use sqlx::pool::{PoolConnection, PoolOptions};
use sqlx::postgres::{PgConnectOptions, PgPool, Postgres};

use crate::{DataSourceConfig, Db, Dialect, Error};

/// A pool of connections to one database: Java's `ConnectionProvider` (a HikariCP pool there, a
/// sqlx pool here). Cloning it is cheap and shares the same pool.
#[derive(Clone)]
pub struct ConnectionProvider {
    pool: Pool,
    dialect: Dialect,
}

#[derive(Clone)]
pub(crate) enum Pool {
    Postgres(PgPool),
    /// MySQL and MariaDB both use sqlx's MySQL driver.
    MySql(MySqlPool),
}

impl ConnectionProvider {
    /// Java's `hikariConnectionProvider(config)`. Opens no connection yet, the first use does,
    /// so an app can start while a database is still coming up (Java sets Hikari's
    /// `initializationFailTimeout` to -1 for the same reason).
    pub fn from_config(config: &DataSourceConfig) -> Result<Self, Error> {
        if config.maximum_pool_size == 0 {
            return Err(Error::Config(
                "maximum_pool_size must be at least 1".to_owned(),
            ));
        }
        if let Some(minimum) = config.minimum_idle
            && minimum > config.maximum_pool_size
        {
            return Err(Error::Config(format!(
                "minimum_idle ({minimum}) can't exceed maximum_pool_size ({})",
                config.maximum_pool_size
            )));
        }
        let pool = match config.dialect {
            Dialect::Postgres => {
                let mut options = PgConnectOptions::from_str(&config.url)?;
                if let Some(username) = &config.username {
                    options = options.username(username);
                }
                if let Some(password) = &config.password {
                    options = options.password(password);
                }
                Pool::Postgres(pool_options(config).connect_lazy_with(options))
            }
            Dialect::MySql | Dialect::MariaDb => {
                let mut options = MySqlConnectOptions::from_str(&config.url)?;
                if let Some(username) = &config.username {
                    options = options.username(username);
                }
                if let Some(password) = &config.password {
                    options = options.password(password);
                }
                Pool::MySql(pool_options(config).connect_lazy_with(options))
            }
        };
        Ok(ConnectionProvider {
            pool,
            dialect: config.dialect,
        })
    }

    pub(crate) fn pool(&self) -> &Pool {
        &self.pool
    }

    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// Takes one connection and keeps it until it is dropped, which returns it to the pool, or
    /// evicted: Java's `acquire()` and `release()`. For work that must stay on one connection,
    /// such as a session lock. Single statements use `db()` instead.
    pub async fn acquire(&self) -> Result<Connection, Error> {
        let inner = match &self.pool {
            Pool::Postgres(pool) => PooledConnection::Postgres(pool.acquire().await?),
            Pool::MySql(pool) => PooledConnection::MySql(pool.acquire().await?),
        };
        Ok(Connection {
            inner,
            dialect: self.dialect,
        })
    }

    /// Statements that each borrow a connection only while they run: what Java's `DSLContext`
    /// over the data source does.
    pub fn db(&self) -> Db<'_> {
        Db::on_pool(&self.pool, self.dialect)
    }

    /// How many connections are open now.
    pub fn size(&self) -> u32 {
        match &self.pool {
            Pool::Postgres(pool) => pool.size(),
            Pool::MySql(pool) => pool.size(),
        }
    }

    /// How many of the open connections are waiting to be lent.
    pub fn idle(&self) -> usize {
        match &self.pool {
            Pool::Postgres(pool) => pool.num_idle(),
            Pool::MySql(pool) => pool.num_idle(),
        }
    }

    /// Closes every connection; nothing can be borrowed after this.
    pub async fn close(&self) {
        match &self.pool {
            Pool::Postgres(pool) => pool.close().await,
            Pool::MySql(pool) => pool.close().await,
        }
    }
}

/// Shows the pool's state, never its login details.
impl fmt::Debug for ConnectionProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionProvider")
            .field("dialect", &self.dialect)
            .field("size", &self.size())
            .field("idle", &self.idle())
            .finish()
    }
}

fn pool_options<DB: sqlx::Database>(config: &DataSourceConfig) -> PoolOptions<DB> {
    let mut options = PoolOptions::new().max_connections(config.maximum_pool_size);
    if let Some(minimum) = config.minimum_idle {
        options = options.min_connections(minimum);
    }
    if let Some(idle) = config.idle_timeout {
        options = options.idle_timeout(idle);
    }
    if let Some(wait) = config.connection_timeout {
        options = options.acquire_timeout(wait);
    }
    options
}

/// One connection taken from a pool and held: the JDBC `Connection` Java's
/// `ConnectionProvider.acquire()` returns. Dropping it gives it back to the pool; `evict`
/// closes it instead.
pub struct Connection {
    inner: PooledConnection,
    dialect: Dialect,
}

pub(crate) enum PooledConnection {
    Postgres(PoolConnection<Postgres>),
    MySql(PoolConnection<MySql>),
}

impl Connection {
    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// Statements on this one connection.
    pub fn db(&mut self) -> Db<'_> {
        Db::on_connection(&mut self.inner, self.dialect)
    }

    /// Closes the connection instead of returning it to the pool: Java's `evict(connection)`.
    /// For a connection whose session state is in doubt.
    pub fn evict(mut self) {
        self.close_on_drop();
    }

    pub(crate) fn close_on_drop(&mut self) {
        match &mut self.inner {
            PooledConnection::Postgres(connection) => connection.close_on_drop(),
            PooledConnection::MySql(connection) => connection.close_on_drop(),
        }
    }

    /// Runs a fixed statement such as `COMMIT`, as plain text rather than prepared.
    pub(crate) async fn run(&mut self, sql: &'static str) -> Result<(), Error> {
        match &mut self.inner {
            PooledConnection::Postgres(connection) => {
                sqlx::raw_sql(sql).execute(&mut **connection).await?;
            }
            PooledConnection::MySql(connection) => {
                sqlx::raw_sql(sql).execute(&mut **connection).await?;
            }
        }
        Ok(())
    }
}
