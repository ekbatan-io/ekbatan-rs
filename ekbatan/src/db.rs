use futures_util::future::try_join_all;
use sea_query::{
    InsertStatement, IntoIden, MysqlQueryBuilder, OnConflict, PostgresQueryBuilder,
    QueryStatementWriter, SelectStatement, Value, Values,
};
use sea_query_sqlx::SqlxValues;
use sqlx::mysql::{MySql, MySqlRow};
use sqlx::postgres::{PgRow, Postgres};
use sqlx::{AssertSqlSafe, Decode, FromRow, Type};

use crate::Error;
use crate::connection::{Pool, PooledConnection};

/// The SQL flavour of one database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    MySql,
    MariaDb,
}

/// Rows that read the same from PostgreSQL and MySQL: what `#[derive(sqlx::FromRow)]` gives a
/// struct whose fields all decode from both.
pub trait Record:
    for<'r> FromRow<'r, PgRow> + for<'r> FromRow<'r, MySqlRow> + Send + Unpin
{
}

impl<T> Record for T where
    T: for<'r> FromRow<'r, PgRow> + for<'r> FromRow<'r, MySqlRow> + Send + Unpin
{
}

/// A single value, such as a count, that reads the same from PostgreSQL and MySQL.
pub trait Scalar:
    for<'r> Decode<'r, Postgres>
    + Type<Postgres>
    + for<'r> Decode<'r, MySql>
    + Type<MySql>
    + Send
    + Unpin
{
}

impl<T> Scalar for T where
    T: for<'r> Decode<'r, Postgres>
        + Type<Postgres>
        + for<'r> Decode<'r, MySql>
        + Type<MySql>
        + Send
        + Unpin
{
}

/// Where statements run and which SQL to write for them: jOOQ's `DSLContext`. On a pool, each
/// statement borrows a connection just while it runs; on a held `Connection` (a transaction is
/// one), every statement uses that connection.
pub struct Db<'a> {
    target: Target<'a>,
    dialect: Dialect,
}

enum Target<'a> {
    Pool(&'a Pool),
    Connection(&'a mut PooledConnection),
}

/// Runs `$run` with the SQL (`$sql`) and bound values (`$values`) written for this `Db`'s
/// database, and the pool or connection to run it on (`$executor`).
macro_rules! on_database {
    ($db:expr, postgres: $postgres:expr, mysql: $mysql:expr, |$sql:ident, $values:ident, $executor:ident| $run:expr) => {
        match &mut $db.target {
            Target::Pool(Pool::Postgres(pool)) => {
                let ($sql, $values) = $postgres;
                let $executor = pool;
                $run
            }
            Target::Pool(Pool::MySql(pool)) => {
                let ($sql, $values) = $mysql;
                let $executor = pool;
                $run
            }
            Target::Connection(PooledConnection::Postgres(connection)) => {
                let ($sql, $values) = $postgres;
                let $executor = &mut **connection;
                $run
            }
            Target::Connection(PooledConnection::MySql(connection)) => {
                let ($sql, $values) = $mysql;
                let $executor = &mut **connection;
                $run
            }
        }
    };
    ($db:expr, $statement:expr, |$sql:ident, $values:ident, $executor:ident| $run:expr) => {
        on_database!($db, postgres: for_postgres($statement), mysql: for_mysql($statement),
            |$sql, $values, $executor| $run)
    };
}

impl<'a> Db<'a> {
    pub(crate) fn on_pool(pool: &'a Pool, dialect: Dialect) -> Self {
        Db {
            target: Target::Pool(pool),
            dialect,
        }
    }

    pub(crate) fn on_connection(connection: &'a mut PooledConnection, dialect: Dialect) -> Self {
        Db {
            target: Target::Connection(connection),
            dialect,
        }
    }

    pub fn dialect(&self) -> Dialect {
        self.dialect
    }

    /// INSERT, UPDATE or DELETE. Returns how many rows it changed.
    pub async fn execute<S: QueryStatementWriter + Sync>(
        &mut self,
        statement: &S,
    ) -> Result<u64, Error> {
        Ok(on_database!(self, statement, |sql, values, executor| {
            sqlx::query_with(AssertSqlSafe(sql), values)
                .execute(executor)
                .await?
                .rows_affected()
        }))
    }

    /// INSERT that skips rows whose `key` already exists: `ON CONFLICT (key) DO NOTHING` on
    /// PostgreSQL, `INSERT IGNORE` on MySQL and MariaDB, as jOOQ's `onConflictDoNothing()`
    /// writes them. sea-query can't write the MySQL one (its `do_nothing()` gives
    /// `ON DUPLICATE KEY IGNORE`, which MySQL rejects), so it is added here. Returns how many
    /// rows were inserted.
    pub async fn insert_ignoring_duplicates(
        &mut self,
        insert: &InsertStatement,
        key: impl IntoIden,
    ) -> Result<u64, Error> {
        let postgres = insert
            .clone()
            .on_conflict(OnConflict::column(key).do_nothing().to_owned())
            .to_owned();
        Ok(
            on_database!(self, postgres: for_postgres(&postgres), mysql: for_mysql_insert_ignore(insert),
            |sql, values, executor| {
                sqlx::query_with(AssertSqlSafe(sql), values)
                    .execute(executor)
                    .await?
                    .rows_affected()
            }),
        )
    }

    pub async fn fetch_all<R: Record>(
        &mut self,
        select: &SelectStatement,
    ) -> Result<Vec<R>, Error> {
        Ok(on_database!(self, select, |sql, values, executor| {
            sqlx::query_as_with(AssertSqlSafe(sql), values)
                .fetch_all(executor)
                .await?
        }))
    }

    pub async fn fetch_optional<R: Record>(
        &mut self,
        select: &SelectStatement,
    ) -> Result<Option<R>, Error> {
        Ok(on_database!(self, select, |sql, values, executor| {
            sqlx::query_as_with(AssertSqlSafe(sql), values)
                .fetch_optional(executor)
                .await?
        }))
    }

    /// The first column of the one row, such as a count.
    pub async fn fetch_scalar<T: Scalar>(&mut self, select: &SelectStatement) -> Result<T, Error> {
        Ok(on_database!(self, select, |sql, values, executor| {
            sqlx::query_scalar_with(AssertSqlSafe(sql), values)
                .fetch_one(executor)
                .await?
        }))
    }
}

/// One `Db` per shard, for asking every shard at once: Java's `dbs()` / `readonlyDbs()`.
pub struct Dbs<'a>(pub Vec<Db<'a>>);

impl<'a> Dbs<'a> {
    /// Runs `select` on every shard at the same time and returns all the rows.
    pub async fn fetch_all<R: Record>(self, select: &SelectStatement) -> Result<Vec<R>, Error> {
        let per_shard = try_join_all(
            self.0
                .into_iter()
                .map(|mut db| async move { db.fetch_all(select).await }),
        )
        .await?;
        Ok(per_shard.into_iter().flatten().collect())
    }

    /// Runs `select` on every shard at the same time: one value per shard, such as a count.
    pub async fn fetch_scalars<T: Scalar>(self, select: &SelectStatement) -> Result<Vec<T>, Error> {
        try_join_all(
            self.0
                .into_iter()
                .map(|mut db| async move { db.fetch_scalar(select).await }),
        )
        .await
    }

    pub fn into_vec(self) -> Vec<Db<'a>> {
        self.0
    }
}

fn for_postgres(statement: &impl QueryStatementWriter) -> (String, SqlxValues) {
    let (sql, values) = statement.build(PostgresQueryBuilder);
    (sql, SqlxValues(values))
}

/// MySQL and MariaDB get UUIDs as text: MySQL keeps them in `CHAR(36)`, and MariaDB's `UUID`
/// type takes text. (Bound as a UUID, sqlx would send 16 raw bytes, which match neither.)
fn for_mysql(statement: &impl QueryStatementWriter) -> (String, SqlxValues) {
    let (sql, Values(values)) = statement.build(MysqlQueryBuilder);
    let values = values
        .into_iter()
        .map(|value| match value {
            Value::Uuid(uuid) => Value::String(uuid.map(|uuid| uuid.hyphenated().to_string())),
            other => other,
        })
        .collect();
    (sql, SqlxValues(Values(values)))
}

/// `INSERT IGNORE INTO ...` for MySQL and MariaDB.
fn for_mysql_insert_ignore(insert: &InsertStatement) -> (String, SqlxValues) {
    let (sql, values) = for_mysql(insert);
    assert!(
        sql.starts_with("INSERT "),
        "an INSERT statement, not: {sql}"
    );
    (sql.replacen("INSERT ", "INSERT IGNORE ", 1), values)
}
