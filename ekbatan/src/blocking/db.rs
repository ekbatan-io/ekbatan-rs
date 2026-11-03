//! `Db` and `Dbs`: statements that wait for their rows. `aio::AsyncDb` is the async twin.

use sea_query::{InsertStatement, IntoIden, QueryStatementWriter, SelectStatement};

use super::block_on;
use crate::aio::{AsyncDb, AsyncDbs};
use crate::{Dialect, Error, Record, Scalar};

/// Where statements run and which SQL to write: jOOQ's `DSLContext`. Its async twin is
/// `AsyncDb`; this one waits on it.
pub struct Db<'a>(AsyncDb<'a>);

impl<'a> Db<'a> {
    pub fn new(db: AsyncDb<'a>) -> Self {
        Db(db)
    }

    pub fn into_async(self) -> AsyncDb<'a> {
        self.0
    }

    pub fn dialect(&self) -> Dialect {
        self.0.dialect()
    }

    /// INSERT, UPDATE or DELETE. Returns how many rows it changed.
    pub fn execute<S: QueryStatementWriter + Sync>(&mut self, statement: &S) -> Result<u64, Error> {
        block_on(self.0.execute(statement))
    }

    /// `ON CONFLICT (key) DO NOTHING` on PostgreSQL, `INSERT IGNORE` on MySQL and MariaDB.
    pub fn insert_ignoring_duplicates(
        &mut self,
        insert: &InsertStatement,
        key: impl IntoIden,
    ) -> Result<u64, Error> {
        block_on(self.0.insert_ignoring_duplicates(insert, key))
    }

    /// `ON CONFLICT DO NOTHING` on PostgreSQL, `INSERT IGNORE` on MySQL and MariaDB.
    pub fn insert_ignoring_any_duplicate(
        &mut self,
        insert: &InsertStatement,
    ) -> Result<u64, Error> {
        block_on(self.0.insert_ignoring_any_duplicate(insert))
    }

    /// Every row a SELECT, or a statement with `RETURNING`, gives back.
    pub fn fetch_all<R: Record, S: QueryStatementWriter + Sync>(
        &mut self,
        statement: &S,
    ) -> Result<Vec<R>, Error> {
        block_on(self.0.fetch_all(statement))
    }

    /// The first row, if any.
    pub fn fetch_optional<R: Record, S: QueryStatementWriter + Sync>(
        &mut self,
        statement: &S,
    ) -> Result<Option<R>, Error> {
        block_on(self.0.fetch_optional(statement))
    }

    /// `SELECT EXISTS(<select>)`.
    pub fn fetch_exists(&mut self, select: &SelectStatement) -> Result<bool, Error> {
        block_on(self.0.fetch_exists(select))
    }

    /// The first column of the one row, such as a count.
    pub fn fetch_scalar<T: Scalar>(&mut self, select: &SelectStatement) -> Result<T, Error> {
        block_on(self.0.fetch_scalar(select))
    }
}

impl<'a> From<AsyncDb<'a>> for Db<'a> {
    fn from(db: AsyncDb<'a>) -> Self {
        Db(db)
    }
}

/// One `Db` per shard, for asking every shard at once: Java's `dbs()` / `readonlyDbs()`. The shards are still asked at the
/// same time; only the caller waits.
pub struct Dbs<'a>(AsyncDbs<'a>);

impl<'a> Dbs<'a> {
    pub fn new(dbs: AsyncDbs<'a>) -> Self {
        Dbs(dbs)
    }

    /// Runs `select` on every shard and returns all the rows.
    pub fn fetch_all<R: Record>(self, select: &SelectStatement) -> Result<Vec<R>, Error> {
        block_on(self.0.fetch_all(select))
    }

    /// Runs `select` on every shard: one value per shard, such as a count.
    pub fn fetch_scalars<T: Scalar>(self, select: &SelectStatement) -> Result<Vec<T>, Error> {
        block_on(self.0.fetch_scalars(select))
    }

    pub fn into_vec(self) -> Vec<Db<'a>> {
        self.0.into_vec().into_iter().map(Db).collect()
    }
}

impl<'a> From<AsyncDbs<'a>> for Dbs<'a> {
    fn from(dbs: AsyncDbs<'a>) -> Self {
        Dbs(dbs)
    }
}
