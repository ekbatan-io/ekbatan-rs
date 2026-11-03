//! `DatabaseRegistry`'s statements and `close()`, waited on. `aio::AsyncDatabaseRegistry` is the
//! async twin.

use super::{Db, Dbs, block_on};
use crate::{DatabaseRegistry, Error, ShardIdentifier};

impl DatabaseRegistry {
    /// Statements on the shard's primary pool: Java's `primary.get(shard)`.
    pub fn primary(&self, shard: ShardIdentifier) -> Result<Db<'_>, Error> {
        self.as_async().primary(shard).map(Db::new)
    }

    /// Statements on the shard's replica pool (the primary's when it has none).
    pub fn secondary(&self, shard: ShardIdentifier) -> Result<Db<'_>, Error> {
        self.as_async().secondary(shard).map(Db::new)
    }

    /// Every shard's primary pool.
    pub fn primaries(&self) -> Dbs<'_> {
        Dbs::new(self.as_async().primaries())
    }

    /// Every shard's replica pool.
    pub fn secondaries(&self) -> Dbs<'_> {
        Dbs::new(self.as_async().secondaries())
    }

    /// Closes every pool: Java's `close()`.
    pub fn close(&self) {
        block_on(self.as_async().close())
    }
}
