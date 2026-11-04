//! `AsyncDatabaseRegistry`: the registry's databases waited on with `.await`.

use crate::aio::AsyncTransactionManager;
use crate::{AsyncDb, AsyncDbs, DatabaseRegistry, Error, ShardIdentifier};

/// The registry's databases, waited on with `.await`: the async twin of `DatabaseRegistry`'s
/// `primary`, `secondary`, `primaries`, `secondaries` and `close` (Python's
/// `AsyncDatabaseRegistry`). `registry.as_async()`.
#[derive(Clone, Copy)]
pub struct AsyncDatabaseRegistry<'a>(pub(crate) &'a DatabaseRegistry);

impl<'a> AsyncDatabaseRegistry<'a> {
    pub fn default_shard(&self) -> ShardIdentifier {
        self.0.default_shard()
    }

    pub fn registry(&self) -> &'a DatabaseRegistry {
        self.0
    }

    /// The shard's transactions, async.
    pub fn transaction_manager(
        &self,
        shard: ShardIdentifier,
    ) -> Result<AsyncTransactionManager<'a>, Error> {
        Ok(self.0.transaction_manager(shard)?.as_async())
    }

    /// Statements on the shard's primary pool: Java's `primary.get(shard)`.
    pub fn primary(&self, shard: ShardIdentifier) -> Result<AsyncDb<'a>, Error> {
        Ok(self
            .0
            .transaction_manager(shard)?
            .primary_connection_provider()
            .db())
    }

    /// Statements on the shard's replica pool (the primary's when it has none).
    pub fn secondary(&self, shard: ShardIdentifier) -> Result<AsyncDb<'a>, Error> {
        Ok(self
            .0
            .transaction_manager(shard)?
            .secondary_connection_provider()
            .db())
    }

    /// Every shard's primary pool.
    pub fn primaries(&self) -> AsyncDbs<'a> {
        AsyncDbs(
            self.0
                .managers()
                .map(|manager| manager.primary_connection_provider().db())
                .collect(),
        )
    }

    /// Every shard's replica pool.
    pub fn secondaries(&self) -> AsyncDbs<'a> {
        AsyncDbs(
            self.0
                .managers()
                .map(|manager| manager.secondary_connection_provider().db())
                .collect(),
        )
    }

    /// Closes every pool: Java's `close()`.
    pub async fn close(&self) {
        for manager in self.0.managers() {
            manager.primary_connection_provider().close().await;
            manager.secondary_connection_provider().close().await;
        }
    }
}
