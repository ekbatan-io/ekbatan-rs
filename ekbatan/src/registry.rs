use std::collections::BTreeMap;

use crate::{Db, Dbs, Error, ShardIdentifier, ShardKey, TransactionManager};

/// Every shard's `TransactionManager`, and with them every pool: Java's `DatabaseRegistry`.
pub struct DatabaseRegistry {
    transaction_managers: BTreeMap<ShardIdentifier, TransactionManager>,
    default_shard: ShardIdentifier,
}

impl DatabaseRegistry {
    pub fn new(
        default_shard: ShardIdentifier,
        transaction_managers: impl IntoIterator<Item = TransactionManager>,
    ) -> Result<Self, Error> {
        let transaction_managers: BTreeMap<_, _> = transaction_managers
            .into_iter()
            .map(|manager| (manager.shard_identifier(), manager))
            .collect();
        if !transaction_managers.contains_key(&default_shard) {
            return Err(Error::UnknownShard(default_shard));
        }
        Ok(DatabaseRegistry {
            transaction_managers,
            default_shard,
        })
    }

    pub fn default_shard(&self) -> ShardIdentifier {
        self.default_shard
    }

    /// The shard's `TransactionManager`: Java's `transactionManager(shard)`.
    pub fn transaction_manager(&self, key: impl ShardKey) -> Result<&TransactionManager, Error> {
        let shard = key.shard(self.default_shard);
        self.transaction_managers
            .get(&shard)
            .ok_or(Error::UnknownShard(shard))
    }

    /// Statements on the shard's primary pool: Java's `primary.get(shard)`.
    pub fn primary(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        Ok(self
            .transaction_manager(key)?
            .primary_connection_provider()
            .db())
    }

    /// Statements on the shard's replica pool (the primary's when it has none).
    pub fn secondary(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        Ok(self
            .transaction_manager(key)?
            .secondary_connection_provider()
            .db())
    }

    /// Every shard's primary pool.
    pub fn primaries(&self) -> Dbs<'_> {
        Dbs(self
            .transaction_managers
            .values()
            .map(|manager| manager.primary_connection_provider().db())
            .collect())
    }

    /// Every shard's replica pool.
    pub fn secondaries(&self) -> Dbs<'_> {
        Dbs(self
            .transaction_managers
            .values()
            .map(|manager| manager.secondary_connection_provider().db())
            .collect())
    }

    /// Closes every pool: Java's `close()`.
    pub async fn close(&self) {
        for manager in self.transaction_managers.values() {
            manager.primary_connection_provider().close().await;
            manager.secondary_connection_provider().close().await;
        }
    }
}
