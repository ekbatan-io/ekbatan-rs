use std::collections::BTreeMap;

use crate::{Db, Dbs, Error, ShardIdentifier, ShardKey, TransactionManager};

/// What to do with a shard this deployment has no database for: Java's `UnknownShardPolicy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UnknownShardPolicy {
    /// Route to the default shard (Java's default, `USE_DEFAULT_SHARD`).
    #[default]
    UseDefaultShard,
    /// Refuse with `Error::UnknownShard` (Java's `FAIL`).
    Fail,
}

/// Every shard's `TransactionManager`, and with them every pool: Java's `DatabaseRegistry`.
pub struct DatabaseRegistry {
    transaction_managers: BTreeMap<ShardIdentifier, TransactionManager>,
    default_shard: ShardIdentifier,
    unknown_shard_policy: UnknownShardPolicy,
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
            unknown_shard_policy: UnknownShardPolicy::default(),
        })
    }

    /// Java's `withUnknownShardPolicy(policy)`.
    pub fn with_unknown_shard_policy(mut self, policy: UnknownShardPolicy) -> Self {
        self.unknown_shard_policy = policy;
        self
    }

    pub fn default_shard(&self) -> ShardIdentifier {
        self.default_shard
    }

    pub fn unknown_shard_policy(&self) -> UnknownShardPolicy {
        self.unknown_shard_policy
    }

    /// The shard a key names, or the default shard when it names one with no database and the
    /// policy says so: Java's `effectiveShard(shard)`.
    pub fn effective_shard(&self, key: impl ShardKey) -> Result<ShardIdentifier, Error> {
        let shard = key.shard(self.default_shard);
        if self.transaction_managers.contains_key(&shard) {
            return Ok(shard);
        }
        match self.unknown_shard_policy {
            UnknownShardPolicy::UseDefaultShard => Ok(self.default_shard),
            UnknownShardPolicy::Fail => Err(Error::UnknownShard(shard)),
        }
    }

    /// The shard's `TransactionManager` (Java's `transactionManager(shard)`), after
    /// `effective_shard`.
    pub fn transaction_manager(&self, key: impl ShardKey) -> Result<&TransactionManager, Error> {
        let shard = self.effective_shard(key)?;
        Ok(&self.transaction_managers[&shard])
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionProvider, DataSourceConfig, Dialect};

    const GLOBAL: ShardIdentifier = ShardIdentifier::new(0, 0);
    const MEXICO: ShardIdentifier = ShardIdentifier::new(1, 0);
    const UNCONFIGURED: ShardIdentifier = ShardIdentifier::new(9, 0);

    /// Pools open nothing until used, so no database is needed here.
    fn registry() -> DatabaseRegistry {
        let config = DataSourceConfig::new(Dialect::Postgres, "postgres://localhost:1/none", 1);
        let managers = [GLOBAL, MEXICO].map(|shard| {
            TransactionManager::new(
                shard,
                ConnectionProvider::from_config(&config).unwrap(),
                None,
            )
        });
        DatabaseRegistry::new(GLOBAL, managers).unwrap()
    }

    #[tokio::test]
    async fn an_unconfigured_shard_goes_to_the_default_by_default() {
        let registry = registry();
        assert_eq!(
            registry.unknown_shard_policy(),
            UnknownShardPolicy::UseDefaultShard
        );
        assert_eq!(registry.effective_shard(UNCONFIGURED).unwrap(), GLOBAL);
        assert_eq!(registry.effective_shard(MEXICO).unwrap(), MEXICO);
        assert_eq!(
            registry
                .transaction_manager(UNCONFIGURED)
                .unwrap()
                .shard_identifier(),
            GLOBAL
        );
    }

    #[tokio::test]
    async fn an_unconfigured_shard_is_refused_under_fail() {
        let registry = registry().with_unknown_shard_policy(UnknownShardPolicy::Fail);
        assert!(
            matches!(registry.effective_shard(UNCONFIGURED), Err(Error::UnknownShard(shard)) if shard == UNCONFIGURED)
        );
        assert!(registry.primary(UNCONFIGURED).is_err());
        assert_eq!(registry.effective_shard(MEXICO).unwrap(), MEXICO);
    }
}
