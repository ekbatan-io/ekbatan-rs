use std::collections::BTreeMap;

use crate::aio::AsyncDatabaseRegistry;
use crate::{ConnectionProvider, Error, ShardIdentifier, ShardingConfig, TransactionManager};

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

    /// Every shard of the config: a pool for its `primaryConfig`, and one for its
    /// `secondaryConfig` when it has one (else replica reads use the primary), as Java's
    /// `DatabaseRegistry.fromConfig`. Pools open no connection until first used.
    pub fn from_sharding_config(config: &ShardingConfig) -> Result<Self, Error> {
        config.validate()?;
        let mut transaction_managers = Vec::new();
        for (shard, _, member) in config.shards() {
            let primary = ConnectionProvider::from_config(member.primary_config())?;
            let secondary = member
                .secondary_config()
                .map(ConnectionProvider::from_config)
                .transpose()?;
            transaction_managers.push(TransactionManager::new(shard, primary, secondary));
        }
        Ok(
            DatabaseRegistry::new(config.default_shard, transaction_managers)?
                .with_unknown_shard_policy(config.unknown_shard_policy),
        )
    }

    /// Every configured shard, in order: Java's `allTransactionManagers()` by key.
    pub fn shards(&self) -> Vec<ShardIdentifier> {
        self.transaction_managers.keys().copied().collect()
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

    /// `shard`, or the default shard when `shard` has no database and the policy says so: Java's
    /// `effectiveShard(shard)`.
    pub fn effective_shard(&self, shard: ShardIdentifier) -> Result<ShardIdentifier, Error> {
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
    pub fn transaction_manager(
        &self,
        shard: ShardIdentifier,
    ) -> Result<&TransactionManager, Error> {
        let shard = self.effective_shard(shard)?;
        Ok(&self.transaction_managers[&shard])
    }

    /// Every shard's `TransactionManager`, in shard order.
    pub(crate) fn managers(&self) -> impl Iterator<Item = &TransactionManager> {
        self.transaction_managers.values()
    }

    /// The async view: statements and `close()` waited on with `.await`.
    pub fn as_async(&self) -> AsyncDatabaseRegistry<'_> {
        AsyncDatabaseRegistry(self)
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
