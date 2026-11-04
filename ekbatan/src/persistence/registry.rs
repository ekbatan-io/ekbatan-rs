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

impl std::str::FromStr for UnknownShardPolicy {
    type Err = Error;

    /// Java's `UnknownShardPolicy.from`: case, `_` and `-` ignored, so `USE_DEFAULT_SHARD`,
    /// `useDefaultShard` and `use-default-shard` all read; blank is `UseDefaultShard`.
    fn from_str(value: &str) -> Result<Self, Error> {
        let normalised: String = value
            .chars()
            .filter(char::is_ascii_alphabetic)
            .map(|c| c.to_ascii_lowercase())
            .collect();
        match normalised.as_str() {
            "" | "usedefaultshard" => Ok(UnknownShardPolicy::UseDefaultShard),
            "fail" => Ok(UnknownShardPolicy::Fail),
            _ => Err(Error::Config(format!(
                "unknownShardPolicy '{value}' is neither USE_DEFAULT_SHARD nor FAIL"
            ))),
        }
    }
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

    /// Java's `DatabaseRegistry.Builder.databaseRegistry()`.
    pub fn builder() -> DatabaseRegistryBuilder {
        DatabaseRegistryBuilder::default()
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

    /// The default shard's `TransactionManager`: Java's `defaultTransactionManager()`.
    pub fn default_transaction_manager(&self) -> &TransactionManager {
        &self.transaction_managers[&self.default_shard]
    }

    /// Every shard's `TransactionManager`, in shard order: Java's `allTransactionManagers()`.
    pub fn all_transaction_managers(&self) -> impl Iterator<Item = &TransactionManager> {
        self.transaction_managers.values()
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

/// Java's `DatabaseRegistry.Builder`. The default shard is the one given with
/// `with_default_database`, or the only one when just one is given.
#[derive(Default)]
pub struct DatabaseRegistryBuilder {
    transaction_managers: Vec<TransactionManager>,
    default_shard: Option<ShardIdentifier>,
    unknown_shard_policy: UnknownShardPolicy,
}

impl DatabaseRegistryBuilder {
    /// A shard that is not the default.
    pub fn with_database(mut self, transaction_manager: TransactionManager) -> Self {
        self.transaction_managers.push(transaction_manager);
        self
    }

    /// The default shard: where the executor writes when an action stages nothing shard-aware.
    pub fn with_default_database(mut self, transaction_manager: TransactionManager) -> Self {
        self.default_shard = Some(transaction_manager.shard_identifier());
        self.transaction_managers.push(transaction_manager);
        self
    }

    /// Optional; `UseDefaultShard` unless set.
    pub fn with_unknown_shard_policy(mut self, policy: UnknownShardPolicy) -> Self {
        self.unknown_shard_policy = policy;
        self
    }

    pub fn build(self) -> Result<DatabaseRegistry, Error> {
        let default_shard = match (self.default_shard, self.transaction_managers.as_slice()) {
            (Some(shard), _) => shard,
            (None, [only]) => only.shard_identifier(),
            (None, []) => {
                return Err(Error::Config(
                    "a DatabaseRegistry needs at least one database".to_owned(),
                ));
            }
            (None, _) => {
                return Err(Error::Config(
                    "several databases and none is the default: use with_default_database"
                        .to_owned(),
                ));
            }
        };
        let mut seen = std::collections::BTreeSet::new();
        for manager in &self.transaction_managers {
            if !seen.insert(manager.shard_identifier()) {
                return Err(Error::Config(format!(
                    "shard {} is registered twice",
                    manager.shard_identifier()
                )));
            }
        }
        Ok(
            DatabaseRegistry::new(default_shard, self.transaction_managers)?
                .with_unknown_shard_policy(self.unknown_shard_policy),
        )
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

    fn manager(shard: ShardIdentifier) -> TransactionManager {
        let config = DataSourceConfig::new(Dialect::Postgres, "postgres://localhost:1/none", 1);
        TransactionManager::new(
            shard,
            ConnectionProvider::from_config(&config).unwrap(),
            None,
        )
    }

    #[tokio::test]
    async fn the_builder_picks_the_default_as_javas_does() {
        let registry = DatabaseRegistry::builder()
            .with_database(manager(MEXICO))
            .with_default_database(manager(GLOBAL))
            .with_unknown_shard_policy(UnknownShardPolicy::Fail)
            .build()
            .unwrap();
        assert_eq!(registry.default_shard(), GLOBAL);
        assert_eq!(
            registry.default_transaction_manager().shard_identifier(),
            GLOBAL
        );
        let all: Vec<_> = registry
            .all_transaction_managers()
            .map(TransactionManager::shard_identifier)
            .collect();
        assert_eq!(all, [GLOBAL, MEXICO]);
        assert!(matches!(
            registry.effective_shard(UNCONFIGURED),
            Err(Error::UnknownShard(_))
        ));
        let only = DatabaseRegistry::builder()
            .with_database(manager(MEXICO))
            .build()
            .unwrap();
        assert_eq!(only.default_shard(), MEXICO);
        assert!(DatabaseRegistry::builder().build().is_err());
        assert!(
            DatabaseRegistry::builder()
                .with_database(manager(GLOBAL))
                .with_database(manager(MEXICO))
                .build()
                .is_err()
        );
        assert!(
            DatabaseRegistry::builder()
                .with_default_database(manager(GLOBAL))
                .with_database(manager(GLOBAL))
                .build()
                .is_err()
        );
    }

    #[test]
    fn the_policy_reads_as_javas_does() {
        for text in [
            "USE_DEFAULT_SHARD",
            "useDefaultShard",
            "use-default-shard",
            " ",
            "",
        ] {
            assert_eq!(
                text.parse::<UnknownShardPolicy>().ok(),
                Some(UnknownShardPolicy::UseDefaultShard),
                "{text:?}"
            );
        }
        for text in ["FAIL", "fail", "Fail"] {
            assert_eq!(
                text.parse::<UnknownShardPolicy>().ok(),
                Some(UnknownShardPolicy::Fail)
            );
        }
        assert!("panic".parse::<UnknownShardPolicy>().is_err());
    }
}
