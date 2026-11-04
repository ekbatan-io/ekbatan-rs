//! Repositories: Java's `io.ekbatan.core.repository`.
//!
//! A repository implements `AbstractRepository` (Java's name): where its databases are, how a
//! row becomes a model or entity and back, how it shards. Its selectors and ready-made methods
//! come in a blocking kind (`blocking::Repository`) and an async kind (`aio::AsyncRepository`),
//! both given to every `AbstractRepository`; `blocking` and `aio` here. `RepositoryRegistry`
//! says which repository writes each type an action stages.

pub(crate) mod aio;
pub(crate) mod blocking;
mod registry;
pub(crate) mod sql;

pub use registry::{AnyRepository, RepositoryRegistry, RepositoryRegistryBuilder};

use sea_query::{Iden, Value};

use crate::{
    DatabaseRegistry, Error, Persistable, Record, ShardIdentifier, ShardKey, ShardingStrategy,
};

/// A table, as schemagen writes it: its name and the columns the ready-made methods use.
pub trait Table: Iden + PartialEq + Copy + Send + Sync + 'static {
    const TABLE: Self;
    const ID: Self;
    const VERSION: Self;
    const STATE: Self;
    const COLUMNS: &'static [Self];
}

/// One row of a table, as schemagen writes it: read by sqlx, written as column-value pairs.
pub trait TableRecord: Record + 'static {
    type Table: Table;

    fn values(&self) -> Vec<(Self::Table, Value)>;
}

pub(crate) type TableOf<R> = <<R as AbstractRepository>::Record as TableRecord>::Table;
pub(crate) type IdOf<R> = <<R as AbstractRepository>::Persistable as Persistable>::Id;

/// Java's `AbstractRepository` (and its `ModelRepository` / `EntityRepository`, which differ
/// only in what they store). An implementation says where the databases are, how a row becomes
/// a model or entity and back (`from_record` / `to_record`, as in Java) and, if it shards, its
/// `sharding_strategy`; the selectors and the ready-made methods below work from that, with
/// Java's SQL.
pub trait AbstractRepository: Send + Sync {
    type Persistable: Persistable;
    type Record: TableRecord;

    fn registry(&self) -> &DatabaseRegistry;
    fn from_record(record: Self::Record) -> Result<Self::Persistable, Error>;
    fn to_record(persistable: &Self::Persistable) -> Self::Record;

    /// How ids name shards: `NoSharding` unless overridden, as Java's repositories default to
    /// `NoShardingStrategy`.
    fn sharding_strategy(&self) -> ShardingStrategy {
        ShardingStrategy::NoSharding
    }

    /// The shard a key names through this repository's strategy, or the default shard; then the
    /// registry's unknown-shard policy. Java's `effectiveShard(...)`.
    fn effective_shard(&self, key: impl ShardKey) -> Result<ShardIdentifier, Error> {
        let registry = self.registry();
        let shard = key
            .resolve(self.sharding_strategy())
            .unwrap_or_else(|| registry.default_shard());
        registry.effective_shard(shard)
    }

    /// The one shard all `models` are on: Java's `effectiveShard(Collection)`, which refuses
    /// models on two shards with `CrossShardException`.
    fn shard_of_all(&self, models: &[Self::Persistable]) -> Result<ShardIdentifier, Error> {
        if self.sharding_strategy() == ShardingStrategy::NoSharding {
            return Ok(self.registry().default_shard());
        }
        let mut first = None;
        for model in models {
            let shard = self.effective_shard(model.id())?;
            match first {
                None => first = Some(shard),
                Some(first) if first != shard => {
                    return Err(Error::CrossShard {
                        first,
                        second: shard,
                    });
                }
                Some(_) => {}
            }
        }
        Ok(first.unwrap_or_else(|| self.registry().default_shard()))
    }
}
