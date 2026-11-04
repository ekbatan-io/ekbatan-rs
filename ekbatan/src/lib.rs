//! Ekbatan for Rust: models with history, actions that write them with their events in one
//! transaction per shard, on PostgreSQL, MySQL and MariaDB (sea-query for SQL, sqlx for
//! connections). Modules follow Java's packages:
//!
//! - `domain`: `Model`, `Entity`, `ModelEvent`, `Id`.
//! - `shard`: `ShardIdentifier`, `ShardedId`, `ShardingStrategy`.
//! - `config`: `DataSourceConfig`, `ShardingConfig`, `EkbatanConfig`.
//! - `persistence`: `ConnectionProvider`, `TransactionManager`, `DatabaseRegistry`, `Db`.
//! - `repository`: `AbstractRepository`, `RepositoryRegistry`, the ready-made methods.
//! - `action`: `ActionPlan`, `ActionRegistry`, the executor, the event log.
//! - `migration`, `keyed_lock`, `distributed_jobs`, `local_event_handler`, `json`.
//!
//! Two APIs over one engine: `blocking` (plain names, for code that is not async, as
//! `reqwest::blocking`) and `aio` (`Async*` names, Python's `ekbatan.aio`). What both share is in
//! the topic modules. The async code is the engine; the blocking API waits on
//! it (`block_on`), on a tokio runtime of its own.

pub mod action;
pub mod aio;
pub mod blocking;
mod clock;
pub mod config;
pub mod distributed_jobs;
pub mod domain;
mod error;
pub mod json;
pub mod keyed_lock;
pub mod local_event_handler;
pub mod migration;
pub mod persistence;
pub mod repository;
pub mod shard;

pub use clock::{Clock, Instant, SystemClock, truncate_to_micros};
pub use config::{
    DataSourceConfig, EkbatanConfig, MYSQL_PROPERTIES, POSTGRES_PROPERTIES, ShardGroupConfig,
    ShardMemberConfig, ShardingConfig, canonical_key,
};
pub use domain::{
    AnyModelEvent, Entity, EntityBase, EntityBuilderBase, GenericState, Id, Model, ModelBase,
    ModelBuilderBase, ModelEvent, ModelId, Persistable, event_type_name,
};
pub use error::Error;
pub use persistence::{
    Connection, ConnectionProvider, DatabaseRegistry, DbUuid, Dialect, Record, Scalar,
    TransactionManager, UnknownShardPolicy,
};
pub use repository::{AbstractRepository, Table, TableRecord};
pub use shard::{DefaultShard, ShardIdentifier, ShardKey, ShardedId, ShardingStrategy};

/// The async twins, by their short path inside the crate.
pub(crate) use aio::{AsyncDb, AsyncDbs, AsyncTransaction};

/// `#[model]`, `#[entity]`, `#[model_event]`: see `ekbatan-macros`.
pub use ekbatan_macros::{embed_migrations, entity, model, model_event};

/// What the macros' output refers to, so an app needs no serde dependency of its own.
#[doc(hidden)]
pub mod __private {
    pub use serde;
}

/// The module `ekbatan-schemagen` wrote in `build.rs` (`$OUT_DIR/schema.rs`): put it in the
/// app's `schema.rs`.
#[macro_export]
macro_rules! include_schema {
    () => {
        include!(concat!(env!("OUT_DIR"), "/schema.rs"));
    };
}
