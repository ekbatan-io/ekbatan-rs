//! A sketch of ekbatan-rs's database layer, on sea-query (SQL) and sqlx (connections), with
//! Java's structure:
//!
//! - `DataSourceConfig`: how to reach one database, and its pool sizes.
//! - `ConnectionProvider`: one database's pool. Lends connections.
//! - `Connection`: one connection taken from a pool and held.
//! - `Transaction`: a `Connection` with a transaction open on it.
//! - `TransactionManager`: one shard's primary and replica pools, and its transactions.
//! - `DatabaseRegistry`: every shard's `TransactionManager`.
//! - `Db`: where statements run and which SQL to write: jOOQ's `DSLContext`.
//! - `AbstractRepository`: Java's `AbstractRepository`: what a repository says about itself;
//!   `Repository` gives it the database selectors and ready-made methods.
//!
//! Two APIs over one engine: `blocking` (plain names, for code that is not async, as
//! `reqwest::blocking`) and `aio` (`Async*` names, Python's `ekbatan.aio`). What both share is in
//! the topic modules. The async code is the engine; the blocking API waits on
//! it (`block_on`), on a tokio runtime of its own.

pub mod action;
pub mod aio;
pub mod blocking;
mod clock;
mod config;
pub(crate) mod connection;
pub(crate) mod db;
pub mod distributed_jobs;
mod domain;
mod error;
mod ids_json;
pub mod json;
pub mod keyed_lock;
pub mod local_event_handler;
pub mod migration;
mod registry;
mod repository;
mod shard;
mod sharding_config;
mod transaction;
mod types;

pub use clock::{Clock, Instant, SystemClock, truncate_to_micros};
pub use config::{DataSourceConfig, MYSQL_PROPERTIES, POSTGRES_PROPERTIES};
pub use connection::{Connection, ConnectionProvider};
pub use db::{Dialect, Record, Scalar};
pub use domain::{
    AnyModelEvent, Entity, EntityBase, EntityBuilderBase, GenericState, Id, Model, ModelBase,
    ModelBuilderBase, ModelEvent, ModelId, Persistable, event_type_name,
};
pub use error::Error;
pub use registry::{DatabaseRegistry, UnknownShardPolicy};
pub use repository::{AbstractRepository, Table, TableRecord};
pub use shard::{DefaultShard, ShardIdentifier, ShardKey, ShardedId, ShardingStrategy};
pub use sharding_config::{
    EkbatanConfig, ShardGroupConfig, ShardMemberConfig, ShardingConfig, canonical_key,
};
pub use transaction::TransactionManager;
pub use types::DbUuid;

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
