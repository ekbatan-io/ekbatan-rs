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
//! - `ModelRepository`: Java's `AbstractRepository`, with the database selectors.

mod clock;
mod config;
mod connection;
mod db;
mod error;
mod registry;
mod repository;
mod shard;
mod transaction;
mod types;

pub use clock::{Clock, Instant, SystemClock, truncate_to_micros};
pub use config::DataSourceConfig;
pub use connection::{Connection, ConnectionProvider};
pub use db::{Db, Dbs, Dialect, Record, Scalar};
pub use error::Error;
pub use registry::{DatabaseRegistry, UnknownShardPolicy};
pub use repository::{Model, ModelId, ModelRepository, Table, TableRecord};
pub use shard::{DefaultShard, ShardIdentifier, ShardKey, ShardedId};
pub use transaction::{Transaction, TransactionManager};
pub use types::DbUuid;
