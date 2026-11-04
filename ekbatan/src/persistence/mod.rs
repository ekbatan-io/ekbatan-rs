//! Where the databases are and how statements reach them: Java's `io.ekbatan.core.persistence`
//! (and its `DatabaseRegistry`).
//!
//! - `ConnectionProvider`: one database's pool. Lends connections (`Connection`).
//! - `TransactionManager`: one shard's primary and replica pools, and its transactions.
//! - `DatabaseRegistry`: every shard's `TransactionManager`.
//! - `Db` (`AsyncDb`): where statements run and which SQL to write, jOOQ's `DSLContext`;
//!   `Transaction` (`AsyncTransaction`): a connection with a transaction open on it. These two
//!   come in a blocking and an async kind (`blocking` and `aio` here).

mod connection;
mod db;
mod registry;
mod transaction;
mod uuid;

pub(crate) mod aio;
pub(crate) mod blocking;

pub use connection::{Connection, ConnectionProvider};
pub(crate) use connection::{Pool, PooledConnection};
pub use db::{Dialect, Record, Scalar};
pub use registry::{DatabaseRegistry, DatabaseRegistryBuilder, UnknownShardPolicy};
pub use transaction::TransactionManager;
pub use uuid::DbUuid;
