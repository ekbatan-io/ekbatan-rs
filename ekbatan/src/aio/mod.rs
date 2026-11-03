//! The async API: the `Async*` twins of `ekbatan::blocking`'s names, waited on with `.await`
//! (Python's `ekbatan.aio`). An async app uses these. Everything else (models, events, plans,
//! registries, ids, sharding, config, migrations) is shared with the blocking API as it is, and so is
//! every rule: this async code is the engine, and the blocking API waits on it.

mod action;
pub(crate) mod db;
mod distributed_job;
mod event_handler;
mod executor;
pub(crate) mod keyed_lock;
mod registry;
mod repository;
mod transaction;

pub use action::AsyncAction;
pub use db::{AsyncDb, AsyncDbs};
pub use distributed_job::AsyncDistributedJob;
pub use event_handler::AsyncEventHandler;
pub use executor::{AsyncActionExecutor, AsyncActionExecutorBuilder};
pub use keyed_lock::{AsyncKeyedLockProvider, AsyncLease};
pub use registry::AsyncDatabaseRegistry;
pub use repository::AsyncRepository;
pub use transaction::{AsyncTransaction, AsyncTransactionManager};
