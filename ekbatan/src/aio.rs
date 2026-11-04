//! The async API: the `Async*` twins of `ekbatan::blocking`'s names, waited on with `.await`
//! (Python's `ekbatan.aio`). Each lives with its topic (`action`, `repository`, `persistence`,
//! ...) next to its blocking twin; this module gathers them. The async code is the engine: the
//! blocking API waits on it.

pub use crate::action::aio::{AsyncAction, AsyncActionExecutor, AsyncActionExecutorBuilder};
pub use crate::distributed_jobs::aio::AsyncDistributedJob;
pub use crate::keyed_lock::aio::{AsyncKeyedLockProvider, AsyncLease};
pub use crate::local_event_handler::aio::AsyncEventHandler;
pub use crate::persistence::aio::{
    AsyncDatabaseRegistry, AsyncDb, AsyncDbs, AsyncTransaction, AsyncTransactionManager,
};
pub use crate::repository::aio::AsyncRepository;
