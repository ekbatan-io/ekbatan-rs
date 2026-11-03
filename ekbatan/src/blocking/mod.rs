//! The blocking (sync) API: the plain names, for code that is not async (Rust's
//! `reqwest::blocking` pattern; Python's sync API). The async twins are `ekbatan::aio::Async*`.
//! What both sides share (plans, registries, `AbstractRepository`, config, lock providers, the
//! job scheduler) is in the topic modules.
//!
//! The async code is the engine and this API waits on it, so every rule has one copy: an
//! `Action`, `DistributedJob` or `EventHandler` is also an `AsyncAction`, `AsyncDistributedJob`
//! or `AsyncEventHandler` (blanket impls), so it registers in the same registries and runs in the
//! same executor and jobs.
//!
//! Underneath is one tokio runtime of its own (`runtime()`), made the first time it is needed; a
//! pool made outside any runtime lives on it. Calls with no blocking twin (migrations, starting
//! and stopping a `JobRegistry`) are run with `block_on`. This API is for code that is not
//! itself async: called from an async function it blocks that function's thread, and on a
//! single-threaded runtime it panics, as `reqwest::blocking` does.

mod action;
mod db;
mod distributed_job;
mod event_handler;
mod executor;
mod keyed_lock;
mod registry;
mod repository;
mod transaction;

pub use action::Action;
pub use db::{Db, Dbs};
pub use distributed_job::DistributedJob;
pub use event_handler::EventHandler;
pub use executor::{ActionExecutor, ActionExecutorBuilder};
pub use keyed_lock::{KeyedLockProvider, Lease};
pub use repository::Repository;
pub use transaction::Transaction;

use std::future::Future;
use std::sync::OnceLock;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// The sync API's own tokio runtime (multi-threaded), made on first use.
pub fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("ekbatan-blocking")
            .enable_all()
            .build()
            .expect("the sync API's tokio runtime could not start")
    })
}

/// Runs an async ekbatan call to the end and returns its result, from code that is not async:
/// `ekbatan::blocking::block_on(migrator.migrate())`.
pub fn block_on<F: Future>(future: F) -> F::Output {
    // Inside a sync action, job or handler we are already on one of the runtime's threads:
    // `block_in_place` steps out of it first, so waiting here doesn't stall other work.
    tokio::task::block_in_place(|| runtime().block_on(future))
}

/// Runs `make` inside the sync API's runtime when no runtime is current, so what it starts (a
/// pool's background work) has a runtime to live on.
pub(crate) fn in_some_runtime<T>(make: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current().is_ok() {
        make()
    } else {
        let _inside = runtime().enter();
        make()
    }
}
