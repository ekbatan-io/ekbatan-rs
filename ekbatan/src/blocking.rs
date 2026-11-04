//! The blocking API: everything an app that is not async uses, under plain names (Rust's
//! `reqwest::blocking`; Python's sync API). Each lives with its topic (`action`, `repository`,
//! `persistence`, ...) next to its async twin in `ekbatan::aio`; this module gathers them.
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

pub use crate::action::blocking::{Action, ActionExecutor, ActionExecutorBuilder};
pub use crate::distributed_jobs::blocking::DistributedJob;
pub use crate::keyed_lock::blocking::{KeyedLockProvider, Lease};
pub use crate::local_event_handler::blocking::EventHandler;
pub use crate::persistence::blocking::{Db, Dbs, Transaction};
pub use crate::repository::blocking::Repository;

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
