//! The tokio runtime under the sync API, and `block_on` for calling async code from sync code.
//!
//! The async code is the engine and the sync API (the plain names) waits on it, so every rule
//! has one copy. The runtime is the sync API's own, made the first time it is needed; a pool made
//! outside any runtime lives on it. The sync API is for code that is not itself async: called
//! from an async function it blocks that function's thread, and on a single-threaded runtime it
//! panics, as `reqwest::blocking` does.

use std::future::Future;
use std::sync::OnceLock;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// The sync API's own tokio runtime (multi-threaded), made on first use.
pub fn blocking_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("ekbatan-blocking")
            .enable_all()
            .build()
            .expect("the sync API's tokio runtime could not start")
    })
}

/// Runs an async ekbatan call to the end and returns its result, from code that is not async:
/// `ekbatan::block_on(migrator.migrate())`.
pub fn block_on<F: Future>(future: F) -> F::Output {
    // Inside a sync action, job or handler we are already on one of the runtime's threads:
    // `block_in_place` steps out of it first, so waiting here doesn't stall other work.
    tokio::task::block_in_place(|| blocking_runtime().block_on(future))
}

/// Runs `make` inside the sync API's runtime when no runtime is current, so what it starts (a
/// pool's background work) has a runtime to live on.
pub(crate) fn in_some_runtime<T>(make: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current().is_ok() {
        make()
    } else {
        let _inside = blocking_runtime().enter();
        make()
    }
}
