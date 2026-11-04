//! `DistributedJob`: a job whose `execute` is a plain `fn`. `aio::AsyncDistributedJob` is the
//! async twin; every `DistributedJob` is also an `AsyncDistributedJob`.

use crate::Error;
use crate::aio::AsyncDistributedJob;
use crate::distributed_jobs::{ExecutionContext, Schedule};

/// A job that runs on one instance per scheduled slot: Java's `DistributedJob`. It is also an
/// `AsyncDistributedJob` (its async twin), so it goes in the same
/// `JobRegistry` with `with_job(...)`. To pause and still notice a shutdown, use
/// `ekbatan::blocking::block_on(ctx.sleep(duration))`.
pub trait DistributedJob: Send + Sync + 'static {
    /// Unique and stable across deploys: the `scheduled_tasks` row's name.
    fn name(&self) -> String;

    fn schedule(&self) -> Schedule;

    /// The work. An `Err` is a failed execution: `consecutive_failures` goes up and the job is
    /// rescheduled by its `Schedule`, as for a success.
    fn execute(&self, ctx: &ExecutionContext) -> Result<(), Error>;
}

impl<J: DistributedJob> AsyncDistributedJob for J {
    fn name(&self) -> String {
        DistributedJob::name(self)
    }

    fn schedule(&self) -> Schedule {
        DistributedJob::schedule(self)
    }

    async fn execute(&self, ctx: &ExecutionContext) -> Result<(), Error> {
        tokio::task::block_in_place(|| DistributedJob::execute(self, ctx))
    }
}
