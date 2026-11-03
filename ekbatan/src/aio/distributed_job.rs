//! `AsyncDistributedJob`: a distributed job whose work is awaited. `blocking::DistributedJob` is its
//! blocking twin; every `DistributedJob` is also an `AsyncDistributedJob`.

use std::future::Future;

use crate::Error;
use crate::distributed_jobs::{ExecutionContext, Schedule};

/// A job whose `execute` is awaited: the async twin of `DistributedJob` (Java's
/// `DistributedJob`). It runs on one instance per scheduled slot.
///
/// `execute` must be idempotent: a slot can run twice (see the module docs).
pub trait AsyncDistributedJob: Send + Sync + 'static {
    /// Cluster-wide unique and stable: the job row's key. Asked once, when the registry is
    /// built.
    fn name(&self) -> String;

    /// When the job runs. Asked once, when the registry is built.
    fn schedule(&self) -> Schedule;

    /// The work. An `Err` is a failed execution: `consecutive_failures` goes up and the job is
    /// rescheduled by its `Schedule`, as for a success.
    fn execute(&self, ctx: &ExecutionContext) -> impl Future<Output = Result<(), Error>> + Send;
}
