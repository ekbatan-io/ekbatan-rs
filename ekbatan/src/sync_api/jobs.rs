use serde::de::DeserializeOwned;

use crate::distributed_jobs::{ExecutionContext, Schedule};
use crate::local_event_handler::EventEnvelope;
use crate::{AnyModelEvent, Error};

/// A job that runs on one instance per scheduled slot: Java's `DistributedJob`. It is also an
/// `AsyncDistributedJob` (its async twin), so it goes in the same
/// `JobRegistry` with `with_job(...)`. To pause and still notice a shutdown, use
/// `ekbatan::block_on(ctx.sleep(duration))`.
pub trait DistributedJob: Send + Sync + 'static {
    /// Unique and stable across deploys: the `scheduled_tasks` row's name.
    fn name(&self) -> String;

    fn schedule(&self) -> Schedule;

    /// One run. An `Err` is logged; the next run is still scheduled.
    fn execute(&self, ctx: &ExecutionContext) -> Result<(), Error>;
}

impl<J: DistributedJob> crate::distributed_jobs::AsyncDistributedJob for J {
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

/// Reacts to one event type: Java's `EventHandler<E>`. It is also an `AsyncEventHandler` (its
/// async twin), so it goes in the same
/// `EventHandlerRegistry` with `with_handler(...)`.
pub trait EventHandler: Send + Sync + 'static {
    /// The event type this handler subscribes to.
    type Event: AnyModelEvent + DeserializeOwned + Send;

    /// Stable across deploys: stored on each notification row.
    fn name(&self) -> String;

    /// Former names whose queued rows still route here (lookup only): for renaming a handler.
    fn aliases(&self) -> Vec<String> {
        Vec::new()
    }

    /// One delivery. An `Err` is a failure: retried later, until the retention window ends.
    fn handle(&self, envelope: EventEnvelope<Self::Event>) -> Result<(), Error>;
}

impl<H: EventHandler> crate::local_event_handler::AsyncEventHandler for H {
    type Event = H::Event;

    fn name(&self) -> String {
        EventHandler::name(self)
    }

    fn aliases(&self) -> Vec<String> {
        EventHandler::aliases(self)
    }

    async fn handle(&self, envelope: EventEnvelope<H::Event>) -> Result<(), Error> {
        tokio::task::block_in_place(|| EventHandler::handle(self, envelope))
    }
}
