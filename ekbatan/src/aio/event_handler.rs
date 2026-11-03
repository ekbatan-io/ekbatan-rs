//! `AsyncEventHandler`: an event handler whose `handle` is awaited. `EventHandler` is its sync
//! twin; every `EventHandler` is also an `AsyncEventHandler`.

use std::future::Future;

use serde::de::DeserializeOwned;

use crate::local_event_handler::EventEnvelope;
use crate::{AnyModelEvent, Error};

/// Reacts to one event type, with an awaited `handle`: the async twin of `EventHandler` (Java's
/// `EventHandler<E>`).
pub trait AsyncEventHandler: Send + Sync + 'static {
    /// The event type this handler subscribes to.
    type Event: AnyModelEvent + DeserializeOwned + Send;

    /// Stable across deploys: stored on each notification row.
    fn name(&self) -> String;

    /// Former names whose queued rows still route here (lookup only): for renaming a handler.
    fn aliases(&self) -> Vec<String> {
        Vec::new()
    }

    /// One delivery. An `Err` is a failure: retried later, until the retention window ends.
    fn handle(
        &self,
        envelope: EventEnvelope<Self::Event>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}
