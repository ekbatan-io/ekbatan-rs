//! `EventHandler`: an event handler whose `handle` is a plain `fn`. `aio::AsyncEventHandler` is
//! the async twin; every `EventHandler` is also an `AsyncEventHandler`.

use serde::de::DeserializeOwned;

use crate::aio::AsyncEventHandler;
use crate::local_event_handler::EventEnvelope;
use crate::{AnyModelEvent, Error};

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

impl<H: EventHandler> AsyncEventHandler for H {
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
