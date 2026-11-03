//! Java's `EventHandler`, `EventEnvelope` and `EventHandlerRegistry`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::notification::EventNotification;
use crate::{AnyModelEvent, Error, Instant, event_type_name};

/// One delivery: the event, and the action it came from. Java's `EventEnvelope`.
#[derive(Clone, Debug)]
pub struct EventEnvelope<E> {
    pub event: E,
    /// The `eventlog.events` row's id: the same on every retry, a natural idempotency key.
    pub event_id: Uuid,
    pub namespace: String,
    pub action_id: Uuid,
    pub action_name: String,
    /// The action's params, as JSON text.
    pub action_params: String,
    pub started_date: Instant,
    pub completion_date: Instant,
    pub model_id: Option<String>,
    pub model_type: Option<String>,
    pub event_date: Instant,
}

/// Reacts to one event type: Java's `EventHandler<E>`.
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

/// A handler, whatever its event type.
pub(crate) trait AnyEventHandler: Send + Sync {
    fn event_type(&self) -> &'static str;
    fn handle<'a>(
        &'a self,
        notification: &'a EventNotification,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

struct Erased<H>(H);

impl<H: AsyncEventHandler> AnyEventHandler for Erased<H> {
    fn event_type(&self) -> &'static str {
        event_type_name::<H::Event>()
    }

    fn handle<'a>(&'a self, n: &'a EventNotification) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let payload = n
                .payload
                .as_deref()
                .ok_or_else(|| Error::Unexpected(format!("event {} has no payload", n.event_id)))?;
            let event: H::Event = serde_json::from_str(payload)?;
            self.0
                .handle(EventEnvelope {
                    event,
                    event_id: n.event_id,
                    namespace: n.namespace.clone(),
                    action_id: n.action_id,
                    action_name: n.action_name.clone(),
                    action_params: n.action_params.clone(),
                    started_date: n.started_date,
                    completion_date: n.completion_date,
                    model_id: n.model_id.clone(),
                    model_type: n.model_type.clone(),
                    event_date: n.event_date,
                })
                .await
        })
    }
}

/// Every handler, by name and by event type: Java's `EventHandlerRegistry`.
#[derive(Clone, Default)]
pub struct EventHandlerRegistry {
    by_name: HashMap<String, Arc<dyn AnyEventHandler>>,
    /// Event type -> the handlers' current names, in registration order.
    subscribers: Vec<(&'static str, Vec<String>)>,
}

impl EventHandlerRegistry {
    pub fn builder() -> EventHandlerRegistryBuilder {
        EventHandlerRegistryBuilder::default()
    }

    /// The event types with at least one handler.
    pub fn handled_event_types(&self) -> Vec<String> {
        self.subscribers
            .iter()
            .map(|(t, _)| (*t).to_owned())
            .collect()
    }

    /// The names of the handlers of `event_type`.
    pub fn subscribers_for(&self, event_type: &str) -> &[String] {
        self.subscribers
            .iter()
            .find(|(t, _)| *t == event_type)
            .map_or(&[], |(_, names)| names.as_slice())
    }

    pub fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    pub(crate) fn handler_for(&self, name: &str) -> Option<&dyn AnyEventHandler> {
        self.by_name.get(name).map(|h| &**h)
    }
}

#[derive(Default)]
pub struct EventHandlerRegistryBuilder {
    handlers: Vec<(String, Vec<String>, Arc<dyn AnyEventHandler>)>,
}

impl EventHandlerRegistryBuilder {
    /// Adds a handler. Its `name()` and `aliases()` are asked once, here.
    pub fn with_handler<H: AsyncEventHandler>(mut self, handler: H) -> Self {
        let name = handler.name();
        let aliases = handler.aliases();
        self.handlers
            .push((name, aliases, Arc::new(Erased(handler))));
        self
    }

    /// Names and aliases must be non-blank and unique across all handlers.
    pub fn build(self) -> Result<EventHandlerRegistry, Error> {
        let mut registry = EventHandlerRegistry::default();
        for (name, aliases, handler) in self.handlers {
            for key in std::iter::once(&name).chain(&aliases) {
                if key.trim().is_empty() {
                    return Err(Error::Config(
                        "an AsyncEventHandler name or alias is blank".to_owned(),
                    ));
                }
                if registry.by_name.contains_key(key) {
                    return Err(Error::Config(format!(
                        "AsyncEventHandler name or alias '{key}' is used twice"
                    )));
                }
                registry.by_name.insert(key.clone(), handler.clone());
            }
            let event_type = handler.event_type();
            match registry
                .subscribers
                .iter_mut()
                .find(|(t, _)| *t == event_type)
            {
                Some((_, names)) => names.push(name),
                None => registry.subscribers.push((event_type, vec![name])),
            }
        }
        Ok(registry)
    }
}
