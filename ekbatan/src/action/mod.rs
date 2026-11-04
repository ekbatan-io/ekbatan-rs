//! Actions: Java's `io.ekbatan.core.action`. An action stages changes on an `ActionPlan`; the
//! executor writes them, with the events the models carry, in one transaction per shard.
//!
//! Shared here: `ActionPlan`, `ActionRegistry`, `ExecutionConfiguration`, and the event log
//! (`EventPersister`). The action and executor come in a blocking kind (`blocking::Action`,
//! `blocking::ActionExecutor`) and an async kind (`aio::AsyncAction`, `aio::AsyncActionExecutor`),
//! in `blocking` and `aio` here.

pub(crate) mod aio;
pub(crate) mod blocking;
pub(crate) mod config;
pub(crate) mod event_persister;
pub(crate) mod plan;
mod registry;

pub use config::{ExecutionConfiguration, RetryConfig, RetryOn};
pub use event_persister::{
    EmittedEvent, EventEntity, EventEntityRepository, EventPersister,
    SingleTableJsonEventPersister, event_payload,
};
pub(crate) use event_persister::{json_text, json_value};
pub use plan::{ActionPlan, AnyChanges, PersistableChanges};
pub use registry::{ActionRegistry, ActionRegistryBuilder, action_name};
