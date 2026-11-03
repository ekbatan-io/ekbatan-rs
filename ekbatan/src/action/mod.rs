//! Actions: Java's `io.ekbatan.core.action`. An action stages changes on an `ActionPlan`; the
//! `AsyncActionExecutor` writes them, with the events the models carry, in one transaction per shard.

mod config;
mod event_persister;
mod executor;
mod plan;
mod repository_registry;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use serde::Serialize;

pub use crate::sync_api::action::{Action, ActionExecutor, ActionExecutorBuilder};
pub use config::{ExecutionConfiguration, RetryConfig, RetryOn};
pub use event_persister::{
    EmittedEvent, EventEntity, EventEntityRepository, EventPersister,
    SingleTableJsonEventPersister, event_payload,
};
pub(crate) use event_persister::{json_text, json_value};
pub use executor::{AsyncActionExecutor, AsyncActionExecutorBuilder};
pub use plan::{ActionPlan, AnyChanges, PersistableChanges};
pub use repository_registry::{AnyRepository, RepositoryRegistry, RepositoryRegistryBuilder};

use crate::Error;

/// One unit of domain work: Java's `Action<PARAM, RESULT>`. One instance serves every call, so
/// it holds only what its constructor gave it (repositories, a clock); what one call stages goes
/// on the `plan` it is handed.
///
/// `perform` returns `Err` to abort: nothing it staged is written. An app's own error goes in
/// `Error::action(...)`.
pub trait AsyncAction: Send + Sync + 'static {
    /// The input. Written to the event log as JSON, so it must serialize to a JSON object.
    type Params: Serialize + Send + Sync;
    type Result: Send;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        principal: Option<&str>,
        params: &Self::Params,
    ) -> impl Future<Output = Result<Self::Result, Error>> + Send;
}

/// The simple name of an action type, as Java's `getSimpleName()`: the event log's
/// `action_name`.
pub fn action_name<A: AsyncAction>() -> &'static str {
    let full = std::any::type_name::<A>();
    full.rsplit("::").next().unwrap_or(full)
}

/// Every action, by type: Java's `ActionRegistry`.
#[derive(Clone, Default)]
pub struct ActionRegistry {
    actions: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl ActionRegistry {
    pub fn builder() -> ActionRegistryBuilder {
        ActionRegistryBuilder::default()
    }

    pub fn get<A: AsyncAction>(&self) -> Result<Arc<A>, Error> {
        self.actions
            .get(&TypeId::of::<A>())
            .and_then(|action| action.clone().downcast::<A>().ok())
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "No action registered for class {}",
                    action_name::<A>()
                ))
            })
    }
}

#[derive(Default)]
pub struct ActionRegistryBuilder {
    actions: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl ActionRegistryBuilder {
    pub fn with_action<A: AsyncAction>(mut self, action: A) -> Self {
        self.actions.insert(TypeId::of::<A>(), Arc::new(action));
        self
    }

    pub fn build(self) -> ActionRegistry {
        ActionRegistry {
            actions: self.actions,
        }
    }
}
