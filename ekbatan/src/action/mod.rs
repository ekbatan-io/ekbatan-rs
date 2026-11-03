//! Actions: Java's `io.ekbatan.core.action`. An action stages changes on an `ActionPlan`; the
//! `ActionExecutor` writes them, with the events the models carry, in one transaction per shard.

pub(crate) mod config;
pub(crate) mod event_persister;
pub(crate) mod plan;
pub(crate) mod repository_registry;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

pub use config::{ExecutionConfiguration, RetryConfig, RetryOn};
pub use event_persister::{
    EmittedEvent, EventEntity, EventEntityRepository, EventPersister,
    SingleTableJsonEventPersister, event_payload,
};
pub(crate) use event_persister::{json_text, json_value};
pub use plan::{ActionPlan, AnyChanges, PersistableChanges};
pub use repository_registry::{AnyRepository, RepositoryRegistry, RepositoryRegistryBuilder};

use crate::Error;
use crate::aio::AsyncAction;

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
