//! Java's `ActionExecutor`: runs an action, then writes its plan and events, one transaction per
//! shard, retrying as configured. It waits on `AsyncActionExecutor`, which does the work.

use std::sync::Arc;

use crate::action::{ActionRegistry, EventPersister, ExecutionConfiguration, RepositoryRegistry};
use crate::aio::{AsyncAction, AsyncActionExecutor, AsyncActionExecutorBuilder};
use crate::runtime::block_on;
use crate::{Clock, DatabaseRegistry, Error};

/// Runs actions: Java's `ActionExecutor`, the framework's entry point. It is the
/// `AsyncActionExecutor` (its async twin), waited on, so it runs sync and async actions alike.
pub struct ActionExecutor {
    inner: AsyncActionExecutor,
}

impl ActionExecutor {
    pub fn builder() -> ActionExecutorBuilder {
        ActionExecutorBuilder(AsyncActionExecutor::builder())
    }

    pub fn namespace(&self) -> &str {
        self.inner.namespace()
    }

    /// The async executor underneath (it holds the event persister).
    pub fn as_async(&self) -> &AsyncActionExecutor {
        &self.inner
    }

    /// Runs `A` with the executor's default `ExecutionConfiguration`.
    pub fn execute<A: AsyncAction>(
        &self,
        principal: Option<&str>,
        params: A::Params,
    ) -> Result<A::Result, Error> {
        block_on(self.inner.execute::<A>(principal, params))
    }

    /// Runs `A` with a configuration for this call only.
    pub fn execute_with<A: AsyncAction>(
        &self,
        principal: Option<&str>,
        params: A::Params,
        execution_configuration: &ExecutionConfiguration,
    ) -> Result<A::Result, Error> {
        block_on(
            self.inner
                .execute_with::<A>(principal, params, execution_configuration),
        )
    }
}

impl From<AsyncActionExecutor> for ActionExecutor {
    fn from(executor: AsyncActionExecutor) -> Self {
        ActionExecutor { inner: executor }
    }
}

/// Java's `ActionExecutor.Builder`.
pub struct ActionExecutorBuilder(AsyncActionExecutorBuilder);

impl ActionExecutorBuilder {
    /// Required: the event log's `namespace` column.
    pub fn namespace(self, namespace: impl Into<String>) -> Self {
        ActionExecutorBuilder(self.0.namespace(namespace))
    }

    pub fn database_registry(self, database_registry: Arc<DatabaseRegistry>) -> Self {
        ActionExecutorBuilder(self.0.database_registry(database_registry))
    }

    pub fn action_registry(self, action_registry: ActionRegistry) -> Self {
        ActionExecutorBuilder(self.0.action_registry(action_registry))
    }

    pub fn repository_registry(self, repository_registry: RepositoryRegistry) -> Self {
        ActionExecutorBuilder(self.0.repository_registry(repository_registry))
    }

    pub fn event_persister(self, event_persister: Arc<dyn EventPersister>) -> Self {
        ActionExecutorBuilder(self.0.event_persister(event_persister))
    }

    pub fn clock(self, clock: Arc<dyn Clock>) -> Self {
        ActionExecutorBuilder(self.0.clock(clock))
    }

    pub fn default_execution_configuration(self, configuration: ExecutionConfiguration) -> Self {
        ActionExecutorBuilder(self.0.default_execution_configuration(configuration))
    }

    pub fn build(self) -> Result<ActionExecutor, Error> {
        self.0.build().map(ActionExecutor::from)
    }
}
