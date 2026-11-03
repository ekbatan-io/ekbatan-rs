use std::sync::Arc;

use serde::Serialize;

use super::block_on;
use crate::action::{
    ActionPlan, ActionRegistry, AsyncAction, AsyncActionExecutor, AsyncActionExecutorBuilder,
    EventPersister, ExecutionConfiguration, RepositoryRegistry,
};
use crate::{Clock, DatabaseRegistry, Error};

/// One unit of domain work: Java's `Action<PARAM, RESULT>`. One instance serves every call, so
/// it holds only what its constructor gave it (repositories, a clock); what one call stages goes
/// on the `plan` it is handed.
///
/// `perform` returns `Err` to abort: nothing it staged is written. An app's own error goes in
/// `Error::action(...)`. Every `Action` is also an `AsyncAction` (its async twin), so it
/// registers with `ActionRegistry::builder().with_action(...)` like any other.
pub trait Action: Send + Sync + 'static {
    /// The input. Written to the event log as JSON, so it must serialize to a JSON object.
    type Params: Serialize + Send + Sync;
    type Result: Send;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        principal: Option<&str>,
        params: &Self::Params,
    ) -> Result<Self::Result, Error>;
}

impl<A: Action> AsyncAction for A {
    type Params = A::Params;
    type Result = A::Result;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        principal: Option<&str>,
        params: &A::Params,
    ) -> Result<A::Result, Error> {
        tokio::task::block_in_place(|| Action::perform(self, plan, principal, params))
    }
}

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
