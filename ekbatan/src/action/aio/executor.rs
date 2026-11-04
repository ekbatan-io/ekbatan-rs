//! Java's `ActionExecutor`: runs an action, then writes its plan and events, one transaction per
//! shard, retrying as configured.

use std::any::TypeId;
use std::sync::Arc;

use opentelemetry::KeyValue;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::action::config::ExecutionConfiguration;
use crate::action::event_persister::{EmittedEvent, EventPersister, SingleTableJsonEventPersister};
use crate::action::plan::{ActionPlan, AnyChanges};
use crate::action::{ActionRegistry, action_name};
use crate::aio::AsyncAction;
use crate::repository::RepositoryRegistry;
use crate::telemetry::{FutureExt, Span, transaction_span};
use crate::{Clock, DatabaseRegistry, Error, Instant, ShardIdentifier, SystemClock};

type ShardChanges = Vec<(TypeId, Box<dyn AnyChanges>)>;

/// Runs actions: Java's `ActionExecutor`, the framework's entry point.
///
/// Each call makes a fresh `ActionPlan`, runs `AsyncAction::perform` on it, groups what it staged by
/// shard (each repository's `ShardingStrategy`), and writes each shard's rows and its
/// `eventlog.events` rows in one transaction, so the event log always matches the data.
///
/// An action touching two shards is refused (`Error::CrossShard`) unless the configuration
/// allows it; then each shard commits on its own (no two-phase commit). A failure the
/// configuration retries starts again from a new plan.
pub struct AsyncActionExecutor {
    namespace: String,
    database_registry: Arc<DatabaseRegistry>,
    action_registry: ActionRegistry,
    repository_registry: RepositoryRegistry,
    /// The event persister in use; public as in Java, for writing events outside an action.
    pub event_persister: Arc<dyn EventPersister>,
    clock: Arc<dyn Clock>,
    default_execution_configuration: ExecutionConfiguration,
}

impl AsyncActionExecutor {
    pub fn builder() -> AsyncActionExecutorBuilder {
        AsyncActionExecutorBuilder::default()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Runs `A` with the executor's default `ExecutionConfiguration`.
    pub async fn execute<A: AsyncAction>(
        &self,
        principal: Option<&str>,
        params: A::Params,
    ) -> Result<A::Result, Error> {
        self.execute_with::<A>(principal, params, &self.default_execution_configuration)
            .await
    }

    /// Runs `A` with a configuration for this call only.
    pub async fn execute_with<A: AsyncAction>(
        &self,
        principal: Option<&str>,
        params: A::Params,
        execution_configuration: &ExecutionConfiguration,
    ) -> Result<A::Result, Error> {
        let action = self.action_registry.get::<A>()?;
        let name = action_name::<A>();
        let principal_name = principal.unwrap_or("");
        let start_time = self.clock.instant();
        info!("Executing {name} [principal={principal_name}]");
        let span = Span::start(
            "ekbatan.action.execute",
            vec![
                KeyValue::new("ekbatan.action.name", name),
                KeyValue::new("ekbatan.action.principal", principal_name.to_owned()),
            ],
        );
        let mut retry_count = 0;
        loop {
            let attempt = self
                .attempt(&*action, name, principal, &params, execution_configuration)
                .with_context(span.context())
                .await;
            let error = match attempt {
                Ok(result) => {
                    // Set once, with the final count: the SDK keeps every value set.
                    span.set("ekbatan.action.retry.count", i64::from(retry_count));
                    span.set("ekbatan.action.outcome", "success");
                    let took = self.clock.instant() - start_time;
                    info!(
                        "{name} completed in {}ms [principal={principal_name}]",
                        took.num_milliseconds()
                    );
                    return Ok(result);
                }
                Err(error) => error,
            };
            match execution_configuration.retry_config_for(&error) {
                Some((on, config)) if retry_count < config.max_retries => {
                    retry_count += 1;
                    warn!(
                        "Retrying {name} (retry {retry_count}/{}) due to {}: {error}",
                        config.max_retries,
                        on.name()
                    );
                    tokio::time::sleep(config.delay).await;
                }
                _ => {
                    span.set("ekbatan.action.retry.count", i64::from(retry_count));
                    span.set("ekbatan.action.outcome", "error");
                    span.fail(&error);
                    error!("{name} failed: {error}");
                    return Err(error);
                }
            }
        }
    }

    async fn attempt<A: AsyncAction>(
        &self,
        action: &A,
        name: &'static str,
        principal: Option<&str>,
        params: &A::Params,
        execution_configuration: &ExecutionConfiguration,
    ) -> Result<A::Result, Error> {
        let action_start_date = self.clock.instant();
        let mut plan = ActionPlan::new();
        let result = {
            let span = Span::start("ekbatan.action.perform", Vec::new());
            let performed = action
                .perform(&mut plan, principal, params)
                .with_context(span.context())
                .await;
            span.finish(performed)?
        };
        let span = Span::start("ekbatan.action.persist", Vec::new());
        let persisted = self
            .persist_changes(
                name,
                plan,
                params,
                action_start_date,
                execution_configuration,
                &span,
            )
            .with_context(span.context())
            .await;
        span.finish(persisted)?;
        Ok(result)
    }

    async fn persist_changes<P: serde::Serialize>(
        &self,
        name: &'static str,
        plan: ActionPlan,
        params: &P,
        action_start_date: Instant,
        execution_configuration: &ExecutionConfiguration,
        span: &Span,
    ) -> Result<(), Error> {
        let action_params = serialize_action_params(params)?;
        let mut changes_by_shard = self.group_changes_by_shard(plan)?;
        if !changes_by_shard.is_empty() {
            debug!(
                "Resolved changes across {} shards [shards={:?}]",
                changes_by_shard.len(),
                shard_list(&changes_by_shard)
            );
        }
        if changes_by_shard.len() > 1 {
            if !execution_configuration.is_cross_shard_allowed() {
                error!(
                    "{name} rejected: spans {} shards {:?} but cross-shard is disabled",
                    changes_by_shard.len(),
                    shard_list(&changes_by_shard)
                );
                return Err(Error::CrossShard {
                    first: changes_by_shard[0].0,
                    second: changes_by_shard[1].0,
                });
            }
            span.set("ekbatan.shard.cross_shard", true);
            warn!(
                "{name} spans {} shards: {:?} [allowCrossShard=true]",
                changes_by_shard.len(),
                shard_list(&changes_by_shard)
            );
        }
        if changes_by_shard.is_empty() {
            changes_by_shard.push((self.database_registry.default_shard(), Vec::new()));
        }
        let action_event_id = Uuid::new_v4();
        let mut committed_shards = Vec::new();
        for (shard, shard_changes) in &changes_by_shard {
            let written = self
                .persist_shard(
                    name,
                    &action_params,
                    action_start_date,
                    *shard,
                    shard_changes,
                    action_event_id,
                )
                .await;
            if let Err(error) = written {
                if !committed_shards.is_empty() {
                    error!(
                        "CRITICAL: Cross-shard action {name} PARTIALLY COMMITTED! Committed \
                         shards: {committed_shards:?}, Failed shard: {shard}. Error: {error}. \
                         Data on committed shards remains persisted and will NOT roll back \
                         automatically."
                    );
                    span.set("ekbatan.shard.partial_commit_failure", true);
                    let committed: Vec<String> =
                        committed_shards.iter().map(ToString::to_string).collect();
                    span.set(
                        "ekbatan.shard.committed_shards",
                        format!("[{}]", committed.join(", ")),
                    );
                    span.set("ekbatan.shard.failed_shard", shard.to_string());
                }
                return Err(error);
            }
            committed_shards.push(*shard);
        }
        Ok(())
    }

    /// One shard's transaction: Java's `ChangePersister.persist` inside `inTransactionChecked`.
    async fn persist_shard(
        &self,
        name: &'static str,
        action_params: &str,
        action_start_date: Instant,
        shard: ShardIdentifier,
        changes: &ShardChanges,
        action_event_id: Uuid,
    ) -> Result<(), Error> {
        let span = transaction_span(shard);
        let written = self
            .persist_shard_in_transaction(
                name,
                action_params,
                action_start_date,
                shard,
                changes,
                action_event_id,
            )
            .with_context(span.context())
            .await;
        span.finish(written)
    }

    async fn persist_shard_in_transaction(
        &self,
        name: &'static str,
        action_params: &str,
        action_start_date: Instant,
        shard: ShardIdentifier,
        changes: &ShardChanges,
        action_event_id: Uuid,
    ) -> Result<(), Error> {
        let transaction_manager = self.database_registry.transaction_manager(shard)?;
        let mut tx = transaction_manager.as_async().begin().await?;
        let written = async {
            let action_completion_date = self.clock.instant();
            let additions: usize = changes.iter().map(|(_, c)| c.addition_count()).sum();
            let updates: usize = changes.iter().map(|(_, c)| c.update_count()).sum();
            debug!("Persisting {additions} additions, {updates} updates");
            for (type_id, type_changes) in changes {
                let repository = self
                    .repository_registry
                    .by_type(*type_id)
                    .ok_or_else(|| no_repository(type_changes.model_name()))?;
                repository.persist(&mut tx, &**type_changes).await?;
            }
            let with_ids: Vec<(&'static str, _)> = changes
                .iter()
                .map(|(_, c)| (c.model_name(), c.events_with_model_ids()))
                .collect();
            let model_events: Vec<EmittedEvent<'_>> = with_ids
                .iter()
                .flat_map(|(model_name, events)| {
                    events.iter().map(|(model_id, event)| EmittedEvent {
                        model_id,
                        model_name,
                        event: *event,
                    })
                })
                .collect();
            self.event_persister
                .persist_action_events(
                    &mut tx,
                    &self.namespace,
                    name,
                    action_start_date,
                    action_completion_date,
                    action_params,
                    &model_events,
                    shard,
                    action_event_id,
                )
                .await?;
            debug!("Persisted events for action {name} [actionEventId={action_event_id}]");
            Ok::<(), Error>(())
        }
        .await;
        match written {
            Ok(()) => tx.commit().await,
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    fn group_changes_by_shard(
        &self,
        plan: ActionPlan,
    ) -> Result<Vec<(ShardIdentifier, ShardChanges)>, Error> {
        let mut result: Vec<(ShardIdentifier, ShardChanges)> = Vec::new();
        for (type_id, changes) in plan.into_changes() {
            if !changes.has_changes() {
                continue;
            }
            let repository = self
                .repository_registry
                .by_type(type_id)
                .ok_or_else(|| no_repository(changes.model_name()))?;
            for (shard, part) in repository.group_by_shard(&*changes)? {
                match result.iter_mut().find(|(s, _)| *s == shard) {
                    Some((_, shard_changes)) => shard_changes.push((type_id, part)),
                    None => result.push((shard, vec![(type_id, part)])),
                }
            }
        }
        Ok(result)
    }
}

fn no_repository(model_name: &str) -> Error {
    Error::Unexpected(format!(
        "No repository found for entity type: {model_name}. Register one with \
         RepositoryRegistry::builder().with_repository(...) - an action cannot stage a change \
         to a type the executor has no way to write."
    ))
}

fn shard_list(changes_by_shard: &[(ShardIdentifier, ShardChanges)]) -> Vec<ShardIdentifier> {
    changes_by_shard.iter().map(|(shard, _)| *shard).collect()
}

fn serialize_action_params<P: serde::Serialize>(params: &P) -> Result<String, Error> {
    let json = crate::json::to_string(params)?;
    if !json.starts_with('{') {
        return Err(Error::Invalid(
            "actionParams must serialize to a JSON object; use a struct params object, not \
             null/scalar/list"
                .to_owned(),
        ));
    }
    Ok(json)
}

/// Dot-separated identifiers, like a Java package: Java's namespace rule. The namespace names
/// Kafka topics and schema packages downstream, where a hyphen is a syntax error.
fn require_valid_namespace(namespace: Option<String>) -> Result<String, Error> {
    let namespace = namespace
        .filter(|n| !n.trim().is_empty())
        .ok_or_else(|| Error::Invalid("namespace is required".to_owned()))?;
    let valid = namespace.split('.').all(|segment| {
        let mut chars = segment.chars();
        matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    });
    if !valid {
        return Err(Error::Invalid(format!(
            "namespace '{namespace}' must be dot-separated identifiers, like a Java package \
             (letters, digits and underscore; each segment starting with a letter or \
             underscore). Hyphens are not legal in a protobuf package or Avro namespace, and \
             the namespace is used as one - write 'my_service' rather than 'my-service'."
        )));
    }
    Ok(namespace)
}

/// Java's `ActionExecutor.Builder`.
#[derive(Default)]
pub struct AsyncActionExecutorBuilder {
    namespace: Option<String>,
    database_registry: Option<Arc<DatabaseRegistry>>,
    action_registry: Option<ActionRegistry>,
    repository_registry: Option<RepositoryRegistry>,
    event_persister: Option<Arc<dyn EventPersister>>,
    clock: Option<Arc<dyn Clock>>,
    default_execution_configuration: Option<ExecutionConfiguration>,
}

impl AsyncActionExecutorBuilder {
    /// Recorded on every event row. Required.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn database_registry(mut self, database_registry: Arc<DatabaseRegistry>) -> Self {
        self.database_registry = Some(database_registry);
        self
    }

    pub fn action_registry(mut self, action_registry: ActionRegistry) -> Self {
        self.action_registry = Some(action_registry);
        self
    }

    pub fn repository_registry(mut self, repository_registry: RepositoryRegistry) -> Self {
        self.repository_registry = Some(repository_registry);
        self
    }

    /// Replaces the default single-table JSON persister.
    pub fn event_persister(mut self, event_persister: Arc<dyn EventPersister>) -> Self {
        self.event_persister = Some(event_persister);
        self
    }

    /// The clock for event times. Defaults to the system clock (UTC).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn default_execution_configuration(
        mut self,
        configuration: ExecutionConfiguration,
    ) -> Self {
        self.default_execution_configuration = Some(configuration);
        self
    }

    pub fn build(self) -> Result<AsyncActionExecutor, Error> {
        let required = |what: &str| Error::Invalid(format!("{what} is required"));
        Ok(AsyncActionExecutor {
            namespace: require_valid_namespace(self.namespace)?,
            database_registry: self
                .database_registry
                .ok_or_else(|| required("databaseRegistry"))?,
            action_registry: self
                .action_registry
                .ok_or_else(|| required("actionRegistry"))?,
            repository_registry: self
                .repository_registry
                .ok_or_else(|| required("repositoryRegistry"))?,
            event_persister: self
                .event_persister
                .unwrap_or_else(|| Arc::new(SingleTableJsonEventPersister::new())),
            clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
            default_execution_configuration: self
                .default_execution_configuration
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::require_valid_namespace;

    #[test]
    fn namespace_is_dot_separated_identifiers() {
        for good in ["wallet", "io.example.wallet", "_x.y_2"] {
            assert!(
                require_valid_namespace(Some(good.to_owned())).is_ok(),
                "{good}"
            );
        }
        for bad in ["", " ", "my-service", "a..b", "1abc", "a.", ".a"] {
            assert!(
                require_valid_namespace(Some(bad.to_owned())).is_err(),
                "{bad}"
            );
        }
        assert!(require_valid_namespace(None).is_err());
    }
}
