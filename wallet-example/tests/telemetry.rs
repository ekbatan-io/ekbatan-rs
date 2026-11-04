//! OpenTelemetry on real databases: with an SDK installed, the executor, transactions,
//! repositories and event log emit Java's spans (names, attributes, nesting, error status), and
//! the local event handler's jobs emit Java's metrics. One test, so the process-wide providers
//! serve it alone.

mod common;

use std::sync::Arc;
use std::time::Duration;

use ekbatan::action::{ActionPlan, ActionRegistry, ExecutionConfiguration, RetryConfig, RetryOn};
use ekbatan::aio::{AsyncActionExecutor, AsyncEventHandler, AsyncRepository};
use ekbatan::blocking::{self, Action};
use ekbatan::local_event_handler::{
    EventEnvelope, EventFanoutJob, EventHandlerRegistry, EventHandlingJob,
};
use ekbatan::repository::RepositoryRegistry;
use ekbatan::{DatabaseRegistry, Dialect, Error, Model, Persistable, ShardedId, SystemClock};
use opentelemetry::trace::{SpanId, Status};
use opentelemetry::{KeyValue, Value, global};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider,
    Temporality,
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;
use wallet_example::action::{
    CreateNotificationAction, WalletCreateAction, WalletDepositMoneyAction,
    create_notification_action, wallet_create_action, wallet_deposit_money_action,
};
use wallet_example::model::{Wallet, WalletCreatedEvent, WalletMoneyDepositedEvent};
use wallet_example::repository::{NotificationRepository, WalletRepository};

use common::{GLOBAL, MEXICO, start};

#[tokio::test(flavor = "multi_thread")]
async fn spans_and_metrics_on_every_database() {
    let spans = InMemorySpanExporter::default();
    let tracer_provider = SdkTracerProvider::builder()
        .with_simple_exporter(spans.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());
    let metrics = InMemoryMetricExporterBuilder::new()
        .with_temporality(Temporality::Delta)
        .build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(metrics.clone()).build())
        .build();
    global::set_meter_provider(meter_provider.clone());

    for dialect in [Dialect::Postgres, Dialect::MySql, Dialect::MariaDb] {
        spans.reset();
        metrics.reset();
        run(dialect, &spans, &metrics, &meter_provider).await;
    }
}

/// Goes stale on its first try (someone else saves the wallet first), then succeeds.
struct StaleOnce {
    wallets: Arc<WalletRepository>,
    calls: std::sync::atomic::AtomicU32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StaleParams {
    wallet_id: ShardedId<Wallet>,
}

impl Action for StaleOnce {
    type Params = StaleParams;
    type Result = ();

    fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &StaleParams,
    ) -> Result<(), Error> {
        use ekbatan::blocking::Repository;
        let wallet = Repository::get_by_id(&*self.wallets, &params.wallet_id)?;
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            Repository::update_no_result(&*self.wallets, None, &wallet.copy().build()?)?;
        }
        plan.update(wallet.deposit(Decimal::ONE)?)?;
        Ok(())
    }
}

/// Opens one wallet on each shard.
struct OpenOnBothShards;

#[derive(Serialize)]
struct NoParams {}

impl Action for OpenOnBothShards {
    type Params = NoParams;
    type Result = ();

    fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        _params: &NoParams,
    ) -> Result<(), Error> {
        for shard in [GLOBAL, MEXICO] {
            let wallet =
                Wallet::create_wallet(shard, Uuid::new_v4(), "EUR", Decimal::ONE, now()).build()?;
            plan.add(wallet)?;
        }
        Ok(())
    }
}

fn now() -> ekbatan::Instant {
    ekbatan::Clock::instant(&SystemClock)
}

struct Delivered;

impl AsyncEventHandler for Delivered {
    type Event = WalletMoneyDepositedEvent;

    fn name(&self) -> String {
        "telemetry-delivered".to_owned()
    }

    async fn handle(&self, _: EventEnvelope<WalletMoneyDepositedEvent>) -> Result<(), Error> {
        Ok(())
    }
}

struct Refuses;

impl AsyncEventHandler for Refuses {
    type Event = WalletCreatedEvent;

    fn name(&self) -> String {
        "telemetry-refuses".to_owned()
    }

    async fn handle(&self, _: EventEnvelope<WalletCreatedEvent>) -> Result<(), Error> {
        Err(Error::Unexpected("refused".to_owned()))
    }
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    let mut values = span.attributes.iter().filter(|kv| kv.key.as_str() == key);
    let value = values.next().map(|kv| &kv.value);
    assert!(values.next().is_none(), "{key} set twice on {}", span.name);
    value
}

fn named<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
    spans.iter().filter(|s| s.name == name).collect()
}

fn child_of(child: &SpanData, parent: &SpanData) -> bool {
    child.parent_span_id == parent.span_context.span_id()
        && child.span_context.trace_id() == parent.span_context.trace_id()
}

/// The data points of `name` since the last reset, as (attributes, value or count).
fn points(metrics: &InMemoryMetricExporter, name: &str) -> Vec<(Vec<KeyValue>, f64)> {
    let mut found = Vec::new();
    for resource in metrics.get_finished_metrics().unwrap() {
        for scope in resource.scope_metrics() {
            assert_eq!(scope.scope().name(), "io.ekbatan.events.localeventhandler");
            for metric in scope.metrics().filter(|m| m.name() == name) {
                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for p in sum.data_points() {
                            found.push((p.attributes().cloned().collect(), p.value() as f64));
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        for p in histogram.data_points() {
                            found.push((p.attributes().cloned().collect(), p.count() as f64));
                        }
                    }
                    other => panic!("{name}: unexpected data {other:?}"),
                }
            }
        }
    }
    found
}

fn has(attributes: &[KeyValue], key: &str, value: &str) -> bool {
    attributes
        .iter()
        .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
}

async fn run(
    dialect: Dialect,
    spans: &InMemorySpanExporter,
    metrics: &InMemoryMetricExporter,
    meter_provider: &SdkMeterProvider,
) {
    let say = |what: &str| println!("[{dialect:?}] {what}");
    let cluster = start(dialect).await;
    let registry: Arc<DatabaseRegistry> = cluster.registry().await;
    let wallets = Arc::new(WalletRepository::new(registry.clone()));
    let executor = AsyncActionExecutor::builder()
        .namespace("io.example.wallet")
        .database_registry(registry.clone())
        .action_registry(
            ActionRegistry::builder()
                .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                .with_action(WalletDepositMoneyAction::new(wallets.clone()))
                .with_action(CreateNotificationAction)
                .with_action(OpenOnBothShards)
                .with_action(StaleOnce {
                    wallets: wallets.clone(),
                    calls: Default::default(),
                })
                .build(),
        )
        .repository_registry(
            RepositoryRegistry::builder()
                .with_repository(wallets.clone())
                .with_repository(Arc::new(NotificationRepository::new(registry.clone())))
                .build(),
        )
        .build()
        .unwrap();

    // A successful action: execute > perform, persist > transaction > repository, event.persist.
    let wallet = executor
        .execute::<WalletCreateAction>(
            Some("alice"),
            wallet_create_action::Params {
                country_code: None,
                owner_id: Uuid::new_v4(),
                currency: "EUR".to_owned(),
                initial_balance: Decimal::TEN,
            },
        )
        .await
        .unwrap();
    let finished = spans.get_finished_spans().unwrap();
    let execute = named(&finished, "ekbatan.action.execute");
    assert_eq!(execute.len(), 1, "{finished:#?}");
    let execute = execute[0];
    assert_eq!(execute.instrumentation_scope.name(), "io.ekbatan.core");
    assert_eq!(
        attribute(execute, "ekbatan.action.name").map(Value::as_str),
        Some("WalletCreateAction".into())
    );
    assert_eq!(
        attribute(execute, "ekbatan.action.principal").map(Value::as_str),
        Some("alice".into())
    );
    assert_eq!(
        attribute(execute, "ekbatan.action.outcome").map(Value::as_str),
        Some("success".into())
    );
    assert_eq!(
        attribute(execute, "ekbatan.action.retry.count"),
        Some(&Value::I64(0))
    );
    assert_eq!(execute.parent_span_id, SpanId::INVALID);
    let perform = named(&finished, "ekbatan.action.perform")[0];
    let persist = named(&finished, "ekbatan.action.persist")[0];
    let transaction = named(&finished, "ekbatan.transaction")[0];
    let insert = named(&finished, "ekbatan.repository")[0];
    let events = named(&finished, "ekbatan.event.persist")[0];
    assert!(child_of(perform, execute) && child_of(persist, execute));
    assert!(child_of(transaction, persist));
    assert!(child_of(insert, transaction) && child_of(events, transaction));
    assert_eq!(
        attribute(transaction, "ekbatan.shard.group"),
        Some(&Value::I64(i64::from(GLOBAL.group)))
    );
    assert_eq!(
        attribute(insert, "db.operation.name").map(Value::as_str),
        Some("BATCH_INSERT".into())
    );
    assert_eq!(
        attribute(insert, "ekbatan.entity.type").map(Value::as_str),
        Some("Wallet".into())
    );
    assert_eq!(
        attribute(insert, "ekbatan.batch.size"),
        Some(&Value::I64(1))
    );
    assert_eq!(
        attribute(events, "ekbatan.event.count"),
        Some(&Value::I64(1))
    );
    assert!(finished.iter().all(|s| s.status != Status::error("")));
    say(
        "an action: execute > perform, persist > transaction > repository (BATCH_INSERT) and \
         event.persist, with Java's attributes",
    );

    // A failing action: outcome error, ERROR status and the error on execute and perform.
    spans.reset();
    executor
        .execute::<CreateNotificationAction>(
            None,
            create_notification_action::Params {
                wallet_id: Uuid::new_v4(),
                kind: "DEPOSIT".to_owned(),
                recipient: " ".to_owned(),
                message: "hi".to_owned(),
            },
        )
        .await
        .unwrap_err();
    let finished = spans.get_finished_spans().unwrap();
    let execute = named(&finished, "ekbatan.action.execute")[0];
    let perform = named(&finished, "ekbatan.action.perform")[0];
    assert_eq!(
        attribute(execute, "ekbatan.action.outcome").map(Value::as_str),
        Some("error".into())
    );
    for span in [execute, perform] {
        assert!(
            matches!(&span.status, Status::Error { description } if description.contains("recipient cannot be blank")),
            "{:?}",
            span.status
        );
        assert!(span.events.iter().any(|e| e.name == "exception"));
    }
    assert!(named(&finished, "ekbatan.action.persist").is_empty());
    say("a failing action: outcome error, ERROR status and the exception on execute and perform");

    // A retried blocking action: retry.count 1, two performs, both under the one execute span.
    spans.reset();
    executor
        .execute_with::<StaleOnce>(
            None,
            StaleParams {
                wallet_id: *wallet.id(),
            },
            &ExecutionConfiguration::builder()
                .with_retry(
                    RetryOn::stale_record(),
                    RetryConfig::new(1, Duration::from_millis(1)),
                )
                .build(),
        )
        .await
        .unwrap();
    let finished = spans.get_finished_spans().unwrap();
    let execute = named(&finished, "ekbatan.action.execute")[0];
    assert_eq!(
        attribute(execute, "ekbatan.action.retry.count"),
        Some(&Value::I64(1))
    );
    let performs = named(&finished, "ekbatan.action.perform");
    assert_eq!(performs.len(), 2);
    assert!(performs.iter().all(|p| child_of(p, execute)));
    let persists = named(&finished, "ekbatan.action.persist");
    assert_eq!(persists.len(), 2);
    assert!(
        persists
            .iter()
            .any(|p| matches!(p.status, Status::Error { .. }))
    );
    let update = named(&finished, "ekbatan.repository");
    assert!(
        update
            .iter()
            .any(|s| attribute(s, "db.operation.name").map(Value::as_str)
                == Some("BATCH_UPDATE".into())),
        "{update:#?}"
    );
    say("a retried blocking action: retry.count 1, both attempts under one execute span");

    // Cross-shard, allowed: the persist span says so, with one transaction span per shard.
    spans.reset();
    executor
        .execute_with::<OpenOnBothShards>(
            None,
            NoParams {},
            &ExecutionConfiguration::builder()
                .allow_cross_shard(true)
                .build(),
        )
        .await
        .unwrap();
    let finished = spans.get_finished_spans().unwrap();
    let persist = named(&finished, "ekbatan.action.persist")[0];
    assert_eq!(
        attribute(persist, "ekbatan.shard.cross_shard"),
        Some(&Value::Bool(true))
    );
    let groups: Vec<_> = named(&finished, "ekbatan.transaction")
        .iter()
        .map(|t| attribute(t, "ekbatan.shard.group").cloned())
        .collect();
    assert_eq!(
        groups,
        vec![
            Some(Value::I64(i64::from(GLOBAL.group))),
            Some(Value::I64(i64::from(MEXICO.group)))
        ]
    );
    say("cross-shard, allowed: cross_shard on persist, a transaction span per shard");

    // in_transaction, async and blocking: an ekbatan.transaction span each.
    spans.reset();
    let manager = registry.transaction_manager(GLOBAL).unwrap();
    manager
        .as_async()
        .in_transaction(async |tx| wallets.count_where_in(tx).await)
        .await
        .unwrap();
    let blocking_registry = registry.clone();
    tokio::task::spawn_blocking(move || {
        blocking_registry
            .transaction_manager(GLOBAL)
            .unwrap()
            .in_transaction(|_| Ok(()))
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        named(&spans.get_finished_spans().unwrap(), "ekbatan.transaction").len(),
        2
    );
    say("in_transaction, async and blocking: one ekbatan.transaction span each");

    // Metrics: fan-out counts events and notifications; handling counts outcomes per handler,
    // times each handler, and measures the delivery lag of successes.
    executor
        .execute::<WalletDepositMoneyAction>(
            None,
            wallet_deposit_money_action::Params {
                wallet_id: *wallet.id(),
                amount: Decimal::ONE,
                recipient: None,
            },
        )
        .await
        .unwrap();
    let handlers = || {
        EventHandlerRegistry::builder()
            .with_handler(Delivered)
            .with_handler(Refuses)
            .build()
            .unwrap()
    };
    let fanout = EventFanoutJob::new(registry.clone(), handlers());
    while fanout.drain_one_round().await.unwrap() {}
    let handling = EventHandlingJob::new(registry.clone(), handlers());
    assert!(handling.drain_one_round().await.unwrap());
    meter_provider.force_flush().unwrap();
    // Events: three creates (one action, then one on each shard), two deposits (StaleOnce's and
    // this one); the failed action wrote nothing. One notification per event: each event type
    // has one handler.
    let fanned: f64 = points(metrics, "ekbatan.events.fanned_out")
        .iter()
        .map(|p| p.1)
        .sum();
    let created: f64 = points(metrics, "ekbatan.events.notifications_created")
        .iter()
        .map(|p| p.1)
        .sum();
    assert_eq!((fanned, created), (5.0, 5.0));
    let handled = points(metrics, "ekbatan.events.handled");
    let count = |outcome: &str, handler: &str| -> f64 {
        handled
            .iter()
            .filter(|(a, _)| has(a, "outcome", outcome) && has(a, "handler", handler))
            .map(|p| p.1)
            .sum()
    };
    assert_eq!(
        count("succeeded", "telemetry-delivered"),
        2.0,
        "{handled:?}"
    );
    assert_eq!(
        count("failed_retry", "telemetry-refuses"),
        3.0,
        "{handled:?}"
    );
    let durations = points(metrics, "ekbatan.events.handler.duration");
    let timed = |handler: &str, outcome: &str| -> f64 {
        durations
            .iter()
            .filter(|(a, _)| has(a, "handler", handler) && has(a, "outcome", outcome))
            .map(|p| p.1)
            .sum()
    };
    assert_eq!(timed("telemetry-delivered", "succeeded"), 2.0);
    assert_eq!(timed("telemetry-refuses", "failed"), 3.0);
    let lag = points(metrics, "ekbatan.events.delivery.lag");
    assert_eq!(lag.iter().map(|p| p.1).sum::<f64>(), 2.0, "{lag:?}");
    assert!(
        lag.iter()
            .all(|(a, _)| has(a, "handler", "telemetry-delivered"))
    );
    say(
        "metrics: fanned_out 5, notifications_created 5, handled by outcome and handler, \
         handler.duration by outcome, delivery.lag for successes",
    );
    let _ = blocking::runtime();
    registry.as_async().close().await;
}

trait CountIn {
    async fn count_where_in(&self, tx: &mut ekbatan::aio::AsyncTransaction) -> Result<i64, Error>;
}

impl CountIn for WalletRepository {
    async fn count_where_in(&self, tx: &mut ekbatan::aio::AsyncTransaction) -> Result<i64, Error> {
        use sea_query::{Asterisk, Expr, Func, Query};
        let count = Query::select()
            .expr(Func::count(Expr::col(Asterisk)))
            .from(wallet_example::schema::Wallets::Table)
            .to_owned();
        AsyncRepository::tx_db(self, tx, GLOBAL)?
            .fetch_scalar::<i64>(&count)
            .await
    }
}
