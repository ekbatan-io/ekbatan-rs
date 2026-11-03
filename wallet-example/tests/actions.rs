//! The action executor on real databases: each action's rows and its event log rows commit
//! together, per shard; cross-shard, retry and failure behave as Java's.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ekbatan::action::{
    Action, ActionExecutor, ActionPlan, ActionRegistry, EventEntity, EventEntityRepository,
    ExecutionConfiguration, RepositoryRegistry, RetryConfig, RetryOn,
};
use ekbatan::{
    Clock, DatabaseRegistry, Dialect, Error, Model, Persistable, Repository, ShardIdentifier,
    SystemClock,
};
use rust_decimal::Decimal;
use serde::Serialize;
use uuid::Uuid;
use wallet_example::action::{
    CreateNotificationAction, WalletCloseAction, WalletCreateAction, WalletDepositMoneyAction,
    create_notification_action, wallet_close_action, wallet_create_action,
    wallet_deposit_money_action,
};
use wallet_example::model::{Wallet, WalletState};
use wallet_example::repository::{NotificationRepository, WalletRepository};

use common::{GLOBAL, MEXICO, start};

#[tokio::test(flavor = "multi_thread")]
async fn on_postgres() {
    run(Dialect::Postgres).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mysql() {
    run(Dialect::MySql).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mariadb() {
    run(Dialect::MariaDb).await;
}

struct App {
    registry: Arc<DatabaseRegistry>,
    wallets: Arc<WalletRepository>,
    executor: ActionExecutor,
    flaky_calls: Arc<AtomicU32>,
}

fn app(registry: Arc<DatabaseRegistry>) -> App {
    let wallets = Arc::new(WalletRepository::new(registry.clone()));
    let notifications = Arc::new(NotificationRepository::new(registry.clone()));
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let flaky_calls = Arc::new(AtomicU32::new(0));
    let actions = ActionRegistry::builder()
        .with_action(WalletCreateAction::new(clock.clone()))
        .with_action(WalletDepositMoneyAction::new(wallets.clone()))
        .with_action(WalletCloseAction::new(wallets.clone()))
        .with_action(CreateNotificationAction)
        .with_action(OpenOnBothShards)
        .with_action(StaleOnce {
            wallets: wallets.clone(),
        })
        .with_action(FailsTwice {
            calls: flaky_calls.clone(),
        })
        .with_action(DoesNothing)
        .with_action(StagesTwice)
        .build();
    let repositories = RepositoryRegistry::builder()
        .with_repository(wallets.clone())
        .with_repository(notifications)
        .build();
    let executor = ActionExecutor::builder()
        .namespace("io.example.wallet")
        .database_registry(registry.clone())
        .action_registry(actions)
        .repository_registry(repositories)
        .clock(clock)
        .build()
        .expect("an executor");
    App {
        registry,
        wallets,
        executor,
        flaky_calls,
    }
}

async fn run(dialect: Dialect) {
    let cluster = start(dialect).await;
    let app = app(cluster.registry().await);
    let say = |what: &str| println!("[{dialect:?}] {what}");
    let owner = Uuid::new_v4();

    // Create: the wallet and its event commit together, on the wallet's shard.
    let wallet = app
        .executor
        .execute::<WalletCreateAction>(
            Some("alice"),
            wallet_create_action::Params {
                country_code: Some("MX".to_owned()),
                owner_id: owner,
                currency: "MXN".to_owned(),
                initial_balance: Decimal::new(1000, 2),
            },
        )
        .await
        .unwrap();
    assert_eq!(wallet.id().shard(), MEXICO);
    assert_eq!(app.wallets.get_by_id(wallet.id()).await.unwrap(), wallet);
    let rows = events_where(&app.registry, MEXICO, |e| {
        e.model_id.as_deref() == Some(&*wallet.id().to_string())
    })
    .await;
    assert_eq!(rows.len(), 1);
    let created = &rows[0];
    assert_eq!(created.namespace, "io.example.wallet");
    assert_eq!(created.action_name, "WalletCreateAction");
    assert_eq!(created.model_type.as_deref(), Some("Wallet"));
    assert_eq!(created.event_type.as_deref(), Some("WalletCreatedEvent"));
    assert!(!created.delivered);
    assert!(created.started_date <= created.completion_date);
    assert_eq!(created.event_date, created.completion_date);
    let payload: serde_json::Value =
        serde_json::from_str(created.payload.as_deref().unwrap()).unwrap();
    assert_eq!(payload["modelId"], wallet.id().to_string());
    assert_eq!(payload["modelName"], "Wallet");
    assert_eq!(payload["ownerId"], owner.to_string());
    assert_eq!(payload["currency"], "MXN");
    let params: serde_json::Value = serde_json::from_str(&created.action_params).unwrap();
    assert_eq!(params["countryCode"], "MX");
    // MySQL's JSON type keeps numbers as doubles, so 10.00 comes back 10.0 there (Java's rows
    // too); PostgreSQL's jsonb and MariaDB keep the digits.
    let stored_number = if dialect == Dialect::MySql {
        "10.0"
    } else {
        "10.00"
    };
    assert_eq!(params["initialBalance"].to_string(), stored_number);
    if dialect == Dialect::MariaDb {
        // MariaDB keeps JSON as the text it was given: Java's bytes.
        let expected = format!(
            r#"{{"modelId":"{id}","modelName":"Wallet","ownerId":"{owner}","currency":"MXN","initialBalance":10.00}}"#,
            id = wallet.id()
        );
        assert_eq!(created.payload.as_deref(), Some(&*expected));
    }
    say("create: wallet and WalletCreatedEvent row commit together on the wallet's shard");

    // Deposit: an update, version 1 -> 2, and its event.
    let richer = app
        .executor
        .execute::<WalletDepositMoneyAction>(
            None,
            wallet_deposit_money_action::Params {
                wallet_id: *wallet.id(),
                amount: Decimal::new(550, 2),
                recipient: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(richer.version(), 2);
    let stored = app.wallets.get_by_id(wallet.id()).await.unwrap();
    assert_eq!(stored.version(), 2);
    assert_eq!(*stored.balance(), Decimal::new(1550, 2));
    let deposited = events_where(&app.registry, MEXICO, |e| {
        e.event_type.as_deref() == Some("WalletMoneyDepositedEvent")
    })
    .await;
    assert_eq!(deposited.len(), 1);
    let payload: serde_json::Value =
        serde_json::from_str(deposited[0].payload.as_deref().unwrap()).unwrap();
    let stored_number = if dialect == Dialect::MySql {
        "5.5"
    } else {
        "5.50"
    };
    assert_eq!(payload["amount"].to_string(), stored_number);
    assert!(payload["newBalance"].is_number());
    let params: serde_json::Value = serde_json::from_str(&deposited[0].action_params).unwrap();
    let id = wallet.id().to_string();
    assert_eq!(
        params["walletId"],
        serde_json::json!({"id": id, "value": id})
    );
    say("deposit: version 2, WalletMoneyDepositedEvent with Java's JSON (numbers, id objects)");

    // Close, twice: the second closes nothing new, so it emits nothing, but the action's row
    // is still written (Java writes one row with no event).
    let close = || {
        app.executor.execute::<WalletCloseAction>(
            None,
            wallet_close_action::Params {
                wallet_id: *wallet.id(),
            },
        )
    };
    assert_eq!(close().await.unwrap().state(), WalletState::Closed);
    let closed_again = close().await.unwrap();
    assert_eq!(closed_again.version(), 4);
    let close_rows = events_where(&app.registry, MEXICO, |e| {
        e.action_name == "WalletCloseAction"
    })
    .await;
    assert_eq!(close_rows.len(), 2);
    assert_eq!(
        close_rows
            .iter()
            .filter(|e| e.event_type.as_deref() == Some("WalletClosedEvent"))
            .count(),
        1
    );
    assert_eq!(
        close_rows.iter().filter(|e| e.event_type.is_none()).count(),
        1
    );
    say("close twice: one WalletClosedEvent, and one action row with no event");

    // An entity: written, with one action row and no event.
    let notification = app
        .executor
        .execute::<CreateNotificationAction>(
            None,
            create_notification_action::Params {
                wallet_id: wallet.id().uuid(),
                kind: "DEPOSIT".to_owned(),
                recipient: "alice@example.com".to_owned(),
                message: "hello".to_owned(),
            },
        )
        .await
        .unwrap();
    let rows = events_where(&app.registry, GLOBAL, |e| {
        e.action_name == "CreateNotificationAction"
    })
    .await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].model_id.is_none() && rows[0].payload.is_none());
    assert_eq!(notification.version(), 1);
    say("notification (entity): written, one action row, no event");

    // Cross-shard: refused by default, nothing written; allowed, each shard commits with the
    // same action id.
    let refused = app
        .executor
        .execute::<OpenOnBothShards>(None, OwnerParams { owner_id: owner })
        .await;
    assert!(
        matches!(refused, Err(Error::CrossShard { .. })),
        "{refused:?}"
    );
    let allow = ExecutionConfiguration::builder().allow_cross_shard(true);
    let opened = app
        .executor
        .execute_with::<OpenOnBothShards>(None, OwnerParams { owner_id: owner }, &allow)
        .await
        .unwrap();
    let global = events_where(&app.registry, GLOBAL, |e| {
        e.action_name == "OpenOnBothShards"
    })
    .await;
    let mexico = events_where(&app.registry, MEXICO, |e| {
        e.action_name == "OpenOnBothShards"
    })
    .await;
    assert_eq!(app.wallets.find_all_by_ids(&opened).await.unwrap().len(), 2);
    assert_eq!((global.len(), mexico.len()), (1, 1));
    assert_eq!(global[0].action_id, mexico[0].action_id);
    say("cross-shard: refused by default; allowed, both shards commit under one action id");

    // A stale record is retried once by default, from a new plan; with no retry it fails.
    let target = app.wallets.add(None, &open(GLOBAL, owner)).await.unwrap();
    let after = app
        .executor
        .execute::<StaleOnce>(
            None,
            WalletParams {
                wallet_id: target.id().uuid(),
            },
        )
        .await
        .unwrap();
    assert_eq!(*after.balance(), Decimal::new(2, 0));
    let no_retry = ExecutionConfiguration::builder().no_retry();
    let failed = app
        .executor
        .execute_with::<StaleOnce>(
            None,
            WalletParams {
                wallet_id: target.id().uuid(),
            },
            &no_retry,
        )
        .await;
    assert!(
        matches!(failed, Err(Error::StaleRecord { .. })),
        "{failed:?}"
    );
    say("stale record: retried from a new plan by default; fails with no_retry");

    // An app error, retried by type: up to max_retries, then it fails.
    let retry_flaky = ExecutionConfiguration::builder().with_retry(
        RetryOn::error::<Flaky>(),
        RetryConfig::new(2, Duration::from_millis(1)),
    );
    app.executor
        .execute_with::<FailsTwice>(None, OwnerParams { owner_id: owner }, &retry_flaky)
        .await
        .unwrap();
    assert_eq!(app.flaky_calls.load(Ordering::SeqCst), 3);
    app.flaky_calls.store(0, Ordering::SeqCst);
    let once = ExecutionConfiguration::builder().with_retry(
        RetryOn::error::<Flaky>(),
        RetryConfig::new(1, Duration::from_millis(1)),
    );
    let gave_up = app
        .executor
        .execute_with::<FailsTwice>(None, OwnerParams { owner_id: owner }, &once)
        .await
        .unwrap_err();
    assert!(gave_up.action_error::<Flaky>().is_some(), "{gave_up:?}");
    assert_eq!(app.flaky_calls.load(Ordering::SeqCst), 2);
    say("app error: retried by type up to max_retries, then returned");

    // Nothing staged: one action row on the default shard.
    app.executor
        .execute::<DoesNothing>(None, OwnerParams { owner_id: owner })
        .await
        .unwrap();
    let rows = events_where(&app.registry, GLOBAL, |e| e.action_name == "DoesNothing").await;
    assert_eq!(rows.len(), 1);
    say("no changes: one action row on the default shard");

    // The same row staged twice is refused, and nothing is written.
    let twice = app
        .executor
        .execute::<StagesTwice>(None, OwnerParams { owner_id: owner })
        .await;
    assert!(
        matches!(&twice, Err(Error::Unexpected(m)) if m.contains("already registered for addition")),
        "{twice:?}"
    );
    say("one row staged twice: refused");

    // Not registered: refused before anything runs.
    let bare = ActionExecutor::builder()
        .namespace("x")
        .database_registry(app.registry.clone())
        .action_registry(ActionRegistry::builder().build())
        .repository_registry(RepositoryRegistry::builder().build())
        .build()
        .unwrap();
    let missing = bare
        .execute::<DoesNothing>(None, OwnerParams { owner_id: owner })
        .await;
    assert!(matches!(missing, Err(Error::Invalid(_))), "{missing:?}");
    let hyphen = ActionExecutor::builder()
        .namespace("my-service")
        .database_registry(app.registry.clone())
        .action_registry(ActionRegistry::builder().build())
        .repository_registry(RepositoryRegistry::builder().build())
        .build();
    assert!(hyphen.is_err());
    say("unregistered action and hyphenated namespace: refused");

    // The event log write fails: the wallet written before it in the same transaction is
    // rolled back with it.
    let failing = ActionExecutor::builder()
        .namespace("io.example.wallet")
        .database_registry(app.registry.clone())
        .action_registry(
            ActionRegistry::builder()
                .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                .build(),
        )
        .repository_registry(
            RepositoryRegistry::builder()
                .with_repository(app.wallets.clone())
                .build(),
        )
        .event_persister(Arc::new(FailingEventPersister))
        .build()
        .unwrap();
    let lonely_owner = Uuid::new_v4();
    let failed = failing
        .execute::<WalletCreateAction>(
            None,
            wallet_create_action::Params {
                country_code: None,
                owner_id: lonely_owner,
                currency: "EUR".to_owned(),
                initial_balance: Decimal::ONE,
            },
        )
        .await;
    assert!(matches!(failed, Err(Error::Unexpected(_))), "{failed:?}");
    assert!(
        app.wallets
            .find_all_by_owner_id(lonely_owner)
            .await
            .unwrap()
            .is_empty()
    );
    say("event log write fails: the wallet is rolled back with it");
}

/// Writes the action's changes, then fails where the event rows would be written.
struct FailingEventPersister;

impl ekbatan::action::EventPersister for FailingEventPersister {
    fn persist_action_events<'a>(
        &'a self,
        _tx: &'a mut ekbatan::Transaction,
        _namespace: &'a str,
        _action_name: &'a str,
        _started_date: ekbatan::Instant,
        _completion_date: ekbatan::Instant,
        _action_params: &'a str,
        _model_events: &'a [ekbatan::action::EmittedEvent<'a>],
        _shard: ShardIdentifier,
        _action_event_id: Uuid,
    ) -> futures_util::future::BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { Err(Error::Unexpected("event log unavailable".to_owned())) })
    }
}

fn open(shard: ShardIdentifier, owner: Uuid) -> Wallet {
    Wallet::create_wallet(shard, owner, "EUR", Decimal::ONE, SystemClock.instant())
        .build()
        .unwrap()
}

/// The event rows on `shard` that `keep` accepts.
async fn events_where(
    registry: &DatabaseRegistry,
    shard: ShardIdentifier,
    keep: impl Fn(&EventEntity) -> bool,
) -> Vec<EventEntity> {
    let mut db = registry.primary(shard).unwrap();
    EventEntityRepository::find_all(&mut db)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| keep(e))
        .collect()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OwnerParams {
    owner_id: Uuid,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WalletParams {
    wallet_id: Uuid,
}

/// Opens one wallet on each shard.
struct OpenOnBothShards;

impl Action for OpenOnBothShards {
    type Params = OwnerParams;
    type Result = Vec<ekbatan::ShardedId<Wallet>>;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &OwnerParams,
    ) -> Result<Self::Result, Error> {
        let both = plan.add_all(vec![
            open(GLOBAL, params.owner_id),
            open(MEXICO, params.owner_id),
        ])?;
        Ok(both.iter().map(|w| *w.id()).collect())
    }
}

/// Reads a wallet, and on its first try someone else saves it in between: the update is stale.
struct StaleOnce {
    wallets: Arc<WalletRepository>,
}

impl Action for StaleOnce {
    type Params = WalletParams;
    type Result = Wallet;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &WalletParams,
    ) -> Result<Wallet, Error> {
        let id = ekbatan::ShardedId::from_uuid(params.wallet_id);
        let wallet = self.wallets.get_by_id(&id).await?;
        if *wallet.balance() == Decimal::ONE {
            // Someone else saves it first: our copy goes stale.
            let theirs = wallet.copy().balance(Decimal::new(2, 0)).build()?;
            self.wallets.update_no_result(None, &theirs).await?;
            return plan.update(wallet.copy().balance(Decimal::new(99, 0)).build()?);
        }
        if *wallet.balance() == Decimal::new(2, 0) && wallet.version() == 2 {
            return plan.update(wallet.copy().build()?);
        }
        // Third call (no_retry run): stale again.
        let theirs = wallet.copy().build()?;
        self.wallets.update_no_result(None, &theirs).await?;
        plan.update(wallet)
    }
}

#[derive(Debug)]
struct Flaky;

impl std::fmt::Display for Flaky {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("flaky")
    }
}

impl std::error::Error for Flaky {}

/// Fails twice with `Flaky`, then succeeds.
struct FailsTwice {
    calls: Arc<AtomicU32>,
}

impl Action for FailsTwice {
    type Params = OwnerParams;
    type Result = ();

    async fn perform(
        &self,
        _plan: &mut ActionPlan,
        _principal: Option<&str>,
        _params: &OwnerParams,
    ) -> Result<(), Error> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < 2 {
            return Err(Error::action(Flaky));
        }
        Ok(())
    }
}

struct DoesNothing;

impl Action for DoesNothing {
    type Params = OwnerParams;
    type Result = ();

    async fn perform(
        &self,
        _plan: &mut ActionPlan,
        _principal: Option<&str>,
        _params: &OwnerParams,
    ) -> Result<(), Error> {
        Ok(())
    }
}

struct StagesTwice;

impl Action for StagesTwice {
    type Params = OwnerParams;
    type Result = ();

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &OwnerParams,
    ) -> Result<(), Error> {
        let wallet = plan.add(open(GLOBAL, params.owner_id))?;
        plan.update(wallet)?;
        Ok(())
    }
}
