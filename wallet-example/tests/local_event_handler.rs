//! The local event handler on real databases: events written by actions reach the handlers
//! through the fan-out and handling jobs; failures retry with backoff; old ones expire.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::TimeDelta;
use ekbatan::action::{ActionExecutor, ActionRegistry, RepositoryRegistry};
use ekbatan::distributed_jobs::JobRegistry;
use ekbatan::local_event_handler::{
    EventEnvelope, EventFanoutJob, EventHandler, EventHandlerRegistry, EventHandlingJob,
    EventNotification, EventNotificationRepository, EventNotificationState,
};
use ekbatan::{
    Clock, ConnectionProvider, DatabaseRegistry, Dialect, Error, Persistable, ShardIdentifier,
    SystemClock,
};
use ekbatan_test_support::VirtualClock;
use rust_decimal::Decimal;
use uuid::Uuid;
use wallet_example::action::{
    WalletCloseAction, WalletCreateAction, WalletDepositMoneyAction, wallet_close_action,
    wallet_create_action, wallet_deposit_money_action,
};
use wallet_example::model::{
    Wallet, WalletClosedEvent, WalletCreatedEvent, WalletMoneyDepositedEvent,
};
use wallet_example::repository::WalletRepository;

use common::{MEXICO, start};

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

type Seen = Arc<Mutex<Vec<(Uuid, Decimal, Option<String>, String)>>>;

/// Records each deposit it is given.
struct Recorder {
    name: &'static str,
    seen: Seen,
}

impl EventHandler for Recorder {
    type Event = WalletMoneyDepositedEvent;

    fn name(&self) -> String {
        self.name.to_owned()
    }

    async fn handle(
        &self,
        envelope: EventEnvelope<WalletMoneyDepositedEvent>,
    ) -> Result<(), Error> {
        self.seen.lock().unwrap().push((
            envelope.event_id,
            envelope.event.amount,
            envelope.model_id,
            envelope.action_name,
        ));
        Ok(())
    }
}

/// Fails the first time, then succeeds.
struct FailsOnce {
    calls: Arc<Mutex<u32>>,
}

impl EventHandler for FailsOnce {
    type Event = WalletCreatedEvent;

    fn name(&self) -> String {
        "fails-once".to_owned()
    }

    async fn handle(&self, _: EventEnvelope<WalletCreatedEvent>) -> Result<(), Error> {
        let mut calls = self.calls.lock().unwrap();
        *calls += 1;
        if *calls == 1 {
            return Err(Error::Unexpected("first try fails".to_owned()));
        }
        Ok(())
    }
}

/// Always fails.
struct AlwaysFails;

impl EventHandler for AlwaysFails {
    type Event = WalletClosedEvent;

    fn name(&self) -> String {
        "always-fails".to_owned()
    }

    fn aliases(&self) -> Vec<String> {
        vec!["old-always-fails".to_owned()]
    }

    async fn handle(&self, _: EventEnvelope<WalletClosedEvent>) -> Result<(), Error> {
        Err(Error::Unexpected("never works".to_owned()))
    }
}

async fn notifications(
    registry: &DatabaseRegistry,
    shard: ShardIdentifier,
) -> Vec<EventNotification> {
    let mut db = registry.primary(shard).unwrap();
    EventNotificationRepository::find_all(&mut db)
        .await
        .unwrap()
}

fn by_handler<'a>(rows: &'a [EventNotification], handler: &str) -> Vec<&'a EventNotification> {
    rows.iter().filter(|n| n.handler_name == handler).collect()
}

async fn run(dialect: Dialect) {
    let cluster = start(dialect).await;
    let registry = cluster.registry().await;
    let say = |what: &str| println!("[{dialect:?}] {what}");
    let wallets = Arc::new(WalletRepository::new(registry.clone()));
    let executor = ActionExecutor::builder()
        .namespace("io.example.wallet")
        .database_registry(registry.clone())
        .action_registry(
            ActionRegistry::builder()
                .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                .with_action(WalletDepositMoneyAction::new(wallets.clone()))
                .with_action(WalletCloseAction::new(wallets.clone()))
                .build(),
        )
        .repository_registry(
            RepositoryRegistry::builder()
                .with_repository(wallets.clone())
                .build(),
        )
        .build()
        .unwrap();

    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let audit: Seen = Arc::new(Mutex::new(Vec::new()));
    let fails_once = Arc::new(Mutex::new(0));
    let handlers = || {
        EventHandlerRegistry::builder()
            .with_handler(Recorder {
                name: "deposit-notifier",
                seen: seen.clone(),
            })
            .with_handler(Recorder {
                name: "deposit-audit",
                seen: audit.clone(),
            })
            .with_handler(FailsOnce {
                calls: fails_once.clone(),
            })
            .with_handler(AlwaysFails)
            .build()
            .unwrap()
    };

    // An action on the Mexico shard: created, deposited, closed - three events.
    let wallet: Wallet = executor
        .execute::<WalletCreateAction>(
            None,
            wallet_create_action::Params {
                country_code: Some("MX".to_owned()),
                owner_id: Uuid::new_v4(),
                currency: "MXN".to_owned(),
                initial_balance: Decimal::ONE,
            },
        )
        .await
        .unwrap();
    executor
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
    executor
        .execute::<WalletCloseAction>(
            None,
            wallet_close_action::Params {
                wallet_id: *wallet.id(),
            },
        )
        .await
        .unwrap();

    let clock = Arc::new(VirtualClock::new());
    clock.pause();
    let fanout = EventFanoutJob::new(registry.clone(), handlers()).clock(clock.clone());
    let handling = EventHandlingJob::new(registry.clone(), handlers())
        .retention_window(Duration::from_secs(90))
        .clock(clock.clone());

    // Fan-out: one notification per (event, handler); then nothing left to fan out.
    assert!(fanout.drain_one_round().await.unwrap());
    assert!(!fanout.drain_one_round().await.unwrap());
    let rows = notifications(&registry, MEXICO).await;
    assert_eq!(rows.len(), 4, "{rows:?}");
    assert_eq!(by_handler(&rows, "deposit-notifier").len(), 1);
    assert_eq!(by_handler(&rows, "deposit-audit").len(), 1);
    assert!(
        rows.iter()
            .all(|n| n.state == EventNotificationState::Pending && n.attempts == 0)
    );
    say("fan-out: 3 events -> 4 notifications (two handlers on deposits), events marked delivered");

    // Handling: the recorders get the deposit, typed; the flaky one fails once.
    let now = clock.instant();
    assert!(handling.drain_one_round().await.unwrap());
    let deposit = seen.lock().unwrap().clone();
    assert_eq!(deposit.len(), 1);
    assert_eq!(deposit[0].1, Decimal::new(550, 2));
    assert_eq!(deposit[0].2.as_deref(), Some(&*wallet.id().to_string()));
    assert_eq!(deposit[0].3, "WalletDepositMoneyAction");
    assert_eq!(audit.lock().unwrap().len(), 1);
    let rows = notifications(&registry, MEXICO).await;
    let flaky = by_handler(&rows, "fails-once")[0];
    assert_eq!(
        (flaky.state, flaky.attempts),
        (EventNotificationState::Failed, 1)
    );
    assert_eq!(flaky.next_retry_at, now + TimeDelta::seconds(30));
    let notifier = by_handler(&rows, "deposit-notifier")[0];
    assert_eq!(
        (notifier.state, notifier.attempts),
        (EventNotificationState::Succeeded, 1)
    );
    say("handling: both deposit handlers ran with the typed event; a failure waits 30s");

    // Not due yet: nothing runs. 31s later: the retry succeeds; the hopeless one expires,
    // since its next try (60s on) would fall past the 90s retention window.
    assert!(!handling.drain_one_round().await.unwrap());
    clock.advance(TimeDelta::seconds(31)).unwrap();
    assert!(handling.drain_one_round().await.unwrap());
    let rows = notifications(&registry, MEXICO).await;
    let flaky = by_handler(&rows, "fails-once")[0];
    assert_eq!(
        (flaky.state, flaky.attempts),
        (EventNotificationState::Succeeded, 2)
    );
    let hopeless = by_handler(&rows, "always-fails")[0];
    assert_eq!(
        (hopeless.state, hopeless.attempts),
        (EventNotificationState::Expired, 2)
    );
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "a handled event is not handled again"
    );
    say("retry after backoff succeeds; repeated failure past the retention window expires");

    // Past the retention window before it ever ran: expired without calling the handler.
    executor
        .execute::<WalletCreateAction>(
            None,
            wallet_create_action::Params {
                country_code: Some("MX".to_owned()),
                owner_id: Uuid::new_v4(),
                currency: "MXN".to_owned(),
                initial_balance: Decimal::ONE,
            },
        )
        .await
        .unwrap();
    assert!(fanout.drain_one_round().await.unwrap());
    clock.advance(TimeDelta::hours(2)).unwrap();
    let calls_before = *fails_once.lock().unwrap();
    assert!(handling.drain_one_round().await.unwrap());
    assert_eq!(*fails_once.lock().unwrap(), calls_before);
    let rows = notifications(&registry, MEXICO).await;
    let late = rows
        .iter()
        .find(|n| n.handler_name == "fails-once" && n.state == EventNotificationState::Expired)
        .expect("the late one expired");
    assert_eq!(late.attempts, 0);
    say("already past the retention window: expired without running the handler");

    // Names and aliases are unique.
    let twice = EventHandlerRegistry::builder()
        .with_handler(AlwaysFails)
        .with_handler(Recorder {
            name: "old-always-fails",
            seen: seen.clone(),
        })
        .build();
    assert!(matches!(twice, Err(Error::Config(m)) if m.contains("used twice")));

    // End to end, as an app runs it: both jobs in a JobRegistry, real time. A deposit reaches
    // the handler, and stop() ends the jobs' loops.
    let jobs_pool = ConnectionProvider::from_config(&cluster.config(common::GLOBAL)).unwrap();
    let jobs = JobRegistry::builder()
        .connection_provider(jobs_pool)
        .poll_interval(Duration::from_millis(100))
        .heartbeat_interval(Duration::from_millis(500))
        .with_job(
            EventFanoutJob::new(registry.clone(), handlers())
                .poll_delay(Duration::from_millis(100)),
        )
        .with_job(
            EventHandlingJob::new(registry.clone(), handlers())
                .poll_delay(Duration::from_millis(100)),
        )
        .build()
        .unwrap();
    jobs.start().await.unwrap();
    let other = executor
        .execute::<WalletCreateAction>(
            None,
            wallet_create_action::Params {
                country_code: None,
                owner_id: Uuid::new_v4(),
                currency: "EUR".to_owned(),
                initial_balance: Decimal::ONE,
            },
        )
        .await
        .unwrap();
    executor
        .execute::<WalletDepositMoneyAction>(
            None,
            wallet_deposit_money_action::Params {
                wallet_id: *other.id(),
                amount: Decimal::new(7, 0),
                recipient: None,
            },
        )
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.lock().unwrap().len() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let stopping = std::time::Instant::now();
    jobs.stop().await;
    assert!(
        stopping.elapsed() < Duration::from_secs(3),
        "{:?}",
        stopping.elapsed()
    );
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "{seen:?}");
    assert_eq!(seen[1].1, Decimal::new(7, 0));
    say(
        "end to end in a JobRegistry: a deposit on the global shard reached the handler; stop() ended the loops",
    );
}
