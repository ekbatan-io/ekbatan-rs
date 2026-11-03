//! The blocking API on real databases, from plain (not async) code: actions in the executor,
//! the repository, transactions, keyed locks, and a job and an event handler in a real
//! `JobRegistry`.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use ekbatan::action::{
    ActionPlan, ActionRegistry, EventEntityRepository, ExecutionConfiguration, RepositoryRegistry,
    RetryConfig, RetryOn,
};
use ekbatan::blocking::{
    self, Action, ActionExecutor, DistributedJob, EventHandler, KeyedLockProvider, Repository,
    block_on,
};
use ekbatan::distributed_jobs::{ExecutionContext, JobRegistry, Schedule};
use ekbatan::keyed_lock::SqlKeyedLockProvider;
use ekbatan::local_event_handler::{
    EventEnvelope, EventFanoutJob, EventHandlerRegistry, EventHandlingJob,
};
use ekbatan::{
    Clock, ConnectionProvider, DatabaseRegistry, Dialect, Error, Model, Persistable, ShardedId,
    SystemClock,
};
use rust_decimal::Decimal;
use sea_query::{Asterisk, Expr, ExprTrait, Func, Query};
use serde::Serialize;
use uuid::Uuid;
use wallet_example::action::{WalletCreateAction, wallet_create_action};
use wallet_example::model::{Wallet, WalletMoneyDepositedEvent};
use wallet_example::repository::WalletRepository;
use wallet_example::schema::Wallets;

use common::{GLOBAL, MEXICO, start};

#[test]
fn on_postgres() {
    run(Dialect::Postgres);
}

#[test]
fn on_mysql() {
    run(Dialect::MySql);
}

#[test]
fn on_mariadb() {
    run(Dialect::MariaDb);
}

/// A deposit whose `perform` is a plain `fn`.
struct SyncDeposit {
    wallets: Arc<WalletRepository>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DepositParams {
    wallet_id: ShardedId<Wallet>,
    amount: Decimal,
}

impl Action for SyncDeposit {
    type Params = DepositParams;
    type Result = Wallet;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &DepositParams,
    ) -> Result<Wallet, Error> {
        let wallet = self.wallets.get_by_id(&params.wallet_id)?;
        plan.update(wallet.deposit(params.amount)?)
    }
}

/// Goes stale on its first try (someone else saves the wallet first), then succeeds.
struct SyncStaleOnce {
    wallets: Arc<WalletRepository>,
    calls: Arc<AtomicU32>,
}

impl Action for SyncStaleOnce {
    type Params = DepositParams;
    type Result = Wallet;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &DepositParams,
    ) -> Result<Wallet, Error> {
        let wallet = self.wallets.get_by_id(&params.wallet_id)?;
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.wallets
                .update_no_result(None, &wallet.copy().build()?)?;
        }
        plan.update(wallet.deposit(params.amount)?)
    }
}

/// Counts its runs, and reads the database the sync way while it runs.
struct SyncCounter {
    runs: Arc<AtomicU32>,
    wallets: Arc<WalletRepository>,
}

impl DistributedJob for SyncCounter {
    fn name(&self) -> String {
        "sync-counter".to_owned()
    }

    fn schedule(&self) -> Schedule {
        Schedule::FixedDelay(Duration::from_millis(200))
    }

    fn execute(&self, _ctx: &ExecutionContext) -> Result<(), Error> {
        self.wallets.count()?;
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Records each deposit, reading the wallet the sync way.
struct SyncRecorder {
    wallets: Arc<WalletRepository>,
    seen: Arc<Mutex<Vec<(Decimal, Decimal)>>>,
}

impl EventHandler for SyncRecorder {
    type Event = WalletMoneyDepositedEvent;

    fn name(&self) -> String {
        "sync-recorder".to_owned()
    }

    fn handle(&self, envelope: EventEnvelope<WalletMoneyDepositedEvent>) -> Result<(), Error> {
        let id = ShardedId::<Wallet>::from_uuid(
            envelope
                .model_id
                .as_deref()
                .and_then(|id| id.parse().ok())
                .expect("a wallet id"),
        );
        let wallet = self.wallets.get_by_id(&id)?;
        self.seen
            .lock()
            .unwrap()
            .push((envelope.event.amount, *wallet.balance()));
        Ok(())
    }
}

fn open(executor: &ActionExecutor, country: Option<&str>, balance: i64) -> Wallet {
    // An async action, run by the sync executor all the same.
    executor
        .execute::<WalletCreateAction>(
            None,
            wallet_create_action::Params {
                country_code: country.map(str::to_owned),
                owner_id: Uuid::new_v4(),
                currency: "EUR".to_owned(),
                initial_balance: Decimal::new(balance, 0),
            },
        )
        .unwrap()
}

fn run(dialect: Dialect) {
    assert!(tokio::runtime::Handle::try_current().is_err(), "not async");
    let cluster = block_on(start(dialect));
    block_on(cluster.migrate());
    let say = |what: &str| println!("[{dialect:?}] {what}");

    // Pools made from plain code, outside any runtime: they live on the sync API's runtime.
    let registry = Arc::new(
        DatabaseRegistry::new(
            GLOBAL,
            cluster
                .shards
                .iter()
                .map(|s| {
                    let config = s.server.config(&s.database);
                    ekbatan::TransactionManager::new(
                        s.shard,
                        ConnectionProvider::from_config(&config).unwrap(),
                        None,
                    )
                })
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    );
    let wallets = Arc::new(WalletRepository::new(registry.clone()));
    let stale_calls = Arc::new(AtomicU32::new(0));
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let executor = ActionExecutor::builder()
        .namespace("io.example.wallet")
        .database_registry(registry.clone())
        .action_registry(
            ActionRegistry::builder()
                .with_action(WalletCreateAction::new(clock.clone()))
                .with_action(SyncDeposit {
                    wallets: wallets.clone(),
                })
                .with_action(SyncStaleOnce {
                    wallets: wallets.clone(),
                    calls: stale_calls.clone(),
                })
                .build(),
        )
        .repository_registry(
            RepositoryRegistry::builder()
                .with_repository(wallets.clone())
                .build(),
        )
        .clock(clock)
        .build()
        .unwrap();

    // Actions: a sync action, on each shard; its row and its event row are written.
    for (country, shard) in [(None, GLOBAL), (Some("MX"), MEXICO)] {
        let wallet = open(&executor, country, 10);
        let deposited = executor
            .execute::<SyncDeposit>(
                Some("alice"),
                DepositParams {
                    wallet_id: *wallet.id(),
                    amount: Decimal::new(550, 2),
                },
            )
            .unwrap();
        assert_eq!(*deposited.balance(), Decimal::new(1550, 2));
        let stored = wallets.get_by_id(wallet.id()).unwrap();
        assert_eq!(*stored.balance(), Decimal::new(1550, 2));
        assert_eq!(stored.version(), 2);
        let mut db = registry.as_async().primary(shard).unwrap();
        let events = block_on(EventEntityRepository::find_all(&mut db)).unwrap();
        let deposit = events
            .iter()
            .find(|e| e.action_name == "SyncDeposit")
            .expect("the deposit's event row");
        assert_eq!(
            deposit.event_type.as_deref(),
            Some("WalletMoneyDepositedEvent")
        );
    }
    say("a sync action ran in the executor on both shards; its row and event row committed");

    // Retry: stale on the first try, retried from a fresh plan, then committed.
    let wallet = open(&executor, None, 1);
    let retried = executor
        .execute_with::<SyncStaleOnce>(
            None,
            DepositParams {
                wallet_id: *wallet.id(),
                amount: Decimal::ONE,
            },
            &ExecutionConfiguration::builder()
                .with_retry(
                    RetryOn::stale_record(),
                    RetryConfig::new(1, Duration::from_millis(1)),
                )
                .build(),
        )
        .unwrap();
    assert_eq!(stale_calls.load(Ordering::SeqCst), 2);
    assert_eq!(*retried.balance(), Decimal::new(2, 0));
    assert_eq!(retried.version(), 3);
    stale_calls.store(0, Ordering::SeqCst);
    let failed = executor.execute_with::<SyncStaleOnce>(
        None,
        DepositParams {
            wallet_id: *wallet.id(),
            amount: Decimal::ONE,
        },
        &ExecutionConfiguration::builder().no_retry().build(),
    );
    assert!(
        matches!(failed, Err(Error::StaleRecord { .. })),
        "{failed:?}"
    );
    say("a stale sync action retries from a fresh plan; without retry it fails stale");

    // Repository: ready-made methods, and a custom query through the sync Db.
    let owner_wallets = wallets
        .find_all_where(Expr::col(Wallets::OwnerId).eq(*wallet.owner_id()).into())
        .unwrap();
    assert_eq!(owner_wallets.len(), 1);
    assert!(wallets.exists_by_id(wallet.id()).unwrap());
    assert_eq!(wallets.count().unwrap(), 3);
    let opened = wallets
        .readonly_dbs()
        .fetch_scalars::<i64>(
            &Query::select()
                .expr(Func::count(Expr::col(Asterisk)))
                .from(Wallets::Table)
                .to_owned(),
        )
        .unwrap();
    assert_eq!(opened.iter().sum::<i64>(), 3);
    say("repository: ready-made methods and a custom query on every shard, without await");

    // Transactions: a rollback leaves nothing; a commit keeps it.
    let manager = registry.transaction_manager(GLOBAL).unwrap();
    let fresh = || {
        Wallet::create_wallet(
            GLOBAL,
            Uuid::new_v4(),
            "EUR",
            Decimal::ONE,
            SystemClock.instant(),
        )
        .build()
        .unwrap()
    };
    let rolled_back = fresh();
    let result: Result<(), Error> = manager.in_transaction(|tx| {
        wallets.add(Some(tx), &rolled_back)?;
        Err(Error::Unexpected("changed my mind".to_owned()))
    });
    assert!(result.is_err());
    assert!(wallets.find_by_id(rolled_back.id()).unwrap().is_none());
    let kept = fresh();
    manager
        .in_transaction(|tx| {
            let added = wallets.add(Some(tx), &kept)?;
            wallets.update(Some(tx), &added.deposit(Decimal::ONE)?)
        })
        .unwrap();
    assert_eq!(
        *wallets.get_by_id(kept.id()).unwrap().balance(),
        Decimal::new(2, 0)
    );
    let mut tx = manager.begin().unwrap();
    wallets.add(Some(&mut tx), &fresh()).unwrap();
    drop(tx); // never ended: the connection is closed and the database rolls it back
    assert_eq!(wallets.count().unwrap(), 4);
    say("transactions: rollback keeps nothing, commit keeps both writes, a dropped one rolls back");

    // Keyed locks: two instances, four threads each; no two ever inside together.
    let instance = || {
        let mut config = cluster.config(GLOBAL);
        config.maximum_pool_size = 6;
        Arc::new(SqlKeyedLockProvider::new(
            ConnectionProvider::from_config(&config).unwrap(),
        ))
    };
    let (a, b) = (instance(), instance());
    let inside = Arc::new(AtomicBool::new(false));
    let entries = Arc::new(AtomicU32::new(0));
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let provider = if i % 2 == 0 { a.clone() } else { b.clone() };
            let (inside, entries) = (inside.clone(), entries.clone());
            thread::spawn(move || {
                for _ in 0..3 {
                    let lease = provider
                        .acquire("wallet:sync", Duration::from_secs(60))
                        .unwrap();
                    assert!(!inside.swap(true, Ordering::SeqCst), "two inside at once");
                    thread::sleep(Duration::from_millis(5));
                    inside.store(false, Ordering::SeqCst);
                    entries.fetch_add(1, Ordering::SeqCst);
                    drop(lease); // dropping releases it, before drop returns
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(entries.load(Ordering::SeqCst), 24);
    let held = a.acquire("wallet:held", Duration::from_secs(60)).unwrap();
    assert!(held.is_held());
    assert!(
        b.try_acquire("wallet:held", Duration::ZERO, Duration::from_secs(60))
            .unwrap()
            .is_none()
    );
    drop(held);
    let mine = b
        .try_acquire("wallet:held", Duration::ZERO, Duration::from_secs(60))
        .unwrap();
    assert!(mine.is_some(), "a dropped lease is free at once");
    mine.unwrap().release();
    say("keyed locks: 24 entries from 8 threads on 2 instances never overlapped; drop releases");

    // A sync job and a sync event handler, in a real JobRegistry.
    let runs = Arc::new(AtomicU32::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handlers = || {
        EventHandlerRegistry::builder()
            .with_handler(SyncRecorder {
                wallets: wallets.clone(),
                seen: seen.clone(),
            })
            .build()
            .unwrap()
    };
    let jobs = JobRegistry::builder()
        .connection_provider(ConnectionProvider::from_config(&cluster.config(GLOBAL)).unwrap())
        .poll_interval(Duration::from_millis(100))
        .heartbeat_interval(Duration::from_millis(500))
        .with_job(SyncCounter {
            runs: runs.clone(),
            wallets: wallets.clone(),
        })
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
    block_on(jobs.start()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while (seen.lock().unwrap().len() < 3 || runs.load(Ordering::SeqCst) < 3)
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(50));
    }
    let stopping = Instant::now();
    block_on(jobs.stop());
    assert!(stopping.elapsed() < Duration::from_secs(3));
    assert!(runs.load(Ordering::SeqCst) >= 3, "{runs:?}");
    let mut seen = seen.lock().unwrap().clone();
    seen.sort();
    // The three deposits: 5.50 on two shards (balance read back 15.50), and the retried 1.
    assert_eq!(
        seen,
        vec![
            (Decimal::ONE, Decimal::new(2, 0)),
            (Decimal::new(550, 2), Decimal::new(1550, 2)),
            (Decimal::new(550, 2), Decimal::new(1550, 2)),
        ]
    );
    say("a sync job ran on its schedule and a sync handler got every deposit; stop() ended them");

    registry.close();
    let _inside = blocking::runtime().enter();
    drop(cluster);
}
