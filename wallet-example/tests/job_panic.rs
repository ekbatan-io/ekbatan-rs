//! A job that panics is a failed run, as a job that returns an error is: the panic is caught,
//! the run is recorded as failed, the job's next slot is scheduled, and it runs again. (Java's
//! scheduler catches every `Throwable` from a job.) Without that, the panicking run stayed
//! "running" on its instance for good, its heartbeat kept the claim alive, and the job never
//! ran again until the process restarted.
//!
//! A handler that panics is a failed delivery, as one that returns an error is: retried with
//! backoff, then expired. Uncaught, it took the whole handling job down with it.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use ekbatan::action::ActionRegistry;
use ekbatan::aio::{AsyncActionExecutor, AsyncDistributedJob, AsyncEventHandler};
use ekbatan::distributed_jobs::{ExecutionContext, JobRegistry, Schedule};
use ekbatan::local_event_handler::{
    EventEnvelope, EventFanoutJob, EventHandlerRegistry, EventHandlingJob,
    EventNotificationRepository, EventNotificationState,
};
use ekbatan::repository::RepositoryRegistry;
use ekbatan::{ConnectionProvider, Dialect, Error, SystemClock};
use rust_decimal::Decimal;
use uuid::Uuid;
use wallet_example::action::{WalletCreateAction, wallet_create_action};
use wallet_example::model::WalletCreatedEvent;
use wallet_example::repository::WalletRepository;

use common::{GLOBAL, start};

/// Panics on its first run, then counts its runs.
struct PanicsOnce {
    runs: Arc<AtomicU32>,
}

impl AsyncDistributedJob for PanicsOnce {
    fn name(&self) -> String {
        "panics-once".to_owned()
    }

    fn schedule(&self) -> Schedule {
        Schedule::fixed_delay(Duration::from_millis(200))
    }

    async fn execute(&self, _ctx: &ExecutionContext) -> Result<(), Error> {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("the first run panics");
        }
        Ok(())
    }
}

/// The same, as a blocking job: the panic happens inside `block_in_place`.
struct BlockingPanicsOnce {
    runs: Arc<AtomicU32>,
}

impl ekbatan::blocking::DistributedJob for BlockingPanicsOnce {
    fn name(&self) -> String {
        "blocking-panics-once".to_owned()
    }

    fn schedule(&self) -> Schedule {
        Schedule::fixed_delay(Duration::from_millis(200))
    }

    fn execute(&self, _ctx: &ExecutionContext) -> Result<(), Error> {
        if self.runs.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("the first blocking run panics");
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_job_runs_again() {
    let cluster = start(Dialect::Postgres).await;
    cluster.migrate().await;
    let (runs, blocking_runs) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
    let jobs = JobRegistry::builder()
        .connection_provider(ConnectionProvider::from_config(&cluster.config(GLOBAL)).unwrap())
        .poll_interval(Duration::from_millis(100))
        .heartbeat_interval(Duration::from_millis(200))
        .with_job(PanicsOnce { runs: runs.clone() })
        .with_job(BlockingPanicsOnce {
            runs: blocking_runs.clone(),
        })
        .build()
        .unwrap();
    jobs.start().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while (runs.load(Ordering::SeqCst) < 3 || blocking_runs.load(Ordering::SeqCst) < 3)
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    jobs.stop().await;
    assert!(
        runs.load(Ordering::SeqCst) >= 3,
        "async job ran {runs:?} time(s)"
    );
    assert!(
        blocking_runs.load(Ordering::SeqCst) >= 3,
        "blocking job ran {blocking_runs:?} time(s)"
    );
    println!(
        "after a panic on the first run: the async job ran {} times, the blocking one {}",
        runs.load(Ordering::SeqCst),
        blocking_runs.load(Ordering::SeqCst)
    );
}

/// Panics on every delivery.
struct PanickingHandler;

impl AsyncEventHandler for PanickingHandler {
    type Event = WalletCreatedEvent;

    fn name(&self) -> String {
        "panicking-handler".to_owned()
    }

    async fn handle(&self, _: EventEnvelope<WalletCreatedEvent>) -> Result<(), Error> {
        panic!("the handler panics");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_handler_is_a_failed_delivery() {
    let cluster = start(Dialect::Postgres).await;
    let registry = cluster.registry().await;
    let wallets = std::sync::Arc::new(WalletRepository::new(registry.clone()));
    let executor = AsyncActionExecutor::builder()
        .namespace("io.example.wallet")
        .database_registry(registry.clone())
        .action_registry(
            ActionRegistry::builder()
                .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                .build(),
        )
        .repository_registry(
            RepositoryRegistry::builder()
                .with_repository(wallets)
                .build(),
        )
        .build()
        .unwrap();
    executor
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
    let handlers = || {
        EventHandlerRegistry::builder()
            .with_handler(PanickingHandler)
            .build()
            .unwrap()
    };
    while EventFanoutJob::new(registry.clone(), handlers())
        .drain_one_round()
        .await
        .unwrap()
    {}
    // The round finishes despite the panic, and records the delivery as failed.
    assert!(
        EventHandlingJob::new(registry.clone(), handlers())
            .drain_one_round()
            .await
            .unwrap()
    );
    let mut db = registry.as_async().primary(GLOBAL).unwrap();
    let rows = EventNotificationRepository::find_all(&mut db)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].state, rows[0].attempts),
        (EventNotificationState::Failed, 1)
    );
    assert!(
        rows[0].next_retry_at > rows[0].event_date,
        "a backoff was set"
    );
    println!("a panicking handler: its delivery is Failed, attempts 1, retried later");
}
