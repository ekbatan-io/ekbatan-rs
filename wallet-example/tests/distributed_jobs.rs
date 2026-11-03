//! Distributed jobs on real databases: two instances sharing one `scheduled_tasks`.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ekbatan::distributed_jobs::{AsyncDistributedJob, ExecutionContext, JobRegistry, Schedule};
use ekbatan::{ConnectionProvider, Dialect, Error, Instant};
use sea_query::{Alias, Expr, ExprTrait, Query};

use common::{GLOBAL, start};

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

type Runs = Arc<Mutex<Vec<(&'static str, Instant)>>>;

/// Records each run: which instance, which slot.
struct Tick {
    instance: &'static str,
    runs: Runs,
}

impl AsyncDistributedJob for Tick {
    fn name(&self) -> String {
        "tick".to_owned()
    }

    fn schedule(&self) -> Schedule {
        Schedule::fixed_delay(Duration::from_secs(1))
    }

    async fn execute(&self, ctx: &ExecutionContext) -> Result<(), Error> {
        self.runs
            .lock()
            .unwrap()
            .push((self.instance, ctx.execution_time()));
        Ok(())
    }
}

/// A job with a name, a schedule and a body given by the test.
struct Named<F> {
    name: &'static str,
    schedule: Schedule,
    body: F,
}

impl<F, Fut> AsyncDistributedJob for Named<F>
where
    F: Fn(ExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), Error>> + Send,
{
    fn name(&self) -> String {
        self.name.to_owned()
    }

    fn schedule(&self) -> Schedule {
        self.schedule.clone()
    }

    async fn execute(&self, ctx: &ExecutionContext) -> Result<(), Error> {
        (self.body)(ctx.clone()).await
    }
}

fn fast(provider: ConnectionProvider) -> ekbatan::distributed_jobs::JobRegistryBuilder {
    JobRegistry::builder()
        .connection_provider(provider)
        .poll_interval(Duration::from_millis(100))
        .heartbeat_interval(Duration::from_millis(200))
        .missed_heartbeats_limit(4)
        .shutdown_max_wait(Duration::from_secs(5))
}

#[derive(Debug, sqlx::FromRow)]
struct TaskRow {
    picked: bool,
    picked_by: Option<String>,
    consecutive_failures: Option<i32>,
    last_success: Option<Instant>,
    last_failure: Option<Instant>,
}

async fn task_row(provider: &ConnectionProvider, name: &str) -> Option<TaskRow> {
    let select = Query::select()
        .columns(
            [
                "picked",
                "picked_by",
                "consecutive_failures",
                "last_success",
                "last_failure",
            ]
            .map(Alias::new),
        )
        .from(Alias::new("scheduled_tasks"))
        .and_where(Expr::col(Alias::new("task_name")).eq(name))
        .to_owned();
    provider.db().fetch_optional(&select).await.unwrap()
}

async fn run(dialect: Dialect) {
    let cluster = start(dialect).await;
    cluster.migrate().await;
    let pool = || ConnectionProvider::from_config(&cluster.config(GLOBAL)).unwrap();
    let say = |what: &str| println!("[{dialect:?}] {what}");

    // Two instances, one job every second, for eight seconds: each slot runs once.
    let runs: Runs = Arc::new(Mutex::new(Vec::new()));
    let a = fast(pool())
        .with_job(Tick {
            instance: "a",
            runs: runs.clone(),
        })
        .build()
        .unwrap();
    let b = fast(pool())
        .with_job(Tick {
            instance: "b",
            runs: runs.clone(),
        })
        .build()
        .unwrap();
    a.start().await.unwrap();
    b.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(8_200)).await;
    a.stop().await;
    b.stop().await;
    let runs = runs.lock().unwrap().clone();
    let mut slots: Vec<Instant> = runs.iter().map(|(_, slot)| *slot).collect();
    slots.sort();
    slots.dedup();
    assert_eq!(slots.len(), runs.len(), "a slot ran twice: {runs:?}");
    assert!(
        (7..=9).contains(&runs.len()),
        "{} runs: {runs:?}",
        runs.len()
    );
    let by_a = runs.iter().filter(|(i, _)| *i == "a").count();
    say(&format!(
        "two instances, 8s of a 1s job: {} runs, no slot twice (a: {by_a}, b: {})",
        runs.len(),
        runs.len() - by_a
    ));

    // A failing job: counted, and rescheduled by its schedule.
    let failing = fast(pool())
        .with_job(Named {
            name: "fails",
            schedule: Schedule::fixed_delay(Duration::from_millis(200)),
            body: |_| async { Err(Error::Unexpected("boom".to_owned())) },
        })
        .build()
        .unwrap();
    failing.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    failing.stop().await;
    let row = task_row(&pool(), "fails").await.unwrap();
    assert!(row.consecutive_failures.unwrap() >= 2, "{row:?}");
    assert!(
        row.last_failure.is_some() && row.last_success.is_none() && !row.picked,
        "{row:?}"
    );
    say("failing job: consecutive_failures counted, rescheduled, row released");

    // A dead execution (picked, heartbeat long gone): revived and run by a live instance.
    cluster
        .run_sql(
            GLOBAL,
            "INSERT INTO scheduled_tasks (task_name, task_instance, execution_time, picked, \
             picked_by, last_heartbeat, consecutive_failures, version) VALUES ('revived', \
             'recurring', '2020-01-01 00:00:00', true, 'ghost', '2020-01-01 00:00:00', 0, 5)",
        )
        .await;
    let revived_runs = Arc::new(AtomicU32::new(0));
    let counter = revived_runs.clone();
    let reviver = fast(pool())
        .with_job(Named {
            name: "revived",
            schedule: Schedule::fixed_delay(Duration::from_secs(60)),
            body: move |_| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        })
        .build()
        .unwrap();
    reviver.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    reviver.stop().await;
    assert_eq!(revived_runs.load(Ordering::SeqCst), 1);
    let row = task_row(&pool(), "revived").await.unwrap();
    assert!(
        !row.picked && row.picked_by.is_none() && row.last_success.is_some(),
        "{row:?}"
    );
    say("dead execution (ghost instance, no heartbeat): revived and run once");

    // Taken over mid-run: the stale instance's result is not recorded, and its heartbeat
    // standing shows it.
    let fraction = Arc::new(Mutex::new(0.0f64));
    let seen = fraction.clone();
    let slow = fast(pool())
        .with_job(Named {
            name: "slow",
            schedule: Schedule::fixed_delay(Duration::from_secs(60)),
            body: move |ctx: ExecutionContext| {
                let seen = seen.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(1_500)).await;
                    *seen.lock().unwrap() = ctx.heartbeat_state().fraction_dead();
                    Ok(())
                }
            },
        })
        .build()
        .unwrap();
    slow.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    // Another instance takes the row (its version moves on) and stays alive (a fresh heartbeat,
    // so the row isn't dead and revived again by us).
    cluster
        .run_sql(
            GLOBAL,
            "UPDATE scheduled_tasks SET version = version + 100, picked_by = 'other', \
             last_heartbeat = '2037-01-01 00:00:00' WHERE task_name = 'slow'",
        )
        .await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    slow.stop().await;
    let row = task_row(&pool(), "slow").await.unwrap();
    assert!(
        row.picked && row.picked_by.as_deref() == Some("other") && row.last_success.is_none(),
        "{row:?}"
    );
    assert!(
        *fraction.lock().unwrap() > 0.5,
        "fraction_dead {}",
        fraction.lock().unwrap()
    );
    say("taken over mid-run: result not recorded; fraction_dead rose past 0.5");

    // stop() waits for a running execution to finish and release its row.
    let finished = Arc::new(AtomicU32::new(0));
    let done = finished.clone();
    let waiter = fast(pool())
        .with_job(Named {
            name: "waited_for",
            schedule: Schedule::fixed_delay(Duration::from_secs(60)),
            body: move |_| {
                let done = done.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    done.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        })
        .build()
        .unwrap();
    waiter.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    waiter.stop().await;
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    assert!(!task_row(&pool(), "waited_for").await.unwrap().picked);
    say("stop(): waited for the running execution, row released");

    // A disabled schedule writes no row; a missing table fails start().
    let disabled = fast(pool())
        .with_job(Named {
            name: "off",
            schedule: Schedule::disabled(),
            body: |_| async { Ok(()) },
        })
        .build()
        .unwrap();
    disabled.start().await.unwrap();
    disabled.stop().await;
    assert!(task_row(&pool(), "off").await.is_none());
    let s = cluster.shard(GLOBAL);
    s.server
        .run_sql(&s.database, "CREATE DATABASE no_tables")
        .await;
    let empty = ConnectionProvider::from_config(&s.server.config("no_tables")).unwrap();
    let refused = fast(empty)
        .with_job(Tick {
            instance: "x",
            runs: Arc::new(Mutex::new(Vec::new())),
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap_err()
        .to_string();
    assert!(
        refused.contains("scheduled_tasks") && refused.contains("tick"),
        "{refused}"
    );
    say("disabled job: no row; missing table: start() fails, naming the job");

    // Names: blank or twice is refused when built.
    let twice = fast(pool())
        .with_job(Tick {
            instance: "x",
            runs: Arc::new(Mutex::new(Vec::new())),
        })
        .with_job(Tick {
            instance: "y",
            runs: Arc::new(Mutex::new(Vec::new())),
        })
        .build();
    assert!(matches!(twice, Err(Error::Config(m)) if m.contains("unique")));
    let blank = fast(pool())
        .with_job(Named {
            name: " ",
            schedule: Schedule::disabled(),
            body: |_| async { Ok(()) },
        })
        .build();
    assert!(matches!(blank, Err(Error::Config(m)) if m.contains("blank")));
}
