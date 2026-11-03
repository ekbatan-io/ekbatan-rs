//! How pooled connections behave with sqlx: reused, lent per statement, held by a transaction
//! or a lock, and what goes wrong when work that needs its own connection runs on the pool.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use sqlx::Connection;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::task::JoinSet;

const PID: &str = "SELECT pg_backend_pid()";

#[tokio::test(flavor = "multi_thread")]
async fn postgres_connections() {
    let server = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("PostgreSQL starts");
    let port = server.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    // 1. A statement borrows a connection only while it runs, so a few serve many.
    let pool = PgPoolOptions::new()
        .max_connections(3)
        .connect(&url)
        .await
        .unwrap();
    let mut statements = JoinSet::new();
    for _ in 0..30 {
        let pool = pool.clone();
        statements.spawn(async move {
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid(), pg_sleep(0.05)")
                .fetch_one(&pool)
                .await
                .unwrap()
        });
    }
    let used: BTreeSet<i32> = statements.join_all().await.into_iter().collect();
    println!(
        "1. 30 statements at once on a pool of 3: all ran, on {} connections",
        used.len()
    );
    assert!(used.len() <= 3);

    // 2. A transaction holds one connection until it ends; the rest keep serving statements.
    let mut first = pool.begin().await.unwrap();
    let mut second = pool.begin().await.unwrap();
    let first_pid: i32 = sqlx::query_scalar(PID)
        .fetch_one(&mut *first)
        .await
        .unwrap();
    let second_pid: i32 = sqlx::query_scalar(PID)
        .fetch_one(&mut *second)
        .await
        .unwrap();
    let mut others = BTreeSet::new();
    for _ in 0..10 {
        others.insert(
            sqlx::query_scalar::<_, i32>(PID)
                .fetch_one(&pool)
                .await
                .unwrap(),
        );
    }
    println!(
        "2. two open transactions hold connections {first_pid} and {second_pid}; \
         10 statements meanwhile ran on {others:?}"
    );
    assert!(!others.contains(&first_pid) && !others.contains(&second_pid));
    first.commit().await.unwrap();
    second.rollback().await.unwrap();

    // 3. When every connection is held, the next statement waits, then gives up.
    let small = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(1))
        .connect(&url)
        .await
        .unwrap();
    let held = (small.begin().await.unwrap(), small.begin().await.unwrap());
    let started = Instant::now();
    let starved = sqlx::query("SELECT 1").execute(&small).await;
    println!(
        "3. pool of 2, both held by transactions: the next statement waited {:.1}s, then: {}",
        started.elapsed().as_secs_f32(),
        starved.as_ref().unwrap_err()
    );
    assert!(matches!(starved, Err(sqlx::Error::PoolTimedOut)));
    drop(held);

    // 4. A session lock belongs to its connection: unlocking from another one does nothing.
    let mut holder = pool.acquire().await.unwrap();
    let mut other = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(42)")
        .execute(&mut *holder)
        .await
        .unwrap();
    let unlock_elsewhere: bool = sqlx::query_scalar("SELECT pg_advisory_unlock(42)")
        .fetch_one(&mut *other)
        .await
        .unwrap();
    let lockable_elsewhere: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(42)")
        .fetch_one(&mut *other)
        .await
        .unwrap();
    let unlock_on_holder: bool = sqlx::query_scalar("SELECT pg_advisory_unlock(42)")
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    println!(
        "4. lock taken on connection A: unlock from B -> {unlock_elsewhere}, B can take it -> \
         {lockable_elsewhere}; unlock from A -> {unlock_on_holder}"
    );
    assert!(!unlock_elsewhere && !lockable_elsewhere && unlock_on_holder);
    drop((holder, other));

    // 5. Taken through the pool, the lock goes back into the pool with its connection.
    sqlx::query("SELECT pg_advisory_lock(7)")
        .execute(&pool)
        .await
        .unwrap();
    let mut outsider = PgConnection::connect(&url).await.unwrap();
    let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(7)")
        .fetch_one(&mut outsider)
        .await
        .unwrap();
    println!(
        "5. lock taken through the pool: its connection went back idle still holding it; \
         another session can take it -> {free}"
    );
    assert!(!free);

    // 6. A session setting also goes back into the pool with its connection, to the next user.
    let single = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query("SET lock_timeout = '1234ms'")
        .execute(&single)
        .await
        .unwrap();
    let inherited: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&single)
        .await
        .unwrap();
    println!(
        "6. SET lock_timeout through the pool; the next statement on the pool sees {inherited}"
    );
    assert_eq!(inherited, "1234ms");

    // 7. Evicting: close a connection instead of returning it (Java's ConnectionProvider.evict).
    let mut doomed = single.acquire().await.unwrap();
    let doomed_pid: i32 = sqlx::query_scalar(PID)
        .fetch_one(&mut *doomed)
        .await
        .unwrap();
    doomed.close_on_drop();
    drop(doomed);
    let next_pid: i32 = sqlx::query_scalar(PID).fetch_one(&single).await.unwrap();
    let fresh: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&single)
        .await
        .unwrap();
    println!(
        "7. close_on_drop: connection {doomed_pid} was closed, not reused; the next one is \
         {next_pid}, with lock_timeout {fresh}"
    );
    assert!(doomed_pid != next_pid && fresh == "0");
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_session_lock() {
    let server = Mysql::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("MySQL starts");
    let port = server.get_host_port_ipv4(3306).await.unwrap();
    let pool = MySqlPoolOptions::new()
        .max_connections(3)
        .connect(&format!("mysql://root@127.0.0.1:{port}/test"))
        .await
        .unwrap();
    let mut holder = pool.acquire().await.unwrap();
    let mut other = pool.acquire().await.unwrap();
    let taken: Option<i64> = sqlx::query_scalar("SELECT GET_LOCK('wallet-42', 0)")
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    let released_elsewhere: Option<i64> = sqlx::query_scalar("SELECT RELEASE_LOCK('wallet-42')")
        .fetch_one(&mut *other)
        .await
        .unwrap();
    let released_on_holder: Option<i64> = sqlx::query_scalar("SELECT RELEASE_LOCK('wallet-42')")
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    println!(
        "MySQL: GET_LOCK on connection A -> {taken:?}; RELEASE_LOCK from B -> \
         {released_elsewhere:?} (not its lock); from A -> {released_on_holder:?}"
    );
    assert_eq!(
        (taken, released_elsewhere, released_on_holder),
        (Some(1), Some(0), Some(1))
    );
}
