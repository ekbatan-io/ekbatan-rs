//! Keyed locks across "instances" (two pools on one database) on real databases.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use ekbatan::keyed_lock::{AsyncKeyedLockProvider, SqlKeyedLockProvider};
use ekbatan::{ConnectionProvider, Dialect};

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

const LONG: Duration = Duration::from_secs(60);

async fn run(dialect: Dialect) {
    let cluster = start(dialect).await;
    let instance = || {
        let mut config = cluster.config(GLOBAL);
        config.maximum_pool_size = 12;
        SqlKeyedLockProvider::new(ConnectionProvider::from_config(&config).unwrap())
    };
    let (a, b) = (instance(), instance());
    let say = |what: &str| println!("[{dialect:?}] {what}");

    // Held by A: B can't have it, but can have another key.
    let held = a.acquire("wallet:1", LONG).await.unwrap();
    assert!(
        b.try_acquire("wallet:1", Duration::ZERO, LONG)
            .await
            .unwrap()
            .is_none()
    );
    let other = b
        .try_acquire("wallet:2", Duration::ZERO, LONG)
        .await
        .unwrap();
    assert!(other.is_some());
    other.unwrap().release().await;
    let started = Instant::now();
    assert!(
        b.try_acquire("wallet:1", Duration::from_millis(300), LONG)
            .await
            .unwrap()
            .is_none()
    );
    let waited = started.elapsed();
    // MySQL and MariaDB round GET_LOCK's wait up to whole seconds.
    assert!(
        waited >= Duration::from_millis(250) && waited < Duration::from_secs(3),
        "{waited:?}"
    );
    held.release().await;
    let mine = b
        .try_acquire("wallet:1", Duration::ZERO, LONG)
        .await
        .unwrap()
        .unwrap();
    mine.release().await;
    say("held by one instance: the other waits, times out, gets it once released; other keys free");

    // max_hold: never released, freed anyway.
    let forgotten = a
        .acquire("wallet:3", Duration::from_millis(300))
        .await
        .unwrap();
    let started = Instant::now();
    let next = b.acquire("wallet:3", LONG).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "{:?}",
        started.elapsed()
    );
    assert!(!forgotten.is_held().await);
    next.release().await;
    say("never released: freed by max_hold, the waiter gets it");

    // A dropped lease frees the lock.
    drop(a.acquire("wallet:4", LONG).await.unwrap());
    assert!(
        b.try_acquire("wallet:4", Duration::from_secs(2), LONG)
            .await
            .unwrap()
            .is_some()
    );
    say("dropped lease: lock freed");

    // A waiter given up (its future dropped) doesn't leak a lock granted later.
    let held = a.acquire("wallet:5", LONG).await.unwrap();
    let given_up =
        tokio::time::timeout(Duration::from_millis(200), b.acquire("wallet:5", LONG)).await;
    assert!(given_up.is_err());
    held.release().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        instance()
            .try_acquire("wallet:5", Duration::from_secs(2), LONG)
            .await
            .unwrap()
            .is_some()
    );
    say("waiter given up mid-wait: its connection closed, no lock leaked");

    // Twenty tasks, two instances, one key: never two inside at once.
    let (a, b) = (Arc::new(a), Arc::new(b));
    let inside = Arc::new(AtomicU32::new(0));
    let most = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();
    for n in 0..20 {
        let locks = if n % 2 == 0 { a.clone() } else { b.clone() };
        let (inside, most) = (inside.clone(), most.clone());
        tasks.push(tokio::spawn(async move {
            let lease = locks.acquire("wallet:hot", LONG).await.unwrap();
            let now = inside.fetch_add(1, Ordering::SeqCst) + 1;
            most.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            inside.fetch_sub(1, Ordering::SeqCst);
            lease.release().await;
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(most.load(Ordering::SeqCst), 1);
    say("20 tasks on 2 instances, one key: never two inside at once");
}
