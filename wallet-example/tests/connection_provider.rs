//! The connection structs on PostgreSQL, logged in with a real password: `DataSourceConfig`,
//! `ConnectionProvider`, `Connection`, `Transaction`, `TransactionManager`.

use std::collections::BTreeSet;
use std::time::Duration;

use ekbatan::{ConnectionProvider, DataSourceConfig, Dialect, ShardIdentifier, TransactionManager};
use sea_query::{Asterisk, Expr, Func, Query, SelectStatement};
use sqlx::Connection as _;
use sqlx::postgres::PgConnection;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const PASSWORD: &str = "Wallet-Secret-77";

fn pid() -> SelectStatement {
    Query::select()
        .expr(Func::cust("pg_backend_pid"))
        .to_owned()
}

fn advisory(function: &'static str) -> SelectStatement {
    Query::select()
        .expr(Func::cust(function).arg(42i64))
        .to_owned()
}

/// The pids of every connection the pool hands out, taking them all at once.
async fn pids_in_pool(provider: &ConnectionProvider, pool_size: usize) -> BTreeSet<i32> {
    let mut held = Vec::new();
    let mut pids = BTreeSet::new();
    for _ in 0..pool_size {
        let mut connection = provider.acquire().await.unwrap();
        pids.insert(connection.db().fetch_scalar::<i32>(&pid()).await.unwrap());
        held.push(connection);
    }
    pids
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_structs_on_postgres() {
    let container = Postgres::default()
        .with_password(PASSWORD)
        .with_tag("17-alpine")
        .start()
        .await
        .expect("PostgreSQL starts");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let mut setup = PgConnection::connect(&format!(
        "postgres://postgres:{PASSWORD}@127.0.0.1:{port}/postgres"
    ))
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TABLE notes (id INT PRIMARY KEY)")
        .execute(&mut setup)
        .await
        .unwrap();

    // 1. The config logs in with the password, and nothing prints it.
    let config = DataSourceConfig {
        username: Some("postgres".to_owned()),
        password: Some(PASSWORD.to_owned()),
        connection_timeout: Some(Duration::from_secs(2)),
        ..DataSourceConfig::new(
            Dialect::Postgres,
            format!("postgres://127.0.0.1:{port}/postgres"),
            3,
        )
    };
    let provider = ConnectionProvider::from_config(&config).unwrap();
    let shown = format!("{config:?} {provider:?}");
    assert!(!shown.contains(PASSWORD), "{shown}");
    println!("1. the password is in neither: {shown}");

    // 2. Nothing is opened until the first use; a statement borrows a connection and returns it.
    assert_eq!(provider.size(), 0);
    let first_pid: i32 = provider.db().fetch_scalar(&pid()).await.unwrap();
    println!(
        "2. before the first statement the pool had 0 connections; the statement ran on \
         {first_pid}, and now: {provider:?}"
    );

    // 3. A held Connection keeps a session lock; another connection can't take it.
    let mut held = provider.acquire().await.unwrap();
    let mut other = provider.acquire().await.unwrap();
    let taken: bool = held
        .db()
        .fetch_scalar(&advisory("pg_try_advisory_lock"))
        .await
        .unwrap();
    let taken_elsewhere: bool = other
        .db()
        .fetch_scalar(&advisory("pg_try_advisory_lock"))
        .await
        .unwrap();
    let released: bool = held
        .db()
        .fetch_scalar(&advisory("pg_advisory_unlock"))
        .await
        .unwrap();
    assert!(taken && !taken_elsewhere && released);
    drop((held, other));
    println!(
        "3. lock on a held Connection: taken {taken}, by another connection {taken_elsewhere}, \
         released on the holder {released}; after dropping both: {provider:?}"
    );

    // 4. evict(): the connection is closed, never handed out again.
    let mut doomed = provider.acquire().await.unwrap();
    let doomed_pid: i32 = doomed.db().fetch_scalar(&pid()).await.unwrap();
    doomed.evict();
    let handed_out = pids_in_pool(&provider, 3).await;
    assert!(!handed_out.contains(&doomed_pid));
    println!("4. evicted {doomed_pid}; the pool now hands out {handed_out:?}");

    // 5. Transactions: commit keeps, rollback drops, and a transaction dropped without either
    //    has its connection closed, so the database rolls it back.
    let manager = TransactionManager::new(ShardIdentifier::DEFAULT, provider.clone(), None);
    let note = |id: i32| {
        Query::insert()
            .into_table("notes")
            .columns(["id"])
            .values_panic([id.into()])
            .to_owned()
    };
    let notes = Query::select()
        .expr(Func::count(Expr::col(Asterisk)))
        .from("notes")
        .to_owned();

    let mut committed = manager.begin().await.unwrap();
    committed.db().execute(&note(1)).await.unwrap();
    committed.commit().await.unwrap();

    let mut rolled_back = manager.begin().await.unwrap();
    rolled_back.db().execute(&note(2)).await.unwrap();
    rolled_back.rollback().await.unwrap();

    let mut abandoned = manager.begin().await.unwrap();
    let abandoned_pid: i32 = abandoned.db().fetch_scalar(&pid()).await.unwrap();
    abandoned.db().execute(&note(3)).await.unwrap();
    drop(abandoned);

    let count: i64 = provider.db().fetch_scalar(&notes).await.unwrap();
    let handed_out = pids_in_pool(&provider, 3).await;
    assert_eq!(count, 1);
    assert!(!handed_out.contains(&abandoned_pid));
    println!(
        "5. committed note 1, rolled back note 2, abandoned note 3 on {abandoned_pid}: \
         {count} note saved; {abandoned_pid} was closed, the pool hands out {handed_out:?}"
    );
}
