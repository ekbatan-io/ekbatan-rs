//! The wallet repository on real databases: PostgreSQL, MySQL and MariaDB, two shards each.
//! The same scenario runs on all three.

use ekbatan::{
    Clock, Dialect, Entity, Error, Model, Persistable, Repository, ShardIdentifier, SystemClock,
};
use rust_decimal::Decimal;
use sea_query::{Asterisk, Cond, Expr, ExprTrait, MysqlQueryBuilder, PostgresQueryBuilder, Query};
use uuid::Uuid;
use wallet_example::model::{Notification, NotificationState, Wallet, WalletState};
use wallet_example::repository::{NotificationRepository, WalletRepository};
use wallet_example::schema::{Wallets, WalletsRecord};

mod common;

use common::{GLOBAL, MEXICO, start};

#[tokio::test(flavor = "multi_thread")]
async fn on_postgres() {
    run_scenario(&start(Dialect::Postgres).await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mysql() {
    run_scenario(&start(Dialect::MySql).await).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mariadb() {
    run_scenario(&start(Dialect::MariaDb).await).await;
}

/// The same statement, written for each database by sea-query.
#[test]
fn one_statement_two_sql_flavours() {
    let id = Uuid::nil();
    let update = Query::update()
        .table(Wallets::Table)
        .value(Wallets::State, "SETTLED")
        .and_where(Expr::col(Wallets::Id).is_in([id]))
        .to_owned();
    println!(
        "PostgreSQL:      {}",
        update.to_string(PostgresQueryBuilder)
    );
    println!("MySQL / MariaDB: {}", update.to_string(MysqlQueryBuilder));
}

async fn run_scenario(cluster: &common::Cluster) {
    let dialect = cluster.dialect;
    let registry = cluster.registry().await;
    let wallets = WalletRepository::new(registry.clone());
    let owner = Uuid::new_v4();
    let say = |what: &str| println!("[{dialect:?}] {what}");

    // add + get_by_id: one wallet on each shard, outside any transaction.
    let global = open(GLOBAL, owner, "EUR", Decimal::new(10000, 2));
    let mexico = open(MEXICO, owner, "MXN", Decimal::new(5000, 2));
    wallets.add(None, &global).await.expect("adds");
    wallets.add(None, &mexico).await.expect("adds");
    assert_eq!(wallets.get_by_id(mexico.id()).await.expect("reads"), mexico);
    say("add + get_by_id: the wallet reads back equal (UUID, decimal, microsecond times)");

    // The id picks the shard: the Mexican wallet is in shard 1/0's database only.
    let by_id = Query::select()
        .column(Asterisk)
        .from(Wallets::Table)
        .and_where(Expr::col(Wallets::Id).eq(mexico.id().uuid()))
        .to_owned();
    let in_global = wallets
        .db(GLOBAL)
        .unwrap()
        .fetch_optional::<WalletsRecord, _>(&by_id)
        .await
        .unwrap();
    let in_mexico = wallets
        .db(MEXICO)
        .unwrap()
        .fetch_optional::<WalletsRecord, _>(&by_id)
        .await
        .unwrap();
    assert!(in_global.is_none() && in_mexico.is_some());
    say("db(shard): the Mexican wallet is in shard 1/0's database and not in 0/0's");

    // Optimistic lock: two writers read version 1; the second one loses.
    let read_once = wallets.get_by_id(mexico.id()).await.unwrap();
    let read_twice = read_once.clone();
    let richer = read_once
        .copy()
        .balance(read_once.balance + Decimal::ONE)
        .build()
        .unwrap();
    wallets.update(None, &richer).await.unwrap();
    let stale = wallets.update(None, &read_twice).await;
    assert!(
        matches!(stale, Err(Error::StaleRecord { version: 1, .. })),
        "{stale:?}"
    );
    let after = wallets.get_by_id(mexico.id()).await.unwrap();
    assert_eq!((after.version(), after.balance), (2, Decimal::new(5100, 2)));
    say("update: the second writer of version 1 gets StaleRecord; the row is at version 2");

    // Transactions: an Err rolls back both writes; an Ok commits both.
    let mexico_tm = registry.transaction_manager(MEXICO).unwrap();
    let extra = open(MEXICO, owner, "MXN", Decimal::ZERO);
    let failed = mexico_tm
        .in_transaction(async |tx| {
            wallets.add(Some(tx), &extra).await?;
            wallets.mark_all_settled(Some(tx), &[*mexico.id()]).await?;
            Err::<(), _>(Error::Mapping("changed my mind".to_owned()))
        })
        .await;
    assert!(failed.is_err());
    assert!(!wallets.exists_by_id(extra.id()).await.unwrap());
    assert_eq!(
        wallets.get_by_id(mexico.id()).await.unwrap().state(),
        WalletState::Opened
    );
    mexico_tm
        .in_transaction(async |tx| {
            wallets.add(Some(tx), &extra).await?;
            wallets.mark_all_settled(Some(tx), &[*mexico.id()]).await?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(wallets.exists_by_id(extra.id()).await.unwrap());
    assert_eq!(
        wallets.get_by_id(mexico.id()).await.unwrap().state(),
        WalletState::Settled
    );
    say("tx_db_else_db(Some(tx)): an Err rolls back the add and the settle; an Ok commits both");

    // A transaction on another shard is refused, not quietly bypassed.
    let wrong = registry
        .transaction_manager(GLOBAL)
        .unwrap()
        .in_transaction(async |tx| wallets.mark_all_settled(Some(tx), &[*extra.id()]).await)
        .await;
    assert!(matches!(wrong, Err(Error::WrongShard { .. })), "{wrong:?}");
    say("tx_db: a transaction on shard 0/0 can't write a wallet on shard 1/0");

    // Scatter-gather: every shard at once.
    let opened = Cond::all().add(Expr::col(Wallets::State).eq(WalletState::Opened.as_str()));
    assert_eq!(wallets.count_where(opened.clone()).await.unwrap(), 2);
    assert_eq!(wallets.find_all_where(opened).await.unwrap().len(), 2);
    assert_eq!(wallets.total_opened_wallets().await.unwrap(), 2);
    assert_eq!(wallets.find_all_by_owner_id(owner).await.unwrap().len(), 3);
    say("dbs / readonly_dbs: counts and lists span both shards");

    // Soft delete: a DELETED wallet is gone from every read.
    let current = wallets.get_by_id(global.id()).await.unwrap();
    wallets
        .update(
            None,
            &current.copy().state(WalletState::Deleted).build().unwrap(),
        )
        .await
        .unwrap();
    assert!(wallets.find_by_id(global.id()).await.unwrap().is_none());
    assert_eq!(wallets.find_all_by_owner_id(owner).await.unwrap().len(), 2);
    say("soft delete: find_by_id and the owner's list no longer see it");

    // Idempotent insert: ON CONFLICT DO NOTHING / INSERT IGNORE.
    let once = open(MEXICO, owner, "MXN", Decimal::ONE);
    assert!(wallets.add_if_absent(None, &once).await.unwrap());
    assert!(!wallets.add_if_absent(None, &once).await.unwrap());
    say("add_if_absent: the second insert of the same wallet is skipped, not an error");

    // Totals per currency: each shard groups and sums; the shards' sums are added up.
    let euros = open(GLOBAL, owner, "EUR", Decimal::new(700, 2));
    let pesos = open(GLOBAL, owner, "MXN", Decimal::new(2000, 2));
    wallets.add(None, &euros).await.unwrap();
    wallets.add(None, &pesos).await.unwrap();
    let totals = wallets.total_balance_by_currency().await.unwrap();
    assert_eq!(totals.get("EUR"), Some(&Decimal::new(700, 2)), "{totals:?}");
    assert_eq!(
        totals.get("MXN"),
        Some(&Decimal::new(7200, 2)),
        "{totals:?}"
    );
    say("total_balance_by_currency: GROUP BY on each shard, added up across shards");

    // The richest MXN wallets, from both shards: a top 2 per shard, merged.
    let richest: Vec<_> = wallets
        .richest_wallets("MXN", 2)
        .await
        .unwrap()
        .iter()
        .map(|wallet| *wallet.id())
        .collect();
    assert_eq!(richest, [*mexico.id(), *pesos.id()]);
    say("richest_wallets: ORDER BY + LIMIT on each shard, merged into one top 2");

    // One UPDATE closes the empty wallet and raises its version.
    let before = wallets.get_by_id(extra.id()).await.unwrap();
    assert_eq!(wallets.close_empty_wallets(None, MEXICO).await.unwrap(), 1);
    let closed = wallets.get_by_id(extra.id()).await.unwrap();
    assert_eq!(
        (closed.state(), closed.version()),
        (WalletState::Closed, before.version() + 1)
    );
    let saved_over = wallets.update(None, &before).await;
    assert!(
        matches!(saved_over, Err(Error::StaleRecord { .. })),
        "{saved_over:?}"
    );
    say("close_empty_wallets: one UPDATE closes the empty wallet; an older copy is now stale");

    java_methods(&wallets, dialect).await;
    notifications(&NotificationRepository::new(registry.clone()), cluster).await;
}

/// Each of Java's repository methods, on wallets of a fresh owner.
async fn java_methods(wallets: &WalletRepository, dialect: Dialect) {
    let say = |what: &str| println!("[{dialect:?}] {what}");
    let owner = Uuid::new_v4();
    let of_owner = || Cond::all().add(Expr::col(Wallets::OwnerId).eq(owner));

    // add: gives back the row as written (RETURNING), or on MySQL the model as given.
    let one = open(MEXICO, owner, "MXN", Decimal::new(100, 0));
    assert_eq!(wallets.add(None, &one).await.unwrap(), one);

    // addAll: one multi-row INSERT; models on two shards are refused.
    let batch: Vec<Wallet> = (1..=3)
        .map(|n| open(MEXICO, owner, "MXN", Decimal::new(n, 0)))
        .collect();
    let mut added = wallets.add_all(None, &batch).await.unwrap();
    added.sort_by_key(|wallet| wallet.balance);
    assert_eq!(added, batch);
    let mixed = [
        open(GLOBAL, owner, "EUR", Decimal::ONE),
        open(MEXICO, owner, "MXN", Decimal::ONE),
    ];
    assert!(matches!(
        wallets.add_all(None, &mixed).await,
        Err(Error::CrossShard { .. })
    ));
    say("add / add_all: rows given back as written; two shards in one call refused");

    // update: version + 1, other columns as the model has them (updated_date included).
    let richer = one.copy().balance(Decimal::new(150, 0)).build().unwrap();
    let updated = wallets.update(None, &richer).await.unwrap();
    assert_eq!(updated, richer.next_version());
    assert_eq!(wallets.get_by_id(one.id()).await.unwrap(), updated);
    assert!(matches!(
        wallets.update_no_result(None, &richer).await,
        Err(Error::StaleRecord { version: 1, .. })
    ));
    say("update: version 1 -> 2, given back; an update from version 1 again is stale");

    // updateAll: ONE statement for every model; all or nothing on staleness.
    let doubled: Vec<Wallet> = batch
        .iter()
        .map(|wallet| {
            wallet
                .copy()
                .balance(wallet.balance * Decimal::TWO)
                .build()
                .unwrap()
        })
        .collect();
    let mut all_updated = wallets.update_all(None, &doubled).await.unwrap();
    all_updated.sort_by_key(|wallet| wallet.balance);
    let expected: Vec<Wallet> = doubled.iter().map(Wallet::next_version).collect();
    assert_eq!(all_updated, expected);
    let stale_again = wallets.update_all(None, &doubled).await;
    assert!(
        matches!(
            stale_again,
            Err(Error::StaleRecords {
                expected: 3,
                updated: 0,
                ..
            })
        ),
        "{stale_again:?}"
    );
    let mut mixed_versions = expected.clone();
    mixed_versions[0] = doubled[0].clone();
    let partly_stale = wallets.update_all_no_result(None, &mixed_versions).await;
    assert!(
        matches!(
            partly_stale,
            Err(Error::StaleRecords {
                expected: 3,
                updated: 2,
                ..
            })
        ),
        "{partly_stale:?}"
    );
    say("update_all: one statement updates all three; stale ones are counted, not skipped");

    // Reads.
    let ids: Vec<_> = batch
        .iter()
        .map(|wallet| *wallet.id())
        .chain([*one.id(), *batch[0].id()])
        .collect();
    assert_eq!(wallets.find_all_by_ids(&ids).await.unwrap().len(), 4);
    assert_eq!(wallets.count_where(of_owner()).await.unwrap(), 4);
    assert!(wallets.find_all().await.unwrap().len() >= 4);
    assert!(wallets.count().await.unwrap() >= 4);
    assert!(wallets.exists_where(of_owner()).await.unwrap());
    assert!(matches!(
        wallets.find_one_where(of_owner()).await,
        Err(Error::TooManyRows { .. })
    ));
    let only_one = of_owner().add(Expr::col(Wallets::Id).eq(one.id().uuid()));
    assert_eq!(
        wallets.find_one_where(only_one).await.unwrap(),
        Some(updated)
    );
    say("find_all_by_ids / find_all / count / find_one_where / exists_where: as Java's");
}

/// An entity repository, unsharded: every call goes to the default shard.
async fn notifications(notifications: &NotificationRepository, cluster: &common::Cluster) {
    let dialect = cluster.dialect;
    // A row on the other shard, put there behind ekbatan's back: an unsharded repository must
    // never see it.
    cluster
        .run_sql(
            MEXICO,
            &format!(
                "INSERT INTO notifications VALUES ('{}', 1, 'PENDING', '{}', 'STRAY', 'r', 'm')",
                Uuid::new_v4(),
                Uuid::new_v4()
            ),
        )
        .await;
    let say = |what: &str| println!("[{dialect:?}] {what}");
    let wallet_id = Uuid::new_v4();
    let pending = Notification::create_notification(wallet_id, "DEPOSIT", "a@example.com", "hi")
        .build()
        .unwrap();
    assert_eq!(notifications.add(None, &pending).await.unwrap(), pending);
    let sent = pending.mark_sent().unwrap();
    let updated = notifications.update(None, &sent).await.unwrap();
    assert_eq!(updated.version(), 2);
    assert_eq!(updated.state(), NotificationState::Sent);
    assert_eq!(
        notifications.get_by_id(pending.id()).await.unwrap(),
        updated
    );
    // Reads stay on the default shard: the stray row on the other shard isn't counted.
    assert_eq!(notifications.count().await.unwrap(), 1);
    assert_eq!(notifications.find_all().await.unwrap(), [updated]);
    say("notifications (unsharded entity): add, update, reads only on the default shard");
}

/// A new wallet, as `Wallet::create_wallet` makes it, at the system clock's time.
fn open(shard: ShardIdentifier, owner: Uuid, currency: &str, balance: Decimal) -> Wallet {
    Wallet::create_wallet(shard, owner, currency, balance, SystemClock.instant())
        .build()
        .expect("a valid wallet")
}
