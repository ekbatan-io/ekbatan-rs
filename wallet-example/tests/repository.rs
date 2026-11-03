//! The wallet repository on real databases: PostgreSQL, MySQL and MariaDB, two shards each
//! (two databases in one server). The same scenario runs on all three.

use std::sync::Arc;

use ekbatan::{
    ConnectionProvider, DataSourceConfig, DatabaseRegistry, Dialect, Error, ModelRepository,
    ShardIdentifier, TransactionManager,
};
use rust_decimal::Decimal;
use sea_query::{Asterisk, Cond, Expr, ExprTrait, MysqlQueryBuilder, PostgresQueryBuilder, Query};
use sqlx::Connection;
use sqlx::mysql::MySqlConnection;
use sqlx::postgres::PgConnection;
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;
use wallet_example::model::{Wallet, WalletState};
use wallet_example::repository::WalletRepository;
use wallet_example::schema::{Wallets, WalletsRecord};

const GLOBAL: ShardIdentifier = ShardIdentifier::new(0, 0);
const MEXICO: ShardIdentifier = ShardIdentifier::new(1, 0);

// The wallets table of the Java examples, per database (V0003__wallets.sql).
const POSTGRES_WALLETS: &str = "CREATE TABLE wallets (id UUID PRIMARY KEY, version BIGINT NOT NULL, \
    state VARCHAR(24) NOT NULL, owner_id UUID NOT NULL, currency CHAR(3) NOT NULL, \
    balance DECIMAL(19, 4) NOT NULL, created_date TIMESTAMP NOT NULL, updated_date TIMESTAMP NOT NULL)";
const MYSQL_WALLETS: &str = "CREATE TABLE wallets (id CHAR(36) CHARACTER SET ascii NOT NULL, \
    version BIGINT NOT NULL, state VARCHAR(24) NOT NULL, owner_id CHAR(36) CHARACTER SET ascii NOT NULL, \
    currency CHAR(3) NOT NULL, balance DECIMAL(19, 4) NOT NULL, created_date DATETIME(6) NOT NULL, \
    updated_date DATETIME(6) NOT NULL, PRIMARY KEY (id))";
const MARIADB_WALLETS: &str = "CREATE TABLE wallets (id UUID PRIMARY KEY, version BIGINT NOT NULL, \
    state VARCHAR(24) NOT NULL, owner_id UUID NOT NULL, currency CHAR(3) NOT NULL, \
    balance DECIMAL(19, 4) NOT NULL, created_date DATETIME(6) NOT NULL, updated_date DATETIME(6) NOT NULL)";

#[tokio::test(flavor = "multi_thread")]
async fn on_postgres() {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("PostgreSQL starts");
    let port = container.get_host_port_ipv4(5432).await.expect("a port");
    let server = Server {
        dialect: Dialect::Postgres,
        host: format!("postgres://127.0.0.1:{port}"),
        username: "postgres",
        password: Some("postgres"),
        first_database: "postgres",
    };
    run_scenario(&server, POSTGRES_WALLETS).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mysql() {
    let container = Mysql::default()
        .with_tag("8.4")
        .start()
        .await
        .expect("MySQL starts");
    let port = container.get_host_port_ipv4(3306).await.expect("a port");
    let server = Server {
        dialect: Dialect::MySql,
        host: format!("mysql://127.0.0.1:{port}"),
        username: "root",
        password: None,
        first_database: "test",
    };
    run_scenario(&server, MYSQL_WALLETS).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mariadb() {
    let container = Mariadb::default()
        .with_tag("11.8")
        .start()
        .await
        .expect("MariaDB starts");
    let port = container.get_host_port_ipv4(3306).await.expect("a port");
    let server = Server {
        dialect: Dialect::MariaDb,
        host: format!("mysql://127.0.0.1:{port}"),
        username: "root",
        password: None,
        first_database: "test",
    };
    run_scenario(&server, MARIADB_WALLETS).await;
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

/// A test database server: where it listens, and how to log in.
struct Server {
    dialect: Dialect,
    host: String,
    username: &'static str,
    password: Option<&'static str>,
    first_database: &'static str,
}

impl Server {
    /// The settings an app would read from its config, for one database on this server.
    fn config(&self, database: &str) -> DataSourceConfig {
        DataSourceConfig {
            username: Some(self.username.to_owned()),
            password: self.password.map(str::to_owned),
            ..DataSourceConfig::new(self.dialect, format!("{}/{database}", self.host), 5)
        }
    }

    /// Setup SQL, run with plain sqlx: creating databases and tables isn't ekbatan's job.
    async fn run_sql(&self, database: &str, sql: &'static str) {
        let login = match self.password {
            Some(password) => format!("{}:{password}@", self.username),
            None => format!("{}@", self.username),
        };
        let host = self.host.replacen("://", &format!("://{login}"), 1);
        let url = format!("{host}/{database}");
        match self.dialect {
            Dialect::Postgres => {
                let mut connection = PgConnection::connect(&url).await.expect("connects");
                sqlx::raw_sql(sql)
                    .execute(&mut connection)
                    .await
                    .expect("setup SQL runs");
            }
            Dialect::MySql | Dialect::MariaDb => {
                let mut connection = MySqlConnection::connect(&url).await.expect("connects");
                sqlx::raw_sql(sql)
                    .execute(&mut connection)
                    .await
                    .expect("setup SQL runs");
            }
        }
    }
}

/// What an app builds from its config: per shard, a primary pool and a replica pool in a
/// `TransactionManager`, all in a `DatabaseRegistry`. Here two databases on one server stand in
/// for two shards, and a second pool on the same database stands in for the replica.
async fn registry(server: &Server, create_wallets: &'static str) -> Arc<DatabaseRegistry> {
    server
        .run_sql(server.first_database, "CREATE DATABASE shard_1_0")
        .await;
    let mut transaction_managers = Vec::new();
    for (shard, database) in [(GLOBAL, server.first_database), (MEXICO, "shard_1_0")] {
        server.run_sql(database, create_wallets).await;
        let primary = ConnectionProvider::from_config(&server.config(database)).expect("a pool");
        let secondary = ConnectionProvider::from_config(&server.config(database)).expect("a pool");
        transaction_managers.push(TransactionManager::new(shard, primary, Some(secondary)));
    }
    Arc::new(DatabaseRegistry::new(GLOBAL, transaction_managers).expect("a registry"))
}

async fn run_scenario(server: &Server, create_wallets: &'static str) {
    let dialect = server.dialect;
    let registry = registry(server, create_wallets).await;
    let wallets = WalletRepository::new(registry.clone());
    let owner = Uuid::new_v4();
    let say = |what: &str| println!("[{dialect:?}] {what}");

    // add + get_by_id: one wallet on each shard, outside any transaction.
    let global = Wallet::open(GLOBAL, owner, "EUR", Decimal::new(10000, 2));
    let mexico = Wallet::open(MEXICO, owner, "MXN", Decimal::new(5000, 2));
    wallets.add(None, &global).await.expect("adds");
    wallets.add(None, &mexico).await.expect("adds");
    assert_eq!(wallets.get_by_id(&mexico.id).await.expect("reads"), mexico);
    say("add + get_by_id: the wallet reads back equal (UUID, decimal, microsecond times)");

    // The id picks the shard: the Mexican wallet is in shard 1/0's database only.
    let by_id = Query::select()
        .column(Asterisk)
        .from(Wallets::Table)
        .and_where(Expr::col(Wallets::Id).eq(mexico.id.uuid()))
        .to_owned();
    let in_global = wallets
        .db(GLOBAL)
        .unwrap()
        .fetch_optional::<WalletsRecord>(&by_id)
        .await
        .unwrap();
    let in_mexico = wallets
        .db(MEXICO)
        .unwrap()
        .fetch_optional::<WalletsRecord>(&by_id)
        .await
        .unwrap();
    assert!(in_global.is_none() && in_mexico.is_some());
    say("db(shard): the Mexican wallet is in shard 1/0's database and not in 0/0's");

    // Optimistic lock: two writers read version 1; the second one loses.
    let read_once = wallets.get_by_id(&mexico.id).await.unwrap();
    let read_twice = read_once.clone();
    let richer = Wallet {
        balance: read_once.balance + Decimal::ONE,
        ..read_once
    };
    wallets.update(None, &richer).await.unwrap();
    let stale = wallets.update(None, &read_twice).await;
    assert!(
        matches!(stale, Err(Error::StaleRecord { version: 1, .. })),
        "{stale:?}"
    );
    let after = wallets.get_by_id(&mexico.id).await.unwrap();
    assert_eq!((after.version, after.balance), (2, Decimal::new(5100, 2)));
    say("update: the second writer of version 1 gets StaleRecord; the row is at version 2");

    // Transactions: an Err rolls back both writes; an Ok commits both.
    let mexico_tm = registry.transaction_manager(MEXICO).unwrap();
    let extra = Wallet::open(MEXICO, owner, "MXN", Decimal::ZERO);
    let failed = mexico_tm
        .in_transaction(async |tx| {
            wallets.add(Some(tx), &extra).await?;
            wallets.mark_all_settled(Some(tx), &[mexico.id]).await?;
            Err::<(), _>(Error::Mapping("changed my mind".to_owned()))
        })
        .await;
    assert!(failed.is_err());
    assert!(!wallets.exists_by_id(&extra.id).await.unwrap());
    assert_eq!(
        wallets.get_by_id(&mexico.id).await.unwrap().state,
        WalletState::Opened
    );
    mexico_tm
        .in_transaction(async |tx| {
            wallets.add(Some(tx), &extra).await?;
            wallets.mark_all_settled(Some(tx), &[mexico.id]).await?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(wallets.exists_by_id(&extra.id).await.unwrap());
    assert_eq!(
        wallets.get_by_id(&mexico.id).await.unwrap().state,
        WalletState::Settled
    );
    let saved_over = wallets.update(None, &after).await;
    assert!(
        matches!(saved_over, Err(Error::StaleRecord { .. })),
        "{saved_over:?}"
    );
    say("tx_db_else_db(Some(tx)): an Err rolls back the add and the settle; an Ok commits both");
    say("bulk_update: a copy read before the settle can't be saved over it (StaleRecord)");

    // A transaction on another shard is refused, not quietly bypassed.
    let wrong = registry
        .transaction_manager(GLOBAL)
        .unwrap()
        .in_transaction(async |tx| wallets.mark_all_settled(Some(tx), &[extra.id]).await)
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
    let current = wallets.get_by_id(&global.id).await.unwrap();
    wallets
        .update(
            None,
            &Wallet {
                state: WalletState::Deleted,
                ..current
            },
        )
        .await
        .unwrap();
    assert!(wallets.find_by_id(&global.id).await.unwrap().is_none());
    assert_eq!(wallets.find_all_by_owner_id(owner).await.unwrap().len(), 2);
    say("soft delete: find_by_id and the owner's list no longer see it");

    // Idempotent insert: ON CONFLICT DO NOTHING / INSERT IGNORE.
    let once = Wallet::open(MEXICO, owner, "MXN", Decimal::ONE);
    assert!(wallets.add_if_absent(None, &once).await.unwrap());
    assert!(!wallets.add_if_absent(None, &once).await.unwrap());
    say("add_if_absent: the second insert of the same wallet is skipped, not an error");

    // Totals per currency: each shard groups and sums; the shards' sums are added up.
    let euros = Wallet::open(GLOBAL, owner, "EUR", Decimal::new(700, 2));
    let pesos = Wallet::open(GLOBAL, owner, "MXN", Decimal::new(2000, 2));
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
        .map(|wallet| wallet.id)
        .collect();
    assert_eq!(richest, [mexico.id, pesos.id]);
    say("richest_wallets: ORDER BY + LIMIT on each shard, merged into one top 2");

    // One UPDATE closes the empty wallet and raises its version.
    let before = wallets.get_by_id(&extra.id).await.unwrap();
    assert_eq!(wallets.close_empty_wallets(None, MEXICO).await.unwrap(), 1);
    let closed = wallets.get_by_id(&extra.id).await.unwrap();
    assert_eq!(
        (closed.state, closed.version),
        (WalletState::Closed, before.version + 1)
    );
    let saved_over = wallets.update(None, &before).await;
    assert!(
        matches!(saved_over, Err(Error::StaleRecord { .. })),
        "{saved_over:?}"
    );
    say("close_empty_wallets: one UPDATE closes the empty wallet; an older copy is now stale");
}
