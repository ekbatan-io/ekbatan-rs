//! Test databases for the example's tests: a container per database kind, two shards on it (two
//! databases in one server), and the example's tables.

#![allow(dead_code)]

use std::any::Any;
use std::sync::Arc;

use ekbatan::{
    ConnectionProvider, DataSourceConfig, DatabaseRegistry, Dialect, ShardIdentifier,
    TransactionManager,
};
use sqlx::Connection;
use sqlx::mysql::MySqlConnection;
use sqlx::postgres::PgConnection;
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

pub const GLOBAL: ShardIdentifier = ShardIdentifier::new(0, 0);
pub const MEXICO: ShardIdentifier = ShardIdentifier::new(1, 0);

/// A test database server: where it listens, and how to log in. Holds its container; the
/// container stops when this is dropped.
pub struct Server {
    pub dialect: Dialect,
    pub host: String,
    pub username: &'static str,
    pub password: Option<&'static str>,
    pub first_database: &'static str,
    _container: Box<dyn Any + Send>,
}

/// Starts a container of `dialect`'s database (PostgreSQL 17, MySQL 8.4, MariaDB 11.8).
pub async fn start(dialect: Dialect) -> Server {
    match dialect {
        Dialect::Postgres => {
            let container = Postgres::default()
                .with_tag("17-alpine")
                .start()
                .await
                .expect("PostgreSQL starts");
            let port = container.get_host_port_ipv4(5432).await.expect("a port");
            Server {
                dialect,
                host: format!("postgres://127.0.0.1:{port}"),
                username: "postgres",
                password: Some("postgres"),
                first_database: "postgres",
                _container: Box::new(container),
            }
        }
        Dialect::MySql => {
            let container = Mysql::default()
                .with_tag("8.4")
                .start()
                .await
                .expect("MySQL starts");
            let port = container.get_host_port_ipv4(3306).await.expect("a port");
            Server {
                dialect,
                host: format!("mysql://127.0.0.1:{port}"),
                username: "root",
                password: None,
                first_database: "test",
                _container: Box::new(container),
            }
        }
        Dialect::MariaDb => {
            let container = Mariadb::default()
                .with_tag("11.8")
                .start()
                .await
                .expect("MariaDB starts");
            let port = container.get_host_port_ipv4(3306).await.expect("a port");
            Server {
                dialect,
                host: format!("mysql://127.0.0.1:{port}"),
                username: "root",
                password: None,
                first_database: "test",
                _container: Box::new(container),
            }
        }
    }
}

impl Server {
    /// The settings an app would read from its config, for one database on this server.
    pub fn config(&self, database: &str) -> DataSourceConfig {
        DataSourceConfig {
            username: Some(self.username.to_owned()),
            password: self.password.map(str::to_owned),
            ..DataSourceConfig::new(self.dialect, format!("{}/{database}", self.host), 5)
        }
    }

    /// Setup SQL, run with plain sqlx: creating databases and tables isn't ekbatan's job.
    pub async fn run_sql(&self, database: &str, sql: &str) {
        let login = match self.password {
            Some(password) => format!("{}:{password}@", self.username),
            None => format!("{}@", self.username),
        };
        let host = self.host.replacen("://", &format!("://{login}"), 1);
        let url = format!("{host}/{database}");
        match self.dialect {
            Dialect::Postgres => {
                let mut connection = PgConnection::connect(&url).await.expect("connects");
                sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned()))
                    .execute(&mut connection)
                    .await
                    .expect("setup SQL runs");
            }
            Dialect::MySql | Dialect::MariaDb => {
                let mut connection = MySqlConnection::connect(&url).await.expect("connects");
                sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned()))
                    .execute(&mut connection)
                    .await
                    .expect("setup SQL runs");
            }
        }
    }

    /// The databases standing in for the two shards.
    pub fn shard_databases(&self) -> [(ShardIdentifier, &'static str); 2] {
        [(GLOBAL, self.first_database), (MEXICO, "shard_1_0")]
    }

    /// What an app builds from its config: per shard, a primary pool and a replica pool in a
    /// `TransactionManager`, all in a `DatabaseRegistry`. Here a second pool on the same
    /// database stands in for the replica. Creates the tables first: wallets and the event log
    /// on both shards, notifications on the default shard only.
    pub async fn registry(&self) -> Arc<DatabaseRegistry> {
        self.run_sql(self.first_database, "CREATE DATABASE shard_1_0")
            .await;
        let schema = Schema::of(self.dialect);
        let mut transaction_managers = Vec::new();
        for (shard, database) in self.shard_databases() {
            self.run_sql(database, schema.wallets).await;
            self.run_sql(database, schema.eventlog).await;
            if shard == GLOBAL {
                self.run_sql(database, schema.notifications).await;
            }
            let primary = ConnectionProvider::from_config(&self.config(database)).expect("a pool");
            let secondary =
                ConnectionProvider::from_config(&self.config(database)).expect("a pool");
            transaction_managers.push(TransactionManager::new(shard, primary, Some(secondary)));
        }
        Arc::new(DatabaseRegistry::new(GLOBAL, transaction_managers).expect("a registry"))
    }
}

/// The example's tables, per database: Java's example migrations.
pub struct Schema {
    pub wallets: &'static str,
    pub notifications: &'static str,
    /// `eventlog.events`: a schema per database on PostgreSQL; one database per server on MySQL
    /// and MariaDB, so there both "shards" of a test server share it.
    pub eventlog: &'static str,
}

impl Schema {
    pub fn of(dialect: Dialect) -> Schema {
        match dialect {
            Dialect::Postgres => Schema {
                wallets: "CREATE TABLE wallets (id UUID PRIMARY KEY, version BIGINT NOT NULL, \
                    state VARCHAR(24) NOT NULL, owner_id UUID NOT NULL, currency CHAR(3) NOT NULL, \
                    balance DECIMAL(19, 4) NOT NULL, created_date TIMESTAMP NOT NULL, \
                    updated_date TIMESTAMP NOT NULL)",
                notifications: "CREATE TABLE notifications (id UUID PRIMARY KEY, \
                    version BIGINT NOT NULL, state VARCHAR(24) NOT NULL, wallet_id UUID NOT NULL, \
                    kind VARCHAR(48) NOT NULL, recipient VARCHAR(255) NOT NULL, message TEXT NOT NULL)",
                eventlog: "CREATE SCHEMA IF NOT EXISTS eventlog; \
                    CREATE TABLE eventlog.events (id UUID PRIMARY KEY, namespace VARCHAR(255) NOT NULL, \
                    action_id UUID NOT NULL, action_name VARCHAR(255) NOT NULL, \
                    action_params JSONB NOT NULL, started_date TIMESTAMP NOT NULL, \
                    completion_date TIMESTAMP NOT NULL, model_id VARCHAR(255), \
                    model_type VARCHAR(255), event_type VARCHAR(255), payload JSONB, \
                    event_date TIMESTAMP NOT NULL, delivered BOOLEAN NOT NULL)",
            },
            Dialect::MySql => Schema {
                wallets: "CREATE TABLE wallets (id CHAR(36) CHARACTER SET ascii NOT NULL, \
                    version BIGINT NOT NULL, state VARCHAR(24) NOT NULL, \
                    owner_id CHAR(36) CHARACTER SET ascii NOT NULL, currency CHAR(3) NOT NULL, \
                    balance DECIMAL(19, 4) NOT NULL, created_date DATETIME(6) NOT NULL, \
                    updated_date DATETIME(6) NOT NULL, PRIMARY KEY (id))",
                notifications: "CREATE TABLE notifications (id CHAR(36) CHARACTER SET ascii NOT NULL, \
                    version BIGINT NOT NULL, state VARCHAR(24) NOT NULL, \
                    wallet_id CHAR(36) CHARACTER SET ascii NOT NULL, kind VARCHAR(48) NOT NULL, \
                    recipient VARCHAR(255) NOT NULL, message TEXT NOT NULL, PRIMARY KEY (id))",
                eventlog: "CREATE DATABASE IF NOT EXISTS eventlog; \
                    CREATE TABLE IF NOT EXISTS eventlog.events (id CHAR(36) CHARACTER SET ascii NOT NULL, \
                    namespace VARCHAR(255) NOT NULL, action_id CHAR(36) CHARACTER SET ascii NOT NULL, \
                    action_name VARCHAR(255) NOT NULL, action_params JSON NOT NULL, \
                    started_date DATETIME(6) NOT NULL, completion_date DATETIME(6) NOT NULL, \
                    model_id VARCHAR(255), model_type VARCHAR(255), event_type VARCHAR(255), \
                    payload JSON, event_date DATETIME(6) NOT NULL, delivered BOOLEAN NOT NULL, \
                    PRIMARY KEY (id))",
            },
            Dialect::MariaDb => Schema {
                wallets: "CREATE TABLE wallets (id UUID PRIMARY KEY, version BIGINT NOT NULL, \
                    state VARCHAR(24) NOT NULL, owner_id UUID NOT NULL, currency CHAR(3) NOT NULL, \
                    balance DECIMAL(19, 4) NOT NULL, created_date DATETIME(6) NOT NULL, \
                    updated_date DATETIME(6) NOT NULL)",
                notifications: "CREATE TABLE notifications (id UUID PRIMARY KEY, \
                    version BIGINT NOT NULL, state VARCHAR(24) NOT NULL, wallet_id UUID NOT NULL, \
                    kind VARCHAR(48) NOT NULL, recipient VARCHAR(255) NOT NULL, message TEXT NOT NULL)",
                eventlog: "CREATE DATABASE IF NOT EXISTS eventlog; \
                    CREATE TABLE IF NOT EXISTS eventlog.events (id UUID PRIMARY KEY, \
                    namespace VARCHAR(255) NOT NULL, action_id UUID NOT NULL, \
                    action_name VARCHAR(255) NOT NULL, action_params JSON NOT NULL, \
                    started_date DATETIME(6) NOT NULL, completion_date DATETIME(6) NOT NULL, \
                    model_id VARCHAR(255), model_type VARCHAR(255), event_type VARCHAR(255), \
                    payload JSON, event_date DATETIME(6) NOT NULL, delivered BOOLEAN NOT NULL)",
            },
        }
    }
}
