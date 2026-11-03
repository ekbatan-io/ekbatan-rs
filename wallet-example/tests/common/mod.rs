//! Test databases for the example's tests: two shards, migrated with the example's migration
//! files (`migrations/<database>`), as an app does at startup.
//!
//! PostgreSQL: one server, a database per shard. MySQL and MariaDB: a server per shard, since
//! their `eventlog` is a database of its own, one per server.

#![allow(dead_code)]

use std::any::Any;
use std::sync::Arc;

use ekbatan::migration::Migrator;
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
/// container stops when the last reference is dropped.
pub struct Server {
    pub dialect: Dialect,
    pub host: String,
    pub username: &'static str,
    pub password: Option<&'static str>,
    pub first_database: &'static str,
    _container: Box<dyn Any + Send + Sync>,
}

/// One shard: a database on a server.
pub struct ShardDatabase {
    pub shard: ShardIdentifier,
    pub server: Arc<Server>,
    pub database: String,
}

/// The two shards of a test.
pub struct Cluster {
    pub dialect: Dialect,
    pub shards: Vec<ShardDatabase>,
}

/// Starts the servers for `dialect`'s two shards (empty databases, nothing migrated yet).
pub async fn start(dialect: Dialect) -> Cluster {
    let shards = match dialect {
        Dialect::Postgres => {
            let server = Arc::new(start_server(dialect).await);
            server
                .run_sql(server.first_database, "CREATE DATABASE shard_1_0")
                .await;
            vec![
                ShardDatabase {
                    shard: GLOBAL,
                    server: server.clone(),
                    database: server.first_database.to_owned(),
                },
                ShardDatabase {
                    shard: MEXICO,
                    server,
                    database: "shard_1_0".to_owned(),
                },
            ]
        }
        Dialect::MySql | Dialect::MariaDb => {
            let (global, mexico) = tokio::join!(start_server(dialect), start_server(dialect));
            [(GLOBAL, global), (MEXICO, mexico)]
                .into_iter()
                .map(|(shard, server)| ShardDatabase {
                    shard,
                    database: server.first_database.to_owned(),
                    server: Arc::new(server),
                })
                .collect()
        }
    };
    Cluster { dialect, shards }
}

async fn start_server(dialect: Dialect) -> Server {
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

    /// SQL outside ekbatan, run with plain sqlx.
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
                    .expect("SQL runs");
            }
            Dialect::MySql | Dialect::MariaDb => {
                let mut connection = MySqlConnection::connect(&url).await.expect("connects");
                sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned()))
                    .execute(&mut connection)
                    .await
                    .expect("SQL runs");
            }
        }
    }
}

impl Cluster {
    pub fn shard(&self, shard: ShardIdentifier) -> &ShardDatabase {
        self.shards
            .iter()
            .find(|s| s.shard == shard)
            .expect("a test shard")
    }

    pub fn config(&self, shard: ShardIdentifier) -> DataSourceConfig {
        let s = self.shard(shard);
        s.server.config(&s.database)
    }

    pub async fn run_sql(&self, shard: ShardIdentifier, sql: &str) {
        let s = self.shard(shard);
        s.server.run_sql(&s.database, sql).await;
    }

    /// The example's migration folder for this database.
    pub fn migrations(&self) -> String {
        let folder = match self.dialect {
            Dialect::Postgres => "postgres",
            Dialect::MySql => "mysql",
            Dialect::MariaDb => "mariadb",
        };
        format!("{}/migrations/{folder}", env!("CARGO_MANIFEST_DIR"))
    }

    /// Migrates every shard, as an app does at startup.
    pub async fn migrate(&self) {
        Migrator::builder()
            .with_targets(
                self.shards
                    .iter()
                    .map(|s| Migrator::target(s.shard, s.server.config(&s.database))),
            )
            .locations([self.migrations()])
            .migrate()
            .await
            .expect("migrations apply");
    }

    /// Migrates, then builds what an app builds from its config: per shard, a primary pool and
    /// a replica pool in a `TransactionManager`, all in a `DatabaseRegistry`. A second pool on
    /// the same database stands in for the replica.
    pub async fn registry(&self) -> Arc<DatabaseRegistry> {
        self.migrate().await;
        let mut transaction_managers = Vec::new();
        for s in &self.shards {
            let config = s.server.config(&s.database);
            let primary = ConnectionProvider::from_config(&config).expect("a pool");
            let secondary = ConnectionProvider::from_config(&config).expect("a pool");
            transaction_managers.push(TransactionManager::new(s.shard, primary, Some(secondary)));
        }
        Arc::new(DatabaseRegistry::new(GLOBAL, transaction_managers).expect("a registry"))
    }
}
