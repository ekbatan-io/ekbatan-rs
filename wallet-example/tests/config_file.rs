//! An app started from a config file: the `ekbatan:` section, in Java's layout, read from a
//! JSON file; migrations and the registry built from it; a driver setting from
//! `dataSourceProperties` seen by the database itself.

mod common;

use ekbatan::aio::AsyncRepository;
use ekbatan::migration::Migrator;
use ekbatan::{DatabaseRegistry, Dialect, EkbatanConfig, ShardIdentifier};
use sea_query::{Expr, Query};
use serde_json::json;
use wallet_example::repository::WalletRepository;

use common::start;

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

async fn run(dialect: Dialect) {
    let cluster = start(dialect).await;
    let (scheme, property) = match dialect {
        Dialect::Postgres => (
            "jdbc:postgresql",
            json!({ "application_name": "wallet-service" }),
        ),
        Dialect::MySql => ("jdbc:mysql", json!({ "timezone": "+05:30" })),
        Dialect::MariaDb => ("jdbc:mariadb", json!({ "timezone": "+05:30" })),
    };
    let members = |group: u8| {
        let s = &cluster.shards[group as usize];
        let address = s.server.host.split_once("://").unwrap().1;
        json!([{
            "member": 0,
            "name": if group == 0 { "global" } else { "mexico" },
            "configs": { "primary-config": {
                "jdbc-url": format!("{scheme}://{address}/{}", s.database),
                "username": s.server.username,
                "password": s.server.password.map(|_| "${TEST_DB_PASSWORD}"),
                "data-source-properties": property,
                "maximum-pool-size": 4
            }}
        }])
    };
    let mut tree = json!({ "ekbatan": {
        "namespace": "example.wallet",
        "sharding": {
            "default-shard": { "group": 0, "member": 0 },
            "groups": [
                { "group": 0, "name": "global", "members": members(0) },
                { "group": 1, "name": "mexico", "members": members(1) }
            ]
        }
    }});
    // MySQL's and MariaDB's test login has no password: leave the key out there.
    for group in 0..2 {
        let primary = &mut tree["ekbatan"]["sharding"]["groups"][group]["members"][0]["configs"]["primary-config"];
        if primary["password"].is_null() {
            primary.as_object_mut().unwrap().remove("password");
        }
    }
    let path =
        std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("ekbatan-{dialect:?}.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&tree).unwrap()).unwrap();
    // SAFETY: set before any thread reads the environment in this test binary's other tests;
    // each test sets the same value.
    unsafe { std::env::set_var("TEST_DB_PASSWORD", "postgres") };

    let config = EkbatanConfig::load_json(&path).unwrap();
    let sharding = config.sharding.unwrap();
    Migrator::builder()
        .with_sharding_config(&sharding)
        .locations([cluster.migrations()])
        .migrate()
        .await
        .unwrap();
    let registry = std::sync::Arc::new(DatabaseRegistry::from_sharding_config(&sharding).unwrap());
    let wallets = WalletRepository::new(registry.clone());
    assert_eq!(wallets.count().await.unwrap(), 0);

    // The driver setting reached the session.
    let mut db = registry
        .as_async()
        .primary(ShardIdentifier::new(1, 0))
        .unwrap();
    let setting = match dialect {
        Dialect::Postgres => Query::select()
            .expr(Expr::cust("current_setting('application_name')"))
            .to_owned(),
        Dialect::MySql | Dialect::MariaDb => Query::select()
            .expr(Expr::cust("@@session.time_zone"))
            .to_owned(),
    };
    let value: String = db.fetch_scalar(&setting).await.unwrap();
    let expected = if dialect == Dialect::Postgres {
        "wallet-service"
    } else {
        "+05:30"
    };
    assert_eq!(value, expected);
    println!(
        "[{dialect:?}] config file: migrated both shards, registry built, setting seen: {value}"
    );
}
