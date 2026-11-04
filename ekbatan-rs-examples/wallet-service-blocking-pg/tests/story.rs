//! The story against a PostgreSQL in Docker, checked step by step.

use ekbatan::{DataSourceConfig, Dialect};
use rust_decimal::Decimal;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::SyncRunner;

#[test]
fn one_wallets_life() {
    let container = Postgres::default().with_tag("17-alpine").start().unwrap();
    let port = container.get_host_port_ipv4(5432).unwrap();
    let mut database = DataSourceConfig::new(
        Dialect::Postgres,
        format!("postgres://127.0.0.1:{port}/postgres"),
        5,
    );
    database.username = Some("postgres".to_owned());
    database.password = Some("postgres".to_owned());

    let mut lines = Vec::new();
    let ending = wallet_service_blocking_pg::story::run(database, |line| {
        println!("{line}");
        lines.push(line);
    })
    .unwrap();

    assert_eq!(lines[0], "1. migrated: 4 migration file(s) applied");
    assert_eq!(ending.balance, Decimal::new(3550, 2));
    assert_eq!((ending.state.as_str(), ending.version), ("CLOSED", 3));
    assert_eq!(
        ending.event_types,
        ["WalletCreatedEvent", "WalletMoneyDepositedEvent"]
    );
    assert_eq!(ending.refused, "Deposit amount must be positive");
    assert_eq!(lines.len(), 7);
}
