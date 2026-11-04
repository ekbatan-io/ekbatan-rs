//! `cargo run -p wallet-service-blocking-pg`: one wallet's life, told step by step.
//!
//! With `DATABASE_URL` set (a `jdbc:postgresql://` URL, with `DATABASE_USER` and
//! `DATABASE_PASSWORD`), it uses that database. Without it, it starts a throwaway
//! PostgreSQL in Docker, and removes it at the end.

use ekbatan::{DataSourceConfig, Dialect};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::SyncRunner;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut _throwaway = None;
    let database = match std::env::var("DATABASE_URL") {
        Ok(url) => {
            let mut config = DataSourceConfig::from_jdbc_url(&url, 5)?;
            config.username = std::env::var("DATABASE_USER").ok();
            config.password = std::env::var("DATABASE_PASSWORD").ok();
            config
        }
        Err(_) => {
            println!("no DATABASE_URL: starting a throwaway PostgreSQL in Docker");
            let container = Postgres::default().with_tag("17-alpine").start()?;
            let port = container.get_host_port_ipv4(5432)?;
            _throwaway = Some(container);
            let mut config = DataSourceConfig::new(
                Dialect::Postgres,
                format!("postgres://127.0.0.1:{port}/postgres"),
                5,
            );
            config.username = Some("postgres".to_owned());
            config.password = Some("postgres".to_owned());
            config
        }
    };
    wallet_service_blocking_pg::story::run(database, |line| println!("{line}"))?;
    Ok(())
}
