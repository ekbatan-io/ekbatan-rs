//! TLS (rustls) and password logins on real databases: each server's own session says the
//! connection is encrypted, and certificates are checked when `sslmode` asks for it.
//!
//! - MySQL 8.4: a password login (its default `caching_sha2_password`) works because the
//!   session is TLS. With TLS turned off, the first login is refused (sqlx has no RSA
//!   exchange); once the server has cached the password, a plain login gets in.
//! - MariaDB 11.8: a password login, over the TLS the server sets up for itself.
//! - PostgreSQL with TLS on (the image's self-signed certificate): `sslmode=require` encrypts;
//!   `verify-full` refuses the self-signed certificate.

use ekbatan::{ConnectionProvider, DataSourceConfig, Dialect};
use sea_query::{Expr, Query};
use testcontainers_modules::mariadb::Mariadb;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// What the server reports about this session's encryption: the TLS version, or the
/// PostgreSQL `pg_stat_ssl.ssl` flag as text.
async fn session_tls(
    config: &DataSourceConfig,
    sql: &'static str,
) -> Result<String, ekbatan::Error> {
    let provider = ConnectionProvider::from_config(config)?;
    let mut db = provider.db();
    let select = Query::select().expr(Expr::cust(sql)).to_owned();
    let value = db.fetch_scalar::<String>(&select).await;
    provider.close().await;
    value
}

fn config(dialect: Dialect, url: String, properties: &[(&str, &str)]) -> DataSourceConfig {
    let mut config = DataSourceConfig::new(dialect, url, 2);
    config.username = Some("wallet".to_owned());
    config.password = Some("wallet-secret".to_owned());
    config.data_source_properties = properties
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_password_login_over_tls() {
    let container = Mysql::default()
        .with_tag("8.4")
        .with_env_var("MYSQL_USER", "wallet")
        .with_env_var("MYSQL_PASSWORD", "wallet-secret")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let url = format!("mysql://127.0.0.1:{port}/test");
    let status = "(SELECT VARIABLE_VALUE FROM performance_schema.session_status \
                  WHERE VARIABLE_NAME = 'Ssl_version')";

    // Before the server has seen this password over TLS, a login without TLS is refused.
    let disabled = config(Dialect::MySql, url.clone(), &[("sslmode", "disabled")]);
    let refused = session_tls(&disabled, status)
        .await
        .unwrap_err()
        .to_string();
    assert!(refused.contains("TLS"), "{refused}");
    assert!(!refused.contains("wallet-secret"), "{refused}");
    println!("MySQL 8.4, first login with sslmode=disabled: refused ({refused})");

    let version = session_tls(&config(Dialect::MySql, url.clone(), &[]), status)
        .await
        .unwrap();
    assert!(version.starts_with("TLSv1."), "{version:?}");
    let required = session_tls(
        &config(Dialect::MySql, url.clone(), &[("sslmode", "required")]),
        status,
    )
    .await
    .unwrap();
    assert!(required.starts_with("TLSv1."), "{required:?}");
    println!("MySQL 8.4: password login over {version}, by default and with sslmode=required");

    // Once it has, MySQL has the password cached and lets a plain login in: unencrypted.
    let plain = session_tls(&disabled, status).await.unwrap();
    assert_eq!(plain, "");
    println!(
        "MySQL 8.4, after a TLS login: sslmode=disabled gets in unencrypted (cached password)"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mariadb_password_login_over_tls() {
    let container = Mariadb::default()
        .with_tag("11.8")
        .with_env_var("MARIADB_USER", "wallet")
        .with_env_var("MARIADB_PASSWORD", "wallet-secret")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let url = format!("mysql://127.0.0.1:{port}/test");
    let version = session_tls(
        &config(Dialect::MariaDb, url, &[]),
        "(SELECT VARIABLE_VALUE FROM information_schema.SESSION_STATUS \
         WHERE VARIABLE_NAME = 'Ssl_version')",
    )
    .await
    .unwrap();
    assert!(version.starts_with("TLSv1."), "{version:?}");
    println!("MariaDB 11.8: password login over {version}");
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_tls_and_certificate_checks() {
    let container = Postgres::default()
        .with_user("wallet")
        .with_password("wallet-secret")
        .with_db_name("wallet")
        .with_tag("17")
        .with_cmd([
            "postgres",
            "-c",
            "ssl=on",
            "-c",
            "ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem",
            "-c",
            "ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key",
        ])
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://127.0.0.1:{port}/wallet");
    let encrypted = "(SELECT ssl::text FROM pg_stat_ssl WHERE pid = pg_backend_pid())";

    let required = session_tls(
        &config(Dialect::Postgres, url.clone(), &[("sslmode", "require")]),
        encrypted,
    )
    .await
    .unwrap();
    assert_eq!(required, "true");
    let preferred = session_tls(&config(Dialect::Postgres, url.clone(), &[]), encrypted)
        .await
        .unwrap();
    assert_eq!(preferred, "true");
    println!("PostgreSQL with ssl=on: encrypted with sslmode=require, and by default (prefer)");

    let refused = session_tls(
        &config(Dialect::Postgres, url, &[("sslmode", "verify-full")]),
        encrypted,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(!refused.contains("wallet-secret"), "{refused}");
    println!(
        "PostgreSQL with sslmode=verify-full: the self-signed certificate is refused ({refused})"
    );
}
