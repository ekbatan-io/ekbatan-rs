use std::fmt;
use std::time::Duration;

use crate::Dialect;

/// How to reach one database and how big its pool may grow: Java's `DataSourceConfig`.
/// Login details go in `username` / `password`, not in the URL. `Debug` never prints the
/// password, nor one put in the URL by mistake.
#[derive(Clone)]
pub struct DataSourceConfig {
    pub dialect: Dialect,
    /// `postgres://host:port/database` or `mysql://host:port/database`.
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// The most connections the pool opens. Java: `maximumPoolSize`.
    pub maximum_pool_size: u32,
    /// Connections kept open even while unused. Java: `minimumIdle`.
    pub minimum_idle: Option<u32>,
    /// How long an unused connection stays open. Java: `idleTimeout`.
    pub idle_timeout: Option<Duration>,
    /// How long to wait for a free connection before failing. Hikari: `connectionTimeout`.
    pub connection_timeout: Option<Duration>,
}

impl DataSourceConfig {
    /// The settings every pool needs; the rest keep their defaults.
    pub fn new(dialect: Dialect, url: impl Into<String>, maximum_pool_size: u32) -> Self {
        DataSourceConfig {
            dialect,
            url: url.into(),
            username: None,
            password: None,
            maximum_pool_size,
            minimum_idle: None,
            idle_timeout: None,
            connection_timeout: None,
        }
    }
}

impl fmt::Debug for DataSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataSourceConfig")
            .field("dialect", &self.dialect)
            .field("url", &redact_url(&self.url))
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("maximum_pool_size", &self.maximum_pool_size)
            .field("minimum_idle", &self.minimum_idle)
            .field("idle_timeout", &self.idle_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .finish()
    }
}

/// `scheme://user:secret@host/db` -> `scheme://user:<redacted>@host/db`.
fn redact_url(url: &str) -> String {
    let Some(start) = url.find("://").map(|at| at + 3) else {
        return url.to_owned();
    };
    let rest = &url[start..];
    let authority = &rest[..rest.find('/').unwrap_or(rest.len())];
    let Some(at) = authority.rfind('@') else {
        return url.to_owned();
    };
    let Some(colon) = authority[..at].find(':') else {
        return url.to_owned();
    };
    format!(
        "{}{}:<redacted>{}",
        &url[..start],
        &rest[..colon],
        &rest[at..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const PASSWORD: &str = "Wallet-Secret-77";

    #[test]
    fn debug_never_shows_the_password() {
        let mut config = DataSourceConfig::new(Dialect::Postgres, "postgres://db:5432/wallets", 5);
        config.username = Some("app".to_owned());
        config.password = Some(PASSWORD.to_owned());
        let shown = format!("{config:?}");
        assert!(!shown.contains(PASSWORD), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    #[test]
    fn debug_hides_a_password_put_in_the_url() {
        let config = DataSourceConfig::new(
            Dialect::MySql,
            format!("mysql://app:{PASSWORD}@db:3306/wallets"),
            5,
        );
        let shown = format!("{config:?}");
        assert!(!shown.contains(PASSWORD), "{shown}");
        assert!(
            shown.contains("mysql://app:<redacted>@db:3306/wallets"),
            "{shown}"
        );
    }
}
