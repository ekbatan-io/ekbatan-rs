use std::fmt;
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use sqlx::postgres::{PgConnectOptions, PgSslMode};

use crate::{Dialect, Error};

/// The `data_source_properties` sqlx's PostgreSQL driver reads, by its own names.
pub const POSTGRES_PROPERTIES: &[&str] = &[
    "sslmode",
    "sslrootcert",
    "sslcert",
    "sslkey",
    "statement-cache-capacity",
    "application_name",
];

/// The `data_source_properties` sqlx's MySQL driver (MySQL and MariaDB) reads, by its own names.
pub const MYSQL_PROPERTIES: &[&str] = &[
    "sslmode",
    "sslca",
    "sslcert",
    "sslkey",
    "charset",
    "collation",
    "statement-cache-capacity",
    "socket",
    "timezone",
];

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
    /// Extra settings for the driver, by the driver's own names (`application_name`,
    /// `sslmode`, ...): Java's `dataSourceProperties`. Every value is treated as a secret and
    /// never printed. Only names sqlx's driver reads are accepted (`POSTGRES_PROPERTIES`,
    /// `MYSQL_PROPERTIES`); `user` and `password` never, as in Java.
    pub data_source_properties: Vec<(String, String)>,
    /// Java's `leakDetectionThreshold`: 0 (off) or at least 2s, as Hikari accepts. sqlx has no
    /// leak detection, so it is checked and kept, and does nothing.
    pub leak_detection_threshold: Option<Duration>,
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
            data_source_properties: Vec::new(),
            leak_detection_threshold: None,
        }
    }

    /// From Java's JDBC URL: `jdbc:postgresql://host:5432/db`, `jdbc:mysql://...`,
    /// `jdbc:mariadb://...` (sqlx's own `postgres://` and `mysql://` too). The dialect comes from
    /// the scheme; query parameters become `data_source_properties`. As in Java, the URL may not
    /// carry a login: a URL gets logged.
    pub fn from_jdbc_url(jdbc_url: &str, maximum_pool_size: u32) -> Result<Self, Error> {
        let (dialect, rest) = if let Some(rest) = jdbc_url.strip_prefix("jdbc:postgresql://") {
            (Dialect::Postgres, rest)
        } else if let Some(rest) = jdbc_url.strip_prefix("jdbc:mysql://") {
            (Dialect::MySql, rest)
        } else if let Some(rest) = jdbc_url.strip_prefix("jdbc:mariadb://") {
            (Dialect::MariaDb, rest)
        } else if let Some(rest) = jdbc_url
            .strip_prefix("postgres://")
            .or_else(|| jdbc_url.strip_prefix("postgresql://"))
        {
            (Dialect::Postgres, rest)
        } else if let Some(rest) = jdbc_url.strip_prefix("mysql://") {
            (Dialect::MySql, rest)
        } else if let Some(rest) = jdbc_url.strip_prefix("mariadb://") {
            (Dialect::MariaDb, rest)
        } else {
            // The URL is not repeated: it may hold a password.
            return Err(Error::Config(
                "Cannot determine the database dialect from the URL. A JDBC URL looks like \
                 jdbc:postgresql://host:5432/db; PostgreSQL, MySQL and MariaDB are supported."
                    .to_owned(),
            ));
        };
        let (address, query) = rest.split_once('?').unwrap_or((rest, ""));
        let authority = address.split('/').next().unwrap_or("");
        if authority.contains('@') {
            return Err(Error::Config(
                "the URL carries a login (user@ or user:password@): set username and password \
                 instead - a URL gets logged"
                    .to_owned(),
            ));
        }
        let scheme = match dialect {
            Dialect::Postgres => "postgres",
            Dialect::MySql | Dialect::MariaDb => "mysql",
        };
        let mut config =
            DataSourceConfig::new(dialect, format!("{scheme}://{address}"), maximum_pool_size);
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            config
                .data_source_properties
                .push((name.to_owned(), value.to_owned()));
        }
        Ok(config)
    }

    /// Java's checks, made when a pool is built from the config and when a config file is read.
    /// No message repeats a value.
    pub fn validate(&self) -> Result<(), Error> {
        if self.url.trim().is_empty() {
            return Err(Error::Config("jdbcUrl is required".to_owned()));
        }
        if self
            .username
            .as_deref()
            .is_some_and(|u| u.trim().is_empty())
        {
            return Err(Error::Config(
                "username cannot be empty; leave it out when the driver supplies the user"
                    .to_owned(),
            ));
        }
        if self.maximum_pool_size == 0 {
            return Err(Error::Config(
                "maximumPoolSize must be at least 1".to_owned(),
            ));
        }
        if let Some(minimum) = self.minimum_idle
            && minimum > self.maximum_pool_size
        {
            return Err(Error::Config(format!(
                "minimumIdle cannot exceed maximumPoolSize ({})",
                self.maximum_pool_size
            )));
        }
        if let Some(threshold) = self.leak_detection_threshold
            && !threshold.is_zero()
            && threshold < Duration::from_secs(2)
        {
            return Err(Error::Config(
                "leakDetectionThreshold must be 0 (disabled) or at least 2000 ms; Hikari \
                 disables anything smaller"
                    .to_owned(),
            ));
        }
        let known = match self.dialect {
            Dialect::Postgres => POSTGRES_PROPERTIES,
            Dialect::MySql | Dialect::MariaDb => MYSQL_PROPERTIES,
        };
        for (name, _) in &self.data_source_properties {
            if name.trim().is_empty() {
                return Err(Error::Config(
                    "a dataSourceProperties name cannot be blank".to_owned(),
                ));
            }
            if name.eq_ignore_ascii_case("user") || name.eq_ignore_ascii_case("password") {
                let login = if name.eq_ignore_ascii_case("user") {
                    "username"
                } else {
                    "password"
                };
                return Err(Error::Config(format!(
                    "'{name}' does not belong in dataSourceProperties, where it would silently \
                     win over {login}; set {login} instead"
                )));
            }
            if !known.contains(&name.as_str()) {
                return Err(Error::Config(format!(
                    "dataSourceProperties '{name}' is not a setting sqlx's {:?} driver reads; it \
                     reads: {}",
                    self.dialect,
                    known.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// The PostgreSQL connect options: URL, login, then the properties.
    pub(crate) fn postgres_options(&self) -> Result<PgConnectOptions, Error> {
        let mut options: PgConnectOptions = self.url.parse()?;
        if let Some(username) = &self.username {
            options = options.username(username);
        }
        if let Some(password) = &self.password {
            options = options.password(password);
        }
        for (name, value) in &self.data_source_properties {
            options = match name.as_str() {
                "sslmode" => options.ssl_mode(parse::<PgSslMode>(name, value)?),
                "sslrootcert" => options.ssl_root_cert(value.as_str()),
                "sslcert" => options.ssl_client_cert(value.as_str()),
                "sslkey" => options.ssl_client_key(value.as_str()),
                "statement-cache-capacity" => {
                    options.statement_cache_capacity(parse::<usize>(name, value)?)
                }
                "application_name" => options.application_name(value),
                _ => {
                    return Err(Error::Config(format!(
                        "dataSourceProperties '{name}' is not read by sqlx's PostgreSQL driver"
                    )));
                }
            };
        }
        Ok(options)
    }

    /// The MySQL / MariaDB connect options: URL, login, then the properties.
    pub(crate) fn mysql_options(&self) -> Result<MySqlConnectOptions, Error> {
        let mut options: MySqlConnectOptions = self.url.parse()?;
        if let Some(username) = &self.username {
            options = options.username(username);
        }
        if let Some(password) = &self.password {
            options = options.password(password);
        }
        for (name, value) in &self.data_source_properties {
            options = match name.as_str() {
                "sslmode" => options.ssl_mode(parse::<MySqlSslMode>(name, value)?),
                "sslca" => options.ssl_ca(value.as_str()),
                "sslcert" => options.ssl_client_cert(value.as_str()),
                "sslkey" => options.ssl_client_key(value.as_str()),
                "charset" => options.charset(value),
                "collation" => options.collation(value),
                "statement-cache-capacity" => {
                    options.statement_cache_capacity(parse::<usize>(name, value)?)
                }
                "socket" => options.socket(value),
                "timezone" => options.timezone(Some(value.clone())),
                _ => {
                    return Err(Error::Config(format!(
                        "dataSourceProperties '{name}' is not read by sqlx's MySQL driver"
                    )));
                }
            };
        }
        Ok(options)
    }
}

/// A property's value as the type sqlx wants; the error names the property, not the value.
fn parse<T: std::str::FromStr>(name: &str, value: &str) -> Result<T, Error> {
    value.parse().map_err(|_| {
        Error::Config(format!(
            "dataSourceProperties '{name}' has a value sqlx can't read"
        ))
    })
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
            .field(
                "data_source_properties",
                &self
                    .data_source_properties
                    .iter()
                    .map(|(name, _)| (name.as_str(), "<redacted>"))
                    .collect::<Vec<_>>(),
            )
            .field("leak_detection_threshold", &self.leak_detection_threshold)
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
    fn debug_shows_property_names_never_values() {
        let mut config = DataSourceConfig::new(Dialect::Postgres, "postgres://db:5432/wallets", 5);
        config
            .data_source_properties
            .push(("sslkey".to_owned(), PASSWORD.to_owned()));
        let shown = format!("{config:?}");
        assert!(!shown.contains(PASSWORD), "{shown}");
        assert!(shown.contains("sslkey"), "{shown}");
    }

    #[test]
    fn jdbc_urls_give_the_dialect_and_properties() {
        let config =
            DataSourceConfig::from_jdbc_url("jdbc:mariadb://db:3306/app?charset=utf8mb4", 5)
                .unwrap();
        assert_eq!(config.dialect, Dialect::MariaDb);
        assert_eq!(config.url, "mysql://db:3306/app");
        assert_eq!(
            config.data_source_properties,
            [("charset".to_owned(), "utf8mb4".to_owned())]
        );
        assert_eq!(
            DataSourceConfig::from_jdbc_url("jdbc:postgresql://h/db", 1)
                .unwrap()
                .dialect,
            Dialect::Postgres
        );
        let login =
            DataSourceConfig::from_jdbc_url(&format!("jdbc:postgresql://app:{PASSWORD}@h/db"), 1)
                .unwrap_err()
                .to_string();
        assert!(!login.contains(PASSWORD), "{login}");
        assert!(DataSourceConfig::from_jdbc_url("jdbc:oracle:thin:@h:1521:x", 1).is_err());
    }

    #[test]
    fn java_checks_refuse_bad_settings_by_name() {
        let base = DataSourceConfig::new(Dialect::Postgres, "postgres://h/db", 5);
        assert!(base.validate().is_ok());
        let refused = |change: fn(&mut DataSourceConfig)| {
            let mut config = base.clone();
            change(&mut config);
            config.validate().unwrap_err().to_string()
        };
        assert!(refused(|c| c.maximum_pool_size = 0).contains("maximumPoolSize"));
        assert!(refused(|c| c.minimum_idle = Some(6)).contains("minimumIdle"));
        assert!(refused(|c| c.username = Some(" ".to_owned())).contains("username"));
        assert!(
            refused(|c| c.leak_detection_threshold = Some(Duration::from_millis(500)))
                .contains("leakDetectionThreshold")
        );
        let message = refused(|c| {
            c.data_source_properties
                .push(("PASSWORD".to_owned(), "Wallet-Secret-77".to_owned()))
        });
        assert!(message.contains("set password instead") && !message.contains("Wallet-Secret-77"));
        assert!(
            refused(|c| c
                .data_source_properties
                .push(("ApplicationName".to_owned(), "x".to_owned())))
            .contains("application_name")
        );
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
