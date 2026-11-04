# Connecting

DataSourceConfig: URLs, logins, pools, driver settings, and TLS. One DataSourceConfig describes how to reach one database and how big its pool may grow.

Every entry of a shard member's `configs` - `primaryConfig`, `secondaryConfig`, `jobsConfig`,
`lockConfig` or any name you add - is one `DataSourceConfig`. The pools, the migrator and the
schema generator all connect the same way, from the same values. The pools are
[sqlx](https://github.com/launchbadge/sqlx) pools: sqlx's PostgreSQL driver for PostgreSQL, and
its MySQL driver for both MySQL and MariaDB.

## The fields

```rust
use std::time::Duration;
use ekbatan::{DataSourceConfig, Dialect};

let config = DataSourceConfig {
    username: Some("wallet".to_owned()),
    password: Some(std::env::var("DB_PASSWORD")?),
    minimum_idle: Some(2),
    connection_timeout: Some(Duration::from_secs(5)),
    data_source_properties: vec![("application_name".to_owned(), "wallet-service".to_owned())],
    ..DataSourceConfig::new(Dialect::Postgres, "postgres://db:5432/wallet", 20)
};
```

`DataSourceConfig::new(dialect, url, maximum_pool_size)` sets what every pool needs; the rest
start empty and keep sqlx's defaults.

| field | default | what it is |
| --- | --- | --- |
| `dialect` | - | `Dialect::Postgres`, `Dialect::MySql` or `Dialect::MariaDb` |
| `url` | - | `postgres://host:port/database`, or `mysql://host:port/database` for MySQL **and** MariaDB. No login in it |
| `username` | `None` | the user to log in as. A blank one is refused |
| `password` | `None` | the login password. Leave it out for a login that has none |
| `maximum_pool_size` | - (10 in a config file) | the most connections the pool opens. At least 1 |
| `minimum_idle` | `None`: 0 | connections kept open even while unused. Not above `maximum_pool_size` |
| `idle_timeout` | `None`: 10 minutes | how long an unused connection stays open |
| `connection_timeout` | `None`: 30 seconds | how long a caller waits for a free connection before failing |
| `data_source_properties` | empty | extra driver settings, by the driver's own names. See [Driver settings](#driver-settings) |
| `leak_detection_threshold` | `None` | accepted so config files written for Ekbatan for Java load unchanged: 0 or at least 2 seconds. It does nothing; sqlx has no leak detection |

The defaults in the table are sqlx's. Two more of sqlx's settings apply and cannot be changed
here: a connection is closed after 30 minutes however busy it is, and each connection is checked
before it is handed out.

**The pool opens no connection when it is built.** The first use opens one. So a service can
start while its database is still coming up, and a wrong password shows up on the first query,
not at startup.

### From a JDBC URL

A config file gives the URL in Ekbatan for Java's form, and `from_jdbc_url` reads it in code too:

```rust
let config = DataSourceConfig::from_jdbc_url(
    "jdbc:mariadb://db:3306/wallet?charset=utf8mb4",
    20, // maximum_pool_size
)?;
// config.dialect == Dialect::MariaDb
// config.url == "mysql://db:3306/wallet"
// config.data_source_properties == [("charset", "utf8mb4")]
```

| URL starts with | dialect |
| --- | --- |
| `jdbc:postgresql://`, `postgres://`, `postgresql://` | `Postgres` |
| `jdbc:mysql://`, `mysql://` | `MySql` |
| `jdbc:mariadb://`, `mariadb://` | `MariaDb` |

Anything else is refused: PostgreSQL, MySQL and MariaDB are the only databases. The URL's query
parameters become `data_source_properties`, in order, and are checked as such. The URL kept is
the address alone.

## The login

**The login goes in `username` and `password`, never in the URL.** A URL gets logged and passed
around, so `from_jdbc_url` - and so every config file - refuses one with `user@` or
`user:password@` in it:

```
the URL carries a login (user@ or user:password@): set username and password instead - a URL gets logged
```

The error does not repeat the URL. A URL built in code with `DataSourceConfig::new` is not
checked for a login, but even there its password is hidden when the config is printed. Keep the
login out of it anyway.

Both are optional. Leave out `username` only when the driver should pick the user itself, and
`password` for a login that needs none.

**The password is never printed.** `Debug` on a `DataSourceConfig`, or on anything that holds one -
a `ShardingConfig`, a member, a migration target - shows `<redacted>` for the password, for a
password written into the URL by mistake, and for **every** `data_source_properties` value. Only
the property names are shown. `tracing::info!("{layout:?}")` is safe to write. No error message
repeats a value either.

## Driver settings

`data_source_properties` are extra settings handed to the driver, as `(name, value)` text pairs,
by the names **sqlx** reads. Only those names are accepted; anything else is refused with the
list of names the driver does read.

**PostgreSQL** (`POSTGRES_PROPERTIES`):

| name | what it does |
| --- | --- |
| `sslmode` | `disable`, `allow`, `prefer` (the default), `require`, `verify-ca`, `verify-full` |
| `sslrootcert` | a CA certificate file to trust, beside the built-in ones |
| `sslcert`, `sslkey` | a client certificate and its key, for a certificate login |
| `statement-cache-capacity` | how many prepared statements each connection keeps |
| `application_name` | the name the server shows for the session |

**MySQL and MariaDB** (`MYSQL_PROPERTIES`):

| name | what it does |
| --- | --- |
| `sslmode` | `disabled`, `preferred` (the default), `required`, `verify_ca`, `verify_identity` - any letter case |
| `sslca` | a CA certificate file to trust |
| `sslcert`, `sslkey` | a client certificate and its key |
| `charset`, `collation` | the session's character set and collation |
| `statement-cache-capacity` | as above |
| `socket` | a Unix socket path, instead of host and port |
| `timezone` | the session's `time_zone`. sqlx sets `+00:00` (UTC) unless told otherwise; leave it - see [Always UTC](/reference/multi-database/#always-utc) |

The rules:

- **Names are the driver's, exactly as written.** A name from a Java driver is refused, with
  sqlx's list: Java's `ApplicationName` is `application_name` here.
- **`user` and `password` are refused**, in any letter case. They would quietly win over
  `username` and `password`; set those instead.
- **A value that does not parse** (`sslmode=sometimes`, a cache size that is not a number) is
  refused, naming the setting and not the value.
- **Every value is treated as a secret** and never printed, so a key path or anything else
  sensitive is safe here.

## TLS

Connections are encrypted with [rustls](https://github.com/rustls/rustls). There is no OpenSSL to
install. The trusted certificate authorities are Mozilla's list, built into the binary; the
operating system's own store is **not** read. To trust another CA - a company CA, or a cloud
provider's - name its file in `sslrootcert` (PostgreSQL) or `sslca` (MySQL, MariaDB).

**By default TLS is used when the server offers it, and the certificate is not checked**
(`prefer` / `preferred`). That stops someone reading the traffic, but not someone pretending to
be the server. To check the certificate and the host name:

```rust
// PostgreSQL
data_source_properties: vec![
    ("sslmode".to_owned(), "verify-full".to_owned()),
    ("sslrootcert".to_owned(), "/run/secrets/db/ca.pem".to_owned()),
],
// MySQL and MariaDB
data_source_properties: vec![
    ("sslmode".to_owned(), "verify_identity".to_owned()),
    ("sslca".to_owned(), "/run/secrets/db/ca.pem".to_owned()),
],
```

Tested on the three databases (`wallet-example/tests/tls.rs`): password logins over TLS 1.3 on
MySQL 8.4 and MariaDB 11.8; on PostgreSQL with TLS on, `require` and `prefer` encrypt, and
`verify-full` refuses a self-signed certificate. No password appears in any error.

### MySQL 8.4 and a password's first login

MySQL 8.4's default login method, `caching_sha2_password`, sends a password safely in one of two
ways: over TLS, or with an RSA key exchange. This framework does only the first. So:

- **With TLS on** (the default, `preferred`), a password login works, as MySQL 8.4 sets up TLS
  for itself.
- **With `sslmode=disabled`**, a password's **first** login is refused. Once the server has seen
  that password over TLS and cached it, a plain login gets in, until the cache is cleared (a
  server restart, or the password changing).

Keep TLS on for MySQL. MariaDB's default login method does not have this rule.

## The pool itself: `ConnectionProvider`

`ConnectionProvider::from_config(&config)` builds one pool from one `DataSourceConfig`. A
`DatabaseRegistry` builds these for each shard's `primaryConfig` and `secondaryConfig`; build the
others (`lockConfig`, `jobsConfig`) yourself:

```rust
let member = layout.member(layout.default_shard).expect("the default shard is configured");
let lock_pool = ConnectionProvider::from_config(
    member.config_for("lockConfig").unwrap_or(member.primary_config()),
)?;
let locks = SqlKeyedLockProvider::new(lock_pool);
```

| | |
| --- | --- |
| `db()` | statements that each borrow a connection only while they run |
| `acquire().await` | one connection, held until it is dropped (back to the pool) |
| `connection.evict()` | close that connection instead of returning it, when its session is in doubt |
| `size()`, `idle()` | connections open now, and how many of them are free |
| `close().await` | close every connection; nothing can be borrowed after |
| `dialect()` | the pool's dialect |

Cloning a `ConnectionProvider` is cheap and shares the same pool. Its `Debug` shows the dialect
and the counts, never the login.

`from_config` runs the checks above and returns `Error::Config` naming the field, never the
value. A `DataSourceConfig` read from a config file has been checked once already, when the file
was read.

Made from code that is not async, a pool lives on the blocking API's own runtime - see
[Blocking and async](/reference/runtime/blocking-and-async/).

## How migrations connect

The migrator connects with the same `DataSourceConfig`: the same URL, login and driver settings,
TLS included, in a pool of one connection that it closes when it is done. See
[Migrations](/reference/migrations/).

## See also

- [Config files](/reference/config-files/) - the same settings, read from JSON with Java's key
  names
- [Sharding](/reference/sharding/) - where each `DataSourceConfig` sits in the layout
- [MySQL](/reference/dialects/mysql/) · [MariaDB](/reference/dialects/mariadb/) ·
  [PostgreSQL](/reference/dialects/postgresql/)
