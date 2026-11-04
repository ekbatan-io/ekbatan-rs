# MariaDB

Close enough to MySQL to share a driver, and where it is not: MariaDB has a uuid type, has RETURNING on inserts, and its default login does not need TLS. The rest of the MySQL page applies.

## Quick reference

| | |
| --- | --- |
| dialect | `Dialect::MariaDb` |
| driver | sqlx's MySQL driver |
| url | `mysql://host:3306/db` with `Dialect::MariaDb`; in a config file `jdbc:mariadb://host:3306/db` |
| login | `username` and `password`, never in the url |
| uuid | `UUID`, native (10.7 and later); `CHAR(36) CHARACTER SET ascii` works too |
| json | `JSON` |
| timestamps | `DATETIME(6)`, holding UTC |
| `RETURNING` | on inserts, yes; on updates, no |
| keyed lock | `SqlKeyedLockProvider`: `GET_LOCK`, server-wide names |
| migration lock | `GET_LOCK`, named after the database |
| TLS | used when the server offers it (`sslmode=preferred`); MariaDB 11.8 offers it by default |
| tested against | `mariadb:11.8` |

**The URL scheme is MySQL's.** sqlx reads MariaDB through its MySQL driver, so in code the URL is
`mysql://...` and the dialect says MariaDB. A config file's `jdbc:mariadb://` (or `mariadb://`)
URL sets both for you. Use `Dialect::MariaDb`, not `MySql`: the framework writes some statements
differently for it.

## Migration order

As on MySQL: `eventlog` is a database of its own, one per server, created by the first migration
on your login's connection.

```
migrations/
  V0000__create_eventlog_database.sql   # CREATE DATABASE IF NOT EXISTS eventlog;
  V0001__eventlog.sql                   # eventlog.events + eventlog.event_notifications + indexes
  V0002__scheduled_tasks.sql            # only with distributed jobs or the local event handler
  V0003__wallets.sql                    # your own tables
```

The login needs the right to create `eventlog` and use it; the examples grant it in an
`init.sql`, as on [MySQL](/reference/dialects/mysql/#migration-order). Two shards on one server
would share `eventlog`: give each its own server.

Copy the framework's files from `wallet-example/migrations/mariadb`.

## Your own tables

| column | type | role |
| --- | --- | --- |
| `id` | `UUID PRIMARY KEY` | read as `ekbatan::DbUuid` |
| `version` | `BIGINT NOT NULL` | optimistic locking |
| `state` | `VARCHAR(24) NOT NULL` | `DELETED` marks a soft-deleted row |
| `created_date` | `DATETIME(6) NOT NULL` | UTC, from the model |
| `updated_date` | `DATETIME(6) NOT NULL` | UTC, from the model |

From these, [schemagen](/reference/schemagen/) writes the same module as from PostgreSQL's and
MySQL's tables.

## Where MariaDB differs from MySQL

**A uuid type.** MariaDB's `UUID` stores 16 bytes and reads back as the usual hyphenated text;
`DbUuid` reads it, and schemagen maps it to `DbUuid` whatever the column is called. The framework
writes uuids to it as text, which MariaDB accepts. The examples' MariaDB migrations use it.

**`RETURNING` on inserts.** `add` gives back the row as MariaDB stored it, as on PostgreSQL.
`update` has no `RETURNING` on MariaDB, so it gives back the model's next version, as on MySQL.

**The default login does not need TLS.** MariaDB's default login method is not MySQL 8.4's
`caching_sha2_password`, so a password login works with or without TLS. MariaDB 11.8 sets up TLS
for itself, and sqlx uses it when offered. Tested (`wallet-example/tests/tls.rs`).

## Shared with MySQL

Everything else on the [MySQL page](/reference/dialects/mysql/#things-to-know) applies:

- `DATETIME(6)`, never a bare `DATETIME`; `scheduled_tasks` keeps db-scheduler's `TIMESTAMP(6)`;
- sqlx sets each session's `time_zone` to `+00:00`: leave the `timezone` setting alone;
- no partial indexes: composite ones, flag first;
- every schema change commits on its own: one per migration file;
- `GET_LOCK` takes its wait in seconds: give lock waits in whole seconds;
- the same driver settings: `sslmode`, `sslca`, `sslcert`, `sslkey`, `charset`, `collation`,
  `statement-cache-capacity`, `socket`, `timezone`.

## See also

- [MySQL](/reference/dialects/mysql/) - the shared ground
- [Framework tables](/reference/tables/) · [Multi-database](/reference/multi-database/)
- [`axum-wallet-rest-mariadb`](https://github.com/ekbatan-io/ekbatan-rs/tree/main/ekbatan-rs-examples/axum-wallet-rest-mariadb)
