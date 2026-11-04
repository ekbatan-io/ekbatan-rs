# Examples

Standalone apps, each its own crate: it imports only `ekbatan` and what its `Cargo.toml` names,
and nothing imports it. Each has a README, its migrations, its config, a generated schema (in
`build.rs`, from a throwaway database with its migrations applied, so Docker is needed to
build), and a test against real databases.

They are members of the repository's workspace, so they share its lock file and the 2-week rule
covers their dependencies too. They are not default members: `cargo test` at the root is the
framework's. Build or test one with `-p`:

```bash
cargo run  -p wallet-service-blocking-pg
cargo test -p axum-wallet-rest-pg
```

## Start here

| Example | Shows |
|---|---|
| [`wallet-service-blocking-pg`](wallet-service-blocking-pg/) | one wallet's life on the **blocking** API, in plain code: open, deposit, read, its events, close, a refused change. Needs only Docker: it starts a throwaway database itself |

## The REST service, one per database

| Database | Example |
|---|---|
| PostgreSQL | [`axum-wallet-rest-pg`](axum-wallet-rest-pg/) |
| MySQL | [`axum-wallet-rest-mysql`](axum-wallet-rest-mysql/) |
| MariaDB | [`axum-wallet-rest-mariadb`](axum-wallet-rest-mariadb/) |

Java's `*-wallet-rest-*` examples in Rust, on the **async** API (`ekbatan::aio`) and
[axum](https://github.com/tokio-rs/axum). It has four endpoints and runs its migrations at
startup. Each database gets three pools (requests, jobs, locks). The deposit runs under a keyed
lock, and an event handler records a notification after each deposit.

The three are the same app. Four things differ, each a real difference rather than a setting:

- the **URL** scheme (`jdbc:postgresql://`, `jdbc:mysql://`, `jdbc:mariadb://`);
- the **migrations**: native `UUID` and `JSONB` against `CHAR(36)` and `JSON`;
- the **database the schema is generated from**;
- the **test database**. The MySQL and MariaDB ones log in with a password over TLS, and their
  `init.sql` lets the app's user create the `eventlog` database the first migration makes.

**The PostgreSQL project is the source.** Change it, then run
`python3 ekbatan-rs-examples/sync-examples.py`: it writes the MySQL and MariaDB copies.
`--check` fails if a copy is out of date.

## Beyond the database matrix

These exist once, on PostgreSQL, as their Java counterparts do: a copy per database would show
nothing the REST service doesn't.

| Example | Shows |
|---|---|
| [`axum-wallet-rest-sharded-pg`](axum-wallet-rest-sharded-pg/) | the REST service over **two** databases: the shard packed into each wallet's id, `MX` wallets on the Mexico shard, every later call routed by the id alone |
| [`axum-wallet-saga-pg`](axum-wallet-saga-pg/) | a transfer as three single-wallet steps chained by events, with forward-only compensation and idempotent replays. `POST /wallets/transfers` answers **202**, because only step one has committed |
| [`job-worker-pg`](job-worker-pg/) | a worker with **no HTTP**, on the **blocking** API: a scheduled query and a job that runs an action, each once per slot across every instance, and a clean shutdown |

## Blocking or async

The REST services, the sharded one and the saga use the async API, as Rust web services
usually are. The start-here service and the worker use the blocking API: plain functions, no
runtime of their own. Both APIs run the same engine, so the actions, events, transactions and
retries behave the same. See `ekbatan::blocking` and `ekbatan::aio`.
