# ekbatan-rs

The Rust port of [Ekbatan](https://github.com/ekbatan-io/ekbatan) (Java) and its Python port
`ekbatan-py`. The crate is `ekbatan`; the repository is `ekbatan-rs`.

## Layout

- `ekbatan/` - the framework crate.
  - `config.rs` `DataSourceConfig`: how to reach one database, and its pool sizes.
  - `connection.rs` `ConnectionProvider` (one database's pool) and `Connection` (one held connection).
  - `transaction.rs` `TransactionManager` (one shard's pools and transactions) and `Transaction`.
  - `registry.rs` `DatabaseRegistry`: every shard's `TransactionManager`.
  - `db.rs` `Db`: where statements run and which SQL to write (jOOQ's `DSLContext`).
  - `repository.rs` `ModelRepository`: Java's `AbstractRepository`, with the database selectors.
  - `shard.rs` `ShardIdentifier`, `ShardedId` (Java's `ShardedUUID` bit layout).
  - `types.rs` `DbUuid`: UUID columns on every database (Java's `UuidStringConverter`).
  - `clock.rs` `Clock`, `SystemClock`, `Instant` (UTC), `truncate_to_micros`.
- `ekbatan-test-support/` - what tests of an app use: `VirtualClock` (Java's, case for case).
- `wallet-example/` - the wallet model, its schema and repository, and the tests on real databases.
- `age-pins/` - no code: exact pins that hold transitive crates to the 2-week rule.
- `docs/LEDGER.md` - what is done, what is next, and decisions taken.

## Rules

- Names follow Java: Java's type names, Java's field and method names in snake_case. Only what
  Rust forces differs (no inheritance, no `ScopedValue`, `Result` instead of exceptions).
- SQL is built with sea-query and run with sqlx. Repository code never names a database or a
  SQL flavour; `Db` does.
- Supported databases: PostgreSQL, MySQL, MariaDB. A change to the database layer is proven on
  all three (`cargo test`, needs Docker).
- 2-week rule: a crate release is adopted only once it is at least 14 days old, transitive ones
  included. Stable cargo has no date cutoff, so young transitive crates are pinned in `age-pins/`.
- The toolchain is pinned in `rust-toolchain.toml` (sqlx 0.9 needs Rust 1.94+).
- Never print secrets: no password in any `Debug`, log, error or result. `DataSourceConfig`
  redacts its password, and one put in the URL.
- Before calling work done: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test`.
