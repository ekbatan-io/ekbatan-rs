# ekbatan-rs

The Rust port of [Ekbatan](https://github.com/ekbatan-io/ekbatan) (Java) and its Python port
`ekbatan-py`. The crate is `ekbatan`; the repository is `ekbatan-rs`.

## Layout

- `ekbatan/` - the framework crate.
  - `config.rs` `DataSourceConfig`: how to reach one database, its pool sizes, driver settings.
  - `sharding_config.rs` `ShardingConfig`, `EkbatanConfig`: Java's `ekbatan:` config section.
  - `connection.rs` `ConnectionProvider` (one database's pool) and `Connection` (one held connection).
  - `transaction.rs` `TransactionManager` (one shard's pools and transactions) and `Transaction`.
  - `registry.rs` `DatabaseRegistry`: every shard's `TransactionManager`.
  - `db.rs` `Db`: where statements run and which SQL to write (jOOQ's `DSLContext`).
  - `action/` `Action`, `ActionPlan`, `ActionExecutor`, the registries, `ExecutionConfiguration`,
    and the event log (`EventPersister`, `eventlog.events`).
  - `migration.rs` `Migrator`: Java's `FlywayMigrator` on sqlx's migrator (Flyway file names).
  - `distributed_jobs/` `JobRegistry`, `DistributedJob`: run-once-per-slot jobs on
    `scheduled_tasks` (db-scheduler's design, our code).
  - `keyed_lock.rs` `KeyedLockProvider`: in-process and database keyed locks.
  - `json.rs` JSON as Java's Jackson writes it (event payloads, action params).
  - `domain.rs` `Model`, `Entity`, `Persistable`, `ModelEvent`, their bases and builder bases, `Id`.
  - `repository.rs` `Repository`: Java's `AbstractRepository`, with the database selectors.
  - `shard.rs` `ShardIdentifier`, `ShardedId` (Java's `ShardedUUID` bit layout), `ShardingStrategy`, `ShardKey`.
  - `types.rs` `DbUuid`: UUID columns on every database (Java's `UuidStringConverter`).
  - `clock.rs` `Clock`, `SystemClock`, `Instant` (UTC), `truncate_to_micros`.
- `ekbatan-macros/` - `#[model]`, `#[entity]`, `#[model_event]`: what Java's superclasses and
  `@AutoBuilder` give a class (the `base` field, the builder, `copy()`, `next_version()`).
- `ekbatan-schemagen/` - generates the app's `schema.rs` in `build.rs` from a Docker database
  with the migrations applied (Java's jOOQ codegen). Building `wallet-example` needs Docker.
- `ekbatan-test-support/` - what tests of an app use: `VirtualClock`, `ActionSpec`, `ActionAssert`.
- `wallet-example/` - the wallet model, schema, repositories, actions, migrations
  (`migrations/<database>`, Java's files), and the tests on real databases.
- `age-pins/` - no code: exact pins that hold transitive crates to the 2-week rule.
- `docs/LEDGER.md` - what is done, what is next, and decisions taken.
- `docs/DECISIONS-TO-REVIEW.md` - judgement calls waiting for the user.

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
- Event JSON must stay byte-compatible with Java's Jackson: `ekbatan/tests/java_json.rs` checks
  it against output recorded from Java.
- Never print secrets: no password in any `Debug`, log, error or result. `DataSourceConfig`
  redacts its password, and one put in the URL.
- Before calling work done: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test`.
