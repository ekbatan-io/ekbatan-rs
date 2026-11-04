# ekbatan-rs

The Rust port of [Ekbatan](https://github.com/ekbatan-io/ekbatan) (Java) and its Python port
`ekbatan-py`. The crate is `ekbatan`; the repository is `ekbatan-rs`.

## Layout

- `ekbatan/` - the framework crate. One folder per topic, as Java's packages; inside a topic,
  the blocking and async kinds sit side by side as `blocking` and `aio` submodules.
  `ekbatan::blocking` and `ekbatan::aio` (`blocking.rs`, `aio.rs`) only gather them.
  - `domain/` `Model`, `Entity`, `ModelEvent`, `Id`, `GenericState`, their bases and builder bases.
  - `shard.rs` `ShardIdentifier`, `ShardedId` (Java's `ShardedUUID` bit layout), `ShardingStrategy`, `ShardKey`.
  - `config/` `DataSourceConfig` (`data_source.rs`), `ShardingConfig` / `EkbatanConfig` (`sharding.rs`).
  - `persistence/` `ConnectionProvider`, `TransactionManager`, `DatabaseRegistry`, `Dialect`,
    `DbUuid`; `blocking/` (`Db`, `Transaction`) and `aio/` (`AsyncDb`, `AsyncTransaction`, the
    `.as_async()` views).
  - `repository/` `AbstractRepository`, `Table`, `TableRecord`, `RepositoryRegistry`, the SQL of
    the ready-made methods (`sql.rs`); `blocking.rs` (`Repository`), `aio.rs` (`AsyncRepository`).
  - `action/` `ActionPlan`, `ActionRegistry`, `ExecutionConfiguration`, the event log
    (`event_persister.rs`); `blocking/` (`Action`, `ActionExecutor`), `aio/` (`AsyncAction`,
    `AsyncActionExecutor`: the engine).
  - `migration.rs` `Migrator`: Java's `FlywayMigrator` on sqlx's migrator (Flyway file names).
  - `keyed_lock/` `lock_key_hash`, `InProcessKeyedLockProvider`, `SqlKeyedLockProvider`;
    `blocking.rs` (`KeyedLockProvider`, `Lease`), `aio.rs` (`AsyncKeyedLockProvider`, `AsyncLease`).
  - `distributed_jobs/` `JobRegistry` (`registry.rs`), `ExecutionContext` (`context.rs`),
    `Schedule`, the `scheduled_tasks` SQL (`store.rs`); `blocking.rs` (`DistributedJob`), `aio.rs`
    (`AsyncDistributedJob`). db-scheduler's design, our code.
  - `local_event_handler/` handlers for our own events: fan-out and handling jobs on
    `eventlog.event_notifications`; `blocking.rs` (`EventHandler`), `aio.rs` (`AsyncEventHandler`).
  - `telemetry.rs` OpenTelemetry spans and metrics under Java's scopes, names and attributes
    (API only; no-op until the app installs an SDK).
  - `json/` JSON as Java's Jackson writes it (event payloads, action params); `ids.rs` for ids.
  - `blocking.rs` gathers the blocking API, with its tokio `runtime()` and `block_on`; `aio.rs`
    gathers the async API.
  - `clock.rs` `Clock`, `SystemClock`, `Instant` (UTC), `truncate_to_micros`; `error.rs` `Error`.
- `ekbatan-macros/` - `#[model]`, `#[entity]`, `#[model_event]`: what Java's superclasses and
  `@AutoBuilder` give a class (the `base` field, the builder, `copy()`, `next_version()`).
- `ekbatan-schemagen/` - generates the app's `schema.rs` in `build.rs` from a Docker database
  with the migrations applied (Java's jOOQ codegen). Building `wallet-example` needs Docker.
- `ekbatan-test-support/` - what tests of an app use: `VirtualClock`, `ActionAssert`,
  `blocking::ActionSpec`, `aio::AsyncActionSpec`.
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
