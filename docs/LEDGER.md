# Ledger

## Done (from the design sketch, tested on PostgreSQL 17, MySQL 8.4, MariaDB 11.8)

- Connections like Java: `DataSourceConfig`, `ConnectionProvider` (sqlx pool, lazy like Hikari's
  `initializationFailTimeout = -1`), `Connection` (held; drop returns it, `evict` closes it),
  `Transaction` (a connection with `BEGIN`; dropped unended, its connection is closed),
  `TransactionManager` (primary + secondary pools per shard), `DatabaseRegistry`.
- `Db` (jOOQ's `DSLContext`): runs sea-query statements on a pool or a held connection, writes
  each database's SQL, sends UUIDs to MySQL/MariaDB as text, and `insert_ignoring_duplicates`
  (`ON CONFLICT DO NOTHING` / `INSERT IGNORE`, which sea-query can't write for MySQL).
- `Repository` (was `ModelRepository`): selectors `db`, `dbs`, `readonly_db`, `readonly_dbs`, `tx_db`,
  `tx_db_else_db` (refuses a transaction on another shard), and Java's methods with Java's SQL:
  `add` / `add_all` (one multi-row INSERT; `RETURNING` on PostgreSQL and MariaDB, the model as
  given on MySQL), `update` (version check; `RETURNING` on PostgreSQL, `next_version` on
  MySQL/MariaDB), `update_all` (ONE statement: `UPDATE ... FROM (VALUES ...)` on PostgreSQL,
  `UPDATE ... JOIN (SELECT ... UNION ALL ...)` on MySQL/MariaDB), the `_no_result` variants,
  `find_by_id`, `get_by_id`, `find_all_by_ids` (per shard; `= ANY(?)` on PostgreSQL),
  `find_all`, `exists_by_id`, `count`, `count_where`, `find_one_where`, `find_all_where`,
  `exists_where`; a private `not_deleted()` like Java's. Batches on two shards are refused
  (`CrossShard`).
- `UnknownShardPolicy` (`UseDefaultShard`, the default, or `Fail`), as in Java and Python;
  `effective_shard`, also used by `tx_db`.
- `ShardedId` in Java's bit layout; `DbUuid`.
- Model layer (item 2): `Model` / `Entity` / `Persistable` / `ModelEvent`, `ModelBase` and
  `EntityBase` (id, state, version, dates; events on models, left out of equality), their
  builders (Java's "x cannot be null" errors, dates cut to microseconds, `updated_date` defaults
  to `created_date`); `Id<M>` and `ShardedId<M>`; `GenericState`. Macros in `ekbatan-macros`:
  `#[model]` / `#[entity]` (adds `base`, writes `<Name>Builder`, `copy()`, `next_version()`,
  an optional `validate = path`), `#[model_event(Model)]` (serde, Java's camelCase names).
  `ShardingStrategy` (`NoSharding`, the default: one shard, `dbs()` = default only;
  `EmbeddedBits`), and `ShardKey` resolves through it. One `Repository` trait serves models and
  entities (Java's `ModelRepository` / `EntityRepository` differ only in the persistable).
- Actions (item 3): `Action` (plan passed in), `ActionPlan` / `PersistableChanges` (one row
  staged once), `ActionRegistry`, `RepositoryRegistry` (type-erased dispatch to each type's
  repository), `ActionExecutor` (fresh plan per attempt, group by shard, refuse cross-shard
  unless allowed, one transaction per shard with the event rows, partial-commit alarm, retries
  by `RetryOn`), `ExecutionConfiguration` / `RetryConfig`, Java's namespace rule. Test support:
  `ActionSpec`, `ActionAssert`. Example: Java's four actions.
- Event log (item 4): `EventPersister`, `SingleTableJsonEventPersister` (one row per event, one
  row with no event when none), `EventEntity`, `EventEntityRepository` on `eventlog.events`;
  `ekbatan::json` writes params and payloads as Jackson does (numbers in Java's forms), checked
  against Java's recorded output; `Id` / `ShardedId` as `{"id":...,"value":...}`.
- Migrations (item 5): `migration::Migrator` (Java's `FlywayMigrator`) on sqlx's migrator:
  Flyway file names, per-shard targets, no-transaction files, `embed_migrations!`. Example:
  Java's migration files per database; tests migrate with it.
- Code generation (item 6): `ekbatan-schemagen` from `build.rs` (Docker, migrations, reflect,
  write `$OUT_DIR/schema.rs`), `ekbatan::include_schema!()`, `ForcedType`. The example's schema
  is generated now.
- Configs (item 7): `ShardingConfig` / `ShardGroupConfig` / `ShardMemberConfig`,
  `EkbatanConfig` (Java's `ekbatan:` section from a JSON file or a parsed tree, `${VAR}`),
  `DataSourceConfig::from_jdbc_url`, `dataSourceProperties` (sqlx's names), Java's checks,
  `DatabaseRegistry::from_sharding_config`, `Migrator::targets`. Open: YAML/TOML reader choice.
- Wallet example: schema (hand-written, as schemagen would write it), repository with custom
  queries, tests on the three databases; connection experiments.

## Plan

Java (`../ekbatan`) is the reference; Python (`../ekbatan-py`) shows how a port settled each
question. One item at a time; each is tested (on PostgreSQL, MySQL and MariaDB when it touches
the database), linted, and committed before the next starts.

| # | Item | Java / Python reference | Status |
|---|---|---|---|
| 1 | Clock: `Clock`, `SystemClock`, `VirtualClock` (frozen / skewed / real time); UTC, microseconds | `ekbatan-test-support` `VirtualClock`; py `clock.py`, `time.py` | done (EKB-0005) |
| 2 | Model layer: `Model` / `Entity`, builders, `ModelEvent`; the `#[model]`, `#[entity]`, `#[model_event]` macros; entities; sharding strategies (`NoShardingStrategy`, embedded bits) | `domain/`, `repository/`, `shard/`; py `domain.py`, `repository.py`, `sharding.py` | done (EKB-0006) |
| 3 | Actions: `Action`, `ActionPlan`, `ActionExecutor` (persist plan and events in one transaction per shard, retries on stale records, cross-shard check), `ActionRegistry`; action test support (`ActionSpec` / `ActionAssert`) | `action/`, `ekbatan-test-support`; py `action/`, `testing.py` | done (EKB-0007) |
| 4 | Events: event log tables, `EventPersister` (single-table JSON), event serialization byte-compatible with Java's | `action/persister/event`, `ekbatan-action-event-json`; py `events/`, `_json.py` | done (EKB-0007) |
| 5 | Migrations: framework tables per database, the app's own migrations | Flyway; py `migrations.py` (yoyo) | done (EKB-0008) |
| 6 | Code generation: schemagen from a test database with the migrations applied | jOOQ codegen; py `schemagen/` | done (EKB-0009) |
| 7 | Configs: config files, data-source properties, sharding config, credentials redaction | `config/`; py `config.py`, `config_files.py` | done (EKB-0011); file format open |
| 8 | Local event handler: listen to our own events | `ekbatan-events/local-event-handler`; py `events/` | |
| 9 | Distributed jobs and keyed locks (locks on a held `Connection`, own pool) | `ekbatan-distributed-jobs`, `concurrent/`; py `distributedjobs/`, `locks/` | |
| 10 | OpenTelemetry tracing and metrics | `TRACER` spans; py `_tracing.py`, `_metrics.py` | |
| 11 | Example apps (`ekbatan-rs-examples`), web framework integration | `ekbatan-examples`; `ekbatan-py-examples` | |
| 12 | Docs, website, GitHub Actions (CI on the three databases, dependency checks, release) | `docs/`, `website/`, `.github/` in both | |

## Differences from Java to keep in mind

- MariaDB UPDATE has no `RETURNING`, so `update` there gives back `next_version()` (Java's
  jOOQ path asks for `RETURNING`).
- `find_all_by_ids` on MariaDB uses `IN (...)` (Java checks only MySQL; jOOQ rewrites the rest).
- No `#[repository]` macro: a repository implements the `Repository` trait by hand
  (`from_record` / `to_record`), as Java's subclasses do; a macro would save little.
- Judgement calls for the user are in `docs/DECISIONS-TO-REVIEW.md`.
