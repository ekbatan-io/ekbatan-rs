# Decisions to review

Judgement calls made while porting, for the user to confirm or overturn. Newest last.

## Items 3-4: actions and the event log (2026-11-03)

1. **The plan is a parameter.** Java binds `ActionPlan` in a `ScopedValue` and actions call
   `plan()`. Rust has no ScopedValue, so `perform(&self, plan: &mut ActionPlan, principal,
   params)`. Same for test support: `ActionSpec` hands a fresh plan in.
2. **Principal is `Option<&str>`** (Java's `Principal`, used only for `getName()`; Python uses
   `str | None` too).
3. **Errors and retry keys.** Java retries by exact exception class over the cause chain. Here
   a failure is `ekbatan::Error`; an app's own error goes in `Error::Action(Box<dyn Error>)`
   (`Error::action(e)`). `RetryOn::stale_record()` covers `StaleRecord` and `StaleRecords`
   (Java has one `StaleRecordException`); `RetryOn::error::<E>()` matches an `E` in the
   `source()` chain, outermost first, as Java walks causes.
4. **An event doesn't hold `modelId` / `modelName`.** Java's event constructor takes them; here
   the event log takes them from the model the event is attached to, and writes them first in
   the payload, as Java's JSON has them. Byte-for-byte the same payload; a Rust event struct is
   just its own fields.
5. **Staging the same row twice** returns `Error::Unexpected` with Java's message (Java throws
   `IllegalStateException`). `plan.add/update` return `Result`.
6. **Params are serialized once per attempt, before the transactions**, and refused if not a
   JSON object; Java checks inside each shard's transaction. Same outcome, earlier.
7. **JSON text matches Jackson's** for what was measured against Java's recorded output
   (`ekbatan/tests/fixtures`, from ekbatan-py's probe): 6000 doubles, 2008 decimals, 3000
   instants, and the probe event, byte for byte. Known gaps, left as they are:
   - `NaiveDateTime` (Java's `LocalDateTime`): chrono writes `.600`, Jackson `.6`. Use
     `Instant` (UTC everywhere).
   - Maps keep insertion order only if the app uses an ordered map (`IndexMap`); `HashMap`
     order is random, `BTreeMap` sorted.
   - Bytes: serde writes an array of numbers, Jackson base64.
   - Decimals with more than 28 digits or a negative scale (`1E+3`) can't be held by
     `rust_decimal`.
   - MySQL's JSON type stores numbers as doubles (`10.00` comes back `10.0`) - for Java's rows
     too; PostgreSQL's `jsonb` and MariaDB keep the digits.
8. **Decimal as a JSON number** needs rust_decimal's `serde-float` + `serde-arbitrary-precision`
   (exact, not a float), which turns on serde_json's `arbitrary_precision` for the whole build.
9. **Logging uses `tracing`** (already in the build through sqlx), at Java's levels and words.

## Item 5: migrations (2026-11-03)

1. **sqlx's migrator does the work** (agreed with the user), behind Java's `FlywayMigrator`
   shape: `Migrator::builder().with_targets(..).locations(..).migrate()`, one target per shard,
   stop at the first failure and log which targets were already migrated.
2. **Java's file names** (`V0001__eventlog.sql`), so Java's migration files copy over as they
   are. Only whole-number versions (sqlx keeps one integer): `V1_1__` / `V1.1__` are refused,
   as are repeatable (`R__`) and undo (`U`) files.
3. **History table is sqlx's `_sqlx_migrations`**, not Flyway's `flyway_schema_history`: a
   database migrated by Java's Flyway is not seen as migrated here, and the other way round.
4. **Outside a transaction:** sqlx's first-line marker `-- no-transaction`, and Flyway's
   `V0005__x.sql.conf` with `executeInTransaction=false`; both work.
5. **`embed_migrations!("migrations/postgres")`** builds the files into the binary (with their
   `.sql.conf`), so an app ships without a migrations folder. The default location, as Java's
   `classpath:db/migration`, is the `migrations` folder.
6. **Tests now run one server per shard for MySQL/MariaDB**: their `eventlog` is a database per
   server, so two shards in one server would both try to create `eventlog.events` (Java's MySQL
   examples are unsharded). PostgreSQL keeps one server with a database per shard.

## Item 6: code generation (2026-11-03)

1. **Runs in the app's `build.rs`** (`ekbatan-schemagen`), as Java's Gradle build runs jOOQ's
   codegen: a Testcontainers database, ekbatan's `Migrator` into it, `information_schema`
   read, `$OUT_DIR/schema.rs` written, pulled in with `ekbatan::include_schema!()`. Not
   committed; regenerated when a migration folder changes (cargo `rerun-if-changed`), so
   **building the example needs Docker**, as Java's does. An IDE that runs build scripts
   (rust-analyzer) will start a container when the migrations change.
2. **Type rules follow Java's builds**: a MySQL/MariaDB `CHAR(36)` is a UUID (`DbUuid`) only on
   `id` and `*_id` columns (Java's forced type); MariaDB's `UUID` and PostgreSQL's `uuid` are
   always. Timestamps stay `NaiveDateTime` in the record (Java's records use `Instant`); the
   repository converts, as before. Project rules: `ForcedType` (regex on `table.column` and on
   the database type, both matched whole), as jOOQ's.
3. **JSON columns are refused for now** (no Rust type mapped; a `ForcedType` can name one).
   Java maps them to a Jackson tree.
4. **Left out**: `_sqlx_migrations`, `scheduled_tasks`, `flyway_schema_history`, and anything
   outside the default schema (so `eventlog.*`). `Table` / `TableRecord` are written only for
   tables with `id`, `version` and `state`; others get the column enum and the row struct.
5. **Proof**: the example's migrations give byte-identical modules from PostgreSQL, MySQL and
   MariaDB (`ekbatan-schemagen/tests/generate.rs`), equal to the schema that was hand-written
   before. The example generates from PostgreSQL.
6. The schemagen test starts the three databases one after another: started together on one
   runtime, MySQL's container timed out twice (alone it is ready in about 5s).

## Immutable models and entities (2026-11-03, asked by the user)

1. `#[model]` / `#[entity]` make the struct's own fields private and add a getter per field,
   with the field's declared visibility, returning a reference: `wallet.balance()` is a
   `&Decimal`. A value changes only through `copy()...build()`, as Java's `final` fields.
2. Inside the model's own module the fields are still reachable (Rust's privacy is per
   module); the module's methods take `&self`, so they can't change them either.
3. Proven by `compile_fail` doc tests on `Wallet` (assigning a field, reaching `base`) next to
   a positive one that differs only in that line.

## Item 7: configs (2026-11-03)

1. **Java's layout and keys** for the `ekbatan:` section (`namespace`, `sharding` with groups,
   members and named `configs`, `jdbcUrl`, `maximumPoolSize`, ...), kebab-case, snake_case or
   camelCase, with Spring's `${VAR}` / `${VAR:default}` / `$${`. Java's sharded example config
   reads as is (`ekbatan/tests/config.rs`).
2. **Which file formats: open, for the user.** No YAML or TOML reader is in the build, and
   picking one is a new dependency. Today: a JSON file (`EkbatanConfig::load_json`), or any
   format the app parses into a `serde_json::Value` (`EkbatanConfig::from_tree`). Java's files are
   YAML; candidates: `serde_yaml` is archived; `serde_yaml_ng`, `serde_norway` and `yaml-rust2`
   are maintained forks/successors; `toml` is the standard TOML reader.
3. **`jdbcUrl` stays Java's** (`jdbc:postgresql://`, `jdbc:mysql://`, `jdbc:mariadb://`; sqlx's
   `postgres://` / `mysql://` too); the dialect comes from it, its query parameters become
   `dataSourceProperties`, and a login in it is refused, as Java's `JdbcUrlCredentials`.
4. **`dataSourceProperties` are sqlx's names**, not the JDBC drivers': only names sqlx reads
   are accepted (`POSTGRES_PROPERTIES`, `MYSQL_PROPERTIES`), anything else refused by name.
   sqlx's PostgreSQL driver logs the value of a URL parameter it doesn't know, and Java treats
   every value as a secret. So Java's `ApplicationName` must be written `application_name`.
5. **Accepted and unused**: `driverClassName` (sqlx picks the driver from the URL) and
   `leakDetectionThreshold` (checked as Java checks it; sqlx has no leak detection), so Java's
   config files load unchanged.
6. **Checks added beyond Java's**: the default shard must be a configured one; a group or a
   member twice is refused. `DataSourceConfig::validate` now runs when a pool is built.
7. `jobs` and `local-event-handler` are kept as read; items 8 and 9 read them.

## Item 9a: distributed jobs (2026-11-03)

1. **Our own scheduler, db-scheduler's way** (the user chose this over apalis; no Rust crate runs
   on PostgreSQL, MySQL *and* MariaDB with a once-per-slot claim). Kept db-scheduler's
   `scheduled_tasks` layout because it is proven and Java's example migrations have it - not to
   share it with Java (no sharing with Java is ever wanted).
2. **How it works**: one row per job; a claim is one `UPDATE ... WHERE version = ?`; heartbeats
   every 10s (Java's `EKBATAN_HEARTBEAT_INTERVAL`); dead after 6 missed (60s), then revived to
   run now; completion recorded only if the row is still the one this instance claimed.
3. **Schedules**: fixed delay, daily (UTC times), disabled. No cron (as the Python port).
4. **Proof**: two instances on one database, a 1s job for 8s: 8 runs, no slot twice, on all
   three databases (`wallet-example/tests/distributed_jobs.rs`), plus failure counting, dead
   revival, takeover (stale result not recorded, `fraction_dead` > 0.5), shutdown waiting for a
   running job, a missing table failing `start()`.
5. **Shutdown waits once** for `shutdown_max_wait` (Java's db-scheduler waits up to twice), then
   aborts the execution; its row is revived later, as after a crash.
6. A recurring job's row keeps its stored next time when its schedule changes between deploys
   (db-scheduler can reschedule on change; not done here).

## Item 9b: keyed locks (2026-11-03)

1. Java's `KeyedLockProvider` with leases and a `max_hold` watchdog: in-process (tokio mutex
   per key), PostgreSQL (`pg_advisory_lock` / `pg_try_advisory_lock`, `SET lock_timeout` for a
   bounded wait), MySQL and MariaDB (`GET_LOCK` / `RELEASE_LOCK`). Keys hashed as Java's
   `LockKeyHash` (first 8 bytes of SHA-256; hex for MySQL lock names). One
   `SqlKeyedLockProvider`, with Java's three names as aliases.
2. **Not re-entrant.** Java's leases are re-entrant per thread; an async task has no thread of
   its own. Taking a key the same task holds waits like any other caller.
3. **Cancellation**: an acquire dropped mid-wait closes its connection (rather than returning it
   to the pool), so a lock the database grants late dies with the session.
4. A lease dropped without `release()` is released in the background.
5. No Redis provider (Java has `ekbatan-keyed-lock-redis`): would be a new dependency; ask.

## Item 8: local event handler (2026-11-03)

1. Java's design as is: `EventFanoutJob` (undelivered events -> one `event_notifications` row
   per handler, events marked delivered, one transaction per shard) and `EventHandlingJob`
   (handlers run on due rows; backoff 30s doubling, capped at 5 min; retention window 7 days,
   expiring before a run or after a failure), both distributed jobs looping until the
   registry stops. Java's defaults for poll delays and batch sizes.
2. **A handler names its event type in Rust's type system** (`type Event = ...`) instead of
   Java's `eventType()` returning a class; the payload is read with serde, so the payload's
   `modelId` / `modelName` keys are ignored by the event struct (they are on the envelope).
3. **`ExecutionContext` gained `is_shutting_down()` and `sleep()`** (Java's
   `getSchedulerState().isShuttingDown()`), so looping jobs end when the registry stops.
4. A handler error is logged and retried; a handler that panics is not caught (Java catches
   `Throwable`). Kept so a panic stays loud; the job's loop ends, and the scheduler runs it again
   on its next slot.
5. No metrics yet (Java counts fan-out and handling in OpenTelemetry): item 10.
6. `LocalEventHandlerConfig` reads `ekbatan.local-event-handler` (Java's keys; ISO-8601
   durations); `handling.enabled` is off unless set, as in Java. Wiring the jobs into a
   `JobRegistry` is the app's (Java's DI does it); the example shows it.

## Blocking and async APIs side by side (2026-11-04)

The user chose to wrap the async code (not a second, synchronous database driver), then
Python's naming, then explicit folders for both sides: `blocking` and `aio`.

1. **Three places.** `ekbatan::blocking` (folder `ekbatan/src/blocking/`) holds the blocking
   API under plain names: `Action`, `ActionExecutor`, `Repository`, `Db`, `Dbs`, `Transaction`,
   `KeyedLockProvider`, `Lease`, `DistributedJob`, `EventHandler`, plus `runtime()` and
   `block_on`. `ekbatan::aio` (`ekbatan/src/aio/`) holds the `Async*` twins. The topic modules
   (`action/`, `repository.rs`, `keyed_lock.rs`, ...) hold only what both share: `ActionPlan`,
   the registries, `AbstractRepository`, config, the lock providers, the job scheduler. Test
   support: `ekbatan_test_support::blocking::ActionSpec` and `aio::AsyncActionSpec`.
2. **Why `blocking`, not `sync`**: Rust's usual word (`reqwest::blocking`, tokio's
   `spawn_blocking`); `sync` reads as `std::sync` (locks, `Arc`) and the `Sync` marker. `aio`
   is Python's name and redis-rs's. (`ekbatan::async` is impossible: a reserved word.)
3. **Names inside `blocking` are plain, `aio` ones carry `Async`**, as in Python, where the sync
   names are unprefixed.
4. **`AbstractRepository` (Java's name) is what a repository implements**: types, `registry`,
   `from_record`, `to_record`, `sharding_strategy`. `blocking::Repository` and
   `aio::AsyncRepository` are given to every `AbstractRepository`, so one repository serves
   both. A module that imports both method traits must say which one it means.
5. **Shared types with methods for both sides**: `DatabaseRegistry` (`primary`, `secondary`,
   `primaries`, `secondaries`, `close`) and `TransactionManager` (`begin`, `in_transaction`)
   have blocking methods (written in `blocking/`); `.as_async()` gives `AsyncDatabaseRegistry`
   / `AsyncTransactionManager` (Python's names) with the `.await` versions.
6. **One copy of every rule.** The async code is the engine; the blocking API waits on it. A
   blocking `Action`, `DistributedJob` or `EventHandler` *is* an async one too (blanket impls),
   so it registers in the same registries and runs in the same executor and jobs. The blocking
   `ActionExecutor` runs async actions too.
7. **Its own tokio runtime** (`blocking::runtime()`, multi-threaded, made on first use). A pool
   made outside any runtime is put on it. Blocking code inside an action, job or handler steps
   off the runtime thread (`block_in_place`) before waiting.
8. **Not for async code**: called from inside an async function the blocking API blocks that
   thread, and on a single-threaded runtime it panics, as `reqwest::blocking` does. Documented,
   not guarded.
9. **Still async only, run with `blocking::block_on(...)` from blocking code**: migrations
   (`Migrator::migrate`), `JobRegistry::start` / `stop`, `ConnectionProvider::acquire` /
   `close`, the event log and notification repositories' functions, and the event jobs'
   `drain_one_round`. Not the request path (Python's `aio` draws the same line). Say if you want
   blocking twins for any.
10. **The wallet example stays async** (`AsyncAction`, `AsyncRepository`), as it was written;
    `wallet-example/tests/blocking.rs` shows the blocking API end to end.
11. Always compiled; no cargo feature (it adds no dependency).
