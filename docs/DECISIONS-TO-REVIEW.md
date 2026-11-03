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
