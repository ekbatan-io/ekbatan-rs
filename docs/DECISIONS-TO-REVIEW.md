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
