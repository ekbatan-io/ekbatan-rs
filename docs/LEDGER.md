# Ledger

## Done (from the design sketch, tested on PostgreSQL 17, MySQL 8.4, MariaDB 11.8)

- Connections like Java: `DataSourceConfig`, `ConnectionProvider` (sqlx pool, lazy like Hikari's
  `initializationFailTimeout = -1`), `Connection` (held; drop returns it, `evict` closes it),
  `Transaction` (a connection with `BEGIN`; dropped unended, its connection is closed),
  `TransactionManager` (primary + secondary pools per shard), `DatabaseRegistry`.
- `Db` (jOOQ's `DSLContext`): runs sea-query statements on a pool or a held connection, writes
  each database's SQL, sends UUIDs to MySQL/MariaDB as text, and `insert_ignoring_duplicates`
  (`ON CONFLICT DO NOTHING` / `INSERT IGNORE`, which sea-query can't write for MySQL).
- `ModelRepository`: selectors `db`, `dbs`, `readonly_db`, `readonly_dbs`, `tx_db`,
  `tx_db_else_db` (refuses a transaction on another shard); ready-made `find_by_id`,
  `get_by_id`, `exists_by_id`, `find_all_where`, `count_where` (every shard), `add`, `update`
  (version check); a private `not_deleted()` like Java's; `bulk_update()`.
- `ShardedId` in Java's bit layout; `DbUuid`.
- Wallet example: schema (hand-written, as schemagen would write it), repository with custom
  queries, tests on the three databases; connection experiments.

## Next

1. Model layer: bring the `#[model]` / `#[model_event]` / `#[repository]` macros from the
   earlier sketch onto this crate's `Model` and `ModelRepository` (the sketch's macros targeted
   a stand-in, not this code).
2. Actions: `Action`, `ActionPlan`, `ActionExecutor` (persist plan + events in one
   transaction, retries on `StaleRecord`, single-shard check).
3. Event log tables and persister, compatible with Java's.
4. Schemagen: generate `schema.rs` from a test database with the migrations applied.
5. Keyed locks on a held `Connection`, with their own pool.
6. Pool settings from config files; tracing.

## Open decisions

- Commits in this repository: like ekbatan-py (commit on `main`) or like Java (only when asked)?
- Keep `bulk_update()` as a shared helper, or let custom bulk updates raise `version` themselves?
- Unknown shard: Java defaults to routing to the default shard (`UnknownShardPolicy`); this
  crate currently fails.
