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

## Differences from Java to keep in mind

- MariaDB UPDATE has no `RETURNING`, so `update` there gives back `next_version()` (Java's
  jOOQ path asks for `RETURNING`).
- `find_all_by_ids` on MariaDB uses `IN (...)` (Java checks only MySQL; jOOQ rewrites the rest).
- No sharding strategy yet: every id is a `ShardedId`; Java's `NoShardingStrategy` (one shard,
  `dbs()` = default only) is not ported.
