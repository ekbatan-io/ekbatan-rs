# events

The outbox: event rows written in the same transaction as each action. Domain rows and their event rows commit together, so this table is the reliable record that the local event handler, a change-data-capture tool such as Debezium, or a broker publisher reads after the commit.

**Needed:** always - every action writes to it.
**Written by:** the executor's default event writer, `SingleTableJsonEventPersister`, inside the
action's transaction.
**Created by:** your migration (`V0001__eventlog.sql` in the examples).
**Where:** `eventlog.events`. On PostgreSQL `eventlog` is a schema in each database; on MySQL and
MariaDB it is a database of its own, one per server. The place is fixed: the framework names the
table itself - see [Framework tables](/reference/tables/).

It is not a model or an entity: it has no version and no `state`. Nothing changes a committed
row except its `delivered` flag.

## DDL

As in the examples' migrations, with their two indexes. The indexes are yours to choose - see
[Indexes](#indexes).

### PostgreSQL

```sql
CREATE SCHEMA IF NOT EXISTS eventlog;

CREATE TABLE eventlog.events (
    id              UUID         PRIMARY KEY,
    namespace       VARCHAR(255) NOT NULL,
    action_id       UUID         NOT NULL,
    action_name     VARCHAR(255) NOT NULL,
    action_params   JSONB        NOT NULL,
    started_date    TIMESTAMP    NOT NULL,
    completion_date TIMESTAMP    NOT NULL,
    model_id        VARCHAR(255),
    model_type      VARCHAR(255),
    event_type      VARCHAR(255),
    payload         JSONB,
    event_date      TIMESTAMP    NOT NULL,
    delivered       BOOLEAN      NOT NULL
);

CREATE INDEX idx_events_action_id ON eventlog.events(action_id);

CREATE INDEX events_undelivered
    ON eventlog.events (event_type, event_date)
    WHERE delivered = FALSE;
```

### MySQL

```sql
-- V0000__create_eventlog_database.sql
CREATE DATABASE IF NOT EXISTS eventlog;

-- V0001__eventlog.sql
CREATE TABLE eventlog.events (
    id              CHAR(36)     CHARACTER SET ascii NOT NULL,
    namespace       VARCHAR(255) NOT NULL,
    action_id       CHAR(36)     CHARACTER SET ascii NOT NULL,
    action_name     VARCHAR(255) NOT NULL,
    action_params   JSON         NOT NULL,
    started_date    DATETIME(6)  NOT NULL,
    completion_date DATETIME(6)  NOT NULL,
    model_id        VARCHAR(255),
    model_type      VARCHAR(255),
    event_type      VARCHAR(255),
    payload         JSON,
    event_date      DATETIME(6)  NOT NULL,
    delivered       BOOLEAN      NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX idx_events_action_id ON eventlog.events(action_id);

CREATE INDEX events_pending_fanout ON eventlog.events (delivered, event_type, event_date);
```

### MariaDB

The same as MySQL, with MariaDB's own `UUID` type for the two ids:

```sql
CREATE TABLE eventlog.events (
    id              UUID         PRIMARY KEY,
    namespace       VARCHAR(255) NOT NULL,
    action_id       UUID         NOT NULL,
    -- ... the other columns as in MySQL ...
    delivered       BOOLEAN      NOT NULL
);
```

`CHAR(36) CHARACTER SET ascii`, as on MySQL, works there too.

`DATETIME(6)`, not a bare `DATETIME`: the bare form silently cuts times to whole seconds, and
`event_date` is the table's only ordering key.

## Columns

| column | null | meaning |
| --- | :---: | --- |
| `id` | no | this row: a new random uuid for each row |
| `namespace` | no | the service that wrote it: the executor's `namespace`, dot-separated names |
| `action_id` | no | one uuid per execution. Every row one action wrote shares it, on every shard it wrote to |
| `action_name` | no | the action type's name, e.g. `WalletDepositMoneyAction` |
| `action_params` | no | the action's params, as JSON. A reader can understand the command without looking at your tables |
| `started_date` | no | when `perform()` began, UTC |
| `completion_date` | no | when the action's changes were written, UTC |
| `model_id` | yes | the model's id, as text. Null on an action row (below) |
| `model_type` | yes | the model type's name, e.g. `Wallet`. Null on an action row |
| `event_type` | yes | the event type's name, e.g. `WalletMoneyDepositedEvent`. Null on an action row |
| `payload` | yes | the event as JSON. Null on an action row |
| `event_date` | no | when the event was produced, UTC: the same as `completion_date` for every row of one action. **The only ordering key** |
| `delivered` | no | `false` on every insert; the fan-out job sets it to `true`. A change-data-capture reader can ignore it |

**Type names are short.** `event_type` and `model_type` are the type's name without its module
path: `WalletMoneyDepositedEvent`, not `wallet::model::WalletMoneyDepositedEvent`. Two event types
with the same short name in one service are refused when the second one is written, because a
reader routes on that name alone.

**The payload carries the model.** The event's own fields are written in camelCase, after two
the framework adds - `modelId` and `modelName`:

```json
{"modelId":"0190c7a2-...","modelName":"Wallet","amount":25.00,"newBalance":125.00}
```

These are the same columns, with the same JSON, as Ekbatan for Java's `events` table, so the same
change-data-capture setup can read either.

## Action rows

An action that emits no events still writes **one row**, with `model_id`, `model_type`,
`event_type` and `payload` all null. The action happened, and that is worth keeping. A reader
skips them with `event_type IS NULL`. In a cross-shard action, each shard that had changes but no
events gets its own.

## Two things about this table

**`action_params` is never null.** Every row carries the whole input of the action that produced
it, so one row is a complete record without a join.

**`delivered` is the only column that changes, and it belongs to the reader.** If something else
sets it, the fan-out job never sees those events.

## Ordering

`event_date` is the only ordering key; there is no sequence number. With a real clock that is
fine. With a **frozen** clock, as in tests, every row of a run has the same time. So in tests,
look rows up by `action_name` or `action_id`, not by position.

## Indexes

The examples create two: one on `action_id`, to find every row of one action, and one for the
rows not yet delivered, which the fan-out job reads (`delivered = FALSE AND event_type IN (...)`,
oldest `event_date` first).

On PostgreSQL that second index is **partial** - it holds only the rows still waiting:

```sql
CREATE INDEX events_undelivered ON eventlog.events (event_type, event_date)
    WHERE delivered = FALSE;
```

MySQL and MariaDB have no partial indexes, so their migrations put the flag first in a composite
index:

```sql
CREATE INDEX events_pending_fanout ON eventlog.events (delivered, event_type, event_date);
```

The composite one holds every row ever written; the partial one only the backlog, so it stays
small however large the table grows. Every insert pays to keep the index up to date.

**Nothing deletes old rows.** The table grows with every action. Removing delivered rows you no
longer need is up to you.

## See also

- [The outbox](/learn/outbox/) - why this table exists
- [`event_notifications`](/reference/tables/event-notifications/) - what fan-out makes from these
  rows
- [Model event](/reference/model-event/) - the event type, and how it becomes a row
- [Framework tables](/reference/tables/) - the map
