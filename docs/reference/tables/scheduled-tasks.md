# scheduled_tasks

Optional. One row per distributed job: the slot, the claim, the heartbeat. Needed only if you use distributed jobs - or the local event handler, whose fan-out and handling are jobs too.

**Needed when:** you use [distributed jobs](/reference/jobs/distributed-jobs/), or the
[local event handler](/reference/events/local-event-handler/).
**Written by:** the `JobRegistry`, on the pool you give it.
**Created by:** your migration (`V0002__scheduled_tasks.sql` in the examples).
**Where:** `scheduled_tasks`, in your own schema or database, beside your tables. The name is
fixed.

The table's layout is **[db-scheduler](https://github.com/kagkarlsson/db-scheduler)'s**, the
scheduling library Ekbatan for Java runs its jobs on. This framework has its own scheduler, which
works the way db-scheduler does, on db-scheduler's table, unchanged. So copy db-scheduler's
schema for your database as it is.

## DDL

### PostgreSQL

```sql
create table scheduled_tasks (
    task_name text not null,
    task_instance text not null,
    task_data bytea,
    execution_time timestamp with time zone not null,
    picked BOOLEAN not null,
    picked_by text,
    last_success timestamp with time zone,
    last_failure timestamp with time zone,
    consecutive_failures INT,
    last_heartbeat timestamp with time zone,
    version BIGINT not null,
    priority SMALLINT,
    PRIMARY KEY (task_name, task_instance)
);

CREATE INDEX execution_time_idx ON scheduled_tasks (execution_time);
CREATE INDEX last_heartbeat_idx ON scheduled_tasks (last_heartbeat);
CREATE INDEX priority_execution_time_idx ON scheduled_tasks (priority desc, execution_time asc);
```

**On PostgreSQL the time columns must be `timestamp with time zone`**, as db-scheduler has them.
The scheduler reads them as UTC moments, and its driver reads a moment only from that type. This
is the one framework table that steps off the [zone-less rule](/reference/multi-database/#always-utc):
its layout is db-scheduler's, not the framework's.

### MySQL and MariaDB

```sql
create table scheduled_tasks (
    task_name VARCHAR(100) NOT NULL,
    task_instance VARCHAR(100) NOT NULL,
    task_data BLOB,
    execution_time TIMESTAMP(6) NOT NULL,
    picked BOOLEAN NOT NULL,
    picked_by VARCHAR(50),
    last_success TIMESTAMP(6) NULL,
    last_failure TIMESTAMP(6) NULL,
    consecutive_failures INT,
    last_heartbeat TIMESTAMP(6) NULL,
    version BIGINT NOT NULL,
    priority SMALLINT,
    PRIMARY KEY (task_name, task_instance),
    INDEX execution_time_idx (execution_time),
    INDEX last_heartbeat_idx (last_heartbeat),
    INDEX priority_execution_time_idx (priority, execution_time)
);
```

`TIMESTAMP(6)` here, db-scheduler's choice. A MySQL `TIMESTAMP` is stored in UTC and shown in
the session's zone; sqlx sets each session to UTC (`+00:00`), and the scheduler counts on it. So
leave the `timezone` driver setting alone - see [Connecting](/reference/connecting/#driver-settings).
The `(6)` matters: the times compared are not whole seconds.

## Columns

| column | null | meaning |
| --- | :---: | --- |
| `task_name` | no | which job: its `name()` |
| `task_instance` | no | always `recurring`: every job here is a recurring one, with exactly one row |
| `task_data` | yes | not used; left null |
| `execution_time` | no | when the job is next due |
| `picked` | no | whether an instance has claimed it |
| `picked_by` | yes | which one: a name made per job registry, `ekbatan-` and 12 hex digits |
| `last_success`, `last_failure` | yes | when the last run ended, by outcome |
| `consecutive_failures` | yes | how many runs in a row have failed; back to 0 after a success. **The column to alert on** |
| `last_heartbeat` | yes | **the sign of life.** A claimed row whose heartbeat is too old is declared dead and made due again |
| `version` | no | raised by every claim, completion and revival. Every update checks it, heartbeats included, so of two instances acting on one row, exactly one wins |
| `priority` | yes | not used; left null |

`task_name` plus `task_instance` is the primary key, and that is what runs each job on only one
instance at a time across the cluster: a job is one row, so only one instance can hold it.

## What happens to a row

1. **`start()` writes it**, if there is none: due now for a fixed-delay job, at the next time of
   day for a daily one. A row already there keeps its time and state, so a restart does not reset
   the schedule - and a schedule changed between deploys takes effect only after the next run.
   A job whose schedule is `Disabled` writes no row.
2. **An instance claims it** when it is due: `picked = true`, `picked_by`, `last_heartbeat`, and
   `version + 1`, all in one `UPDATE ... WHERE version = <what it read>`. Only one instance's
   update matches.
3. **While the job runs**, its instance stamps `last_heartbeat` every heartbeat interval (10
   seconds by default).
4. **When the job returns**, the row is let go: `picked = false`, the next `execution_time` from
   the schedule, `last_success` or `last_failure`, and `consecutive_failures`. Only if the row is
   still at the version this instance wrote; otherwise the result is not recorded.
5. **A claimed row whose heartbeat is older than** heartbeat interval x 6 (60 seconds by default)
   is dead: an instance that polls makes it due now, and the next poll claims it.

A job removed from the code leaves its row behind. Delete it by hand if you want it gone.

## The heartbeat is why the jobs pool is separate

`last_heartbeat` is written by its own loop, over and over, for every job this instance is
running. If that loop cannot get a connection, the heartbeat stops, and the job is declared dead
and **run again somewhere else** - while it is still running here.

So give the job registry a pool of its own (`jobsConfig` in the examples), not the request pool,
and big enough for the polling, the heartbeats and whatever your jobs do on it.

## See also

- [Distributed jobs](/reference/jobs/distributed-jobs/) - the registry that writes this table
- [Framework tables](/reference/tables/) - the map
