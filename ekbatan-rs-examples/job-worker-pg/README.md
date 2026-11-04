# job-worker-pg

A worker process with **no HTTP at all**, on Ekbatan's **blocking** API: a plain `fn main`,
blocking actions, a blocking repository, blocking jobs and a blocking event handler. There is no
`async` in this app's code. It is the Rust counterpart of Java's `spring-boot-job-worker-gradle-pg`.

## What it shows

| Piece | Where |
|---|---|
| A scheduled query | `WalletReportJob` in `src/jobs.rs`: counts the wallets and sums their balances, every 5s |
| A job that runs an action | `WalletStipendJob`: tops up every open wallet under 100.00 by 10.00, every 2s, under each wallet's keyed lock |
| Blocking actions | `src/action/`: `perform` is a plain `fn` (`ekbatan::blocking::Action`) |
| Blocking repository query | `WalletRepository::find_open_with_balance_below` in `src/repository.rs` |
| Blocking event handler | `src/handler.rs`: each deposit becomes a `Notification` |
| Wiring | `src/app.rs`; `ekbatan::blocking::block_on` runs the two async-only calls (migrations, starting and stopping the job registry) |

## Once per slot, across every instance

Run three copies of this worker against one database and each job still runs **once per
slot**, not three times. Each job is one row in `scheduled_tasks`; an instance claims a due row
with a conditional update, and only one claim can win. While a job runs, its instance stamps a
heartbeat. If the instance dies, another one takes the row over once the heartbeat is a minute
old.

A slot can still run twice in one case: an instance that stalls (or briefly loses the database)
can be taken over while it is still running. So a job should be harmless to repeat. Here the
stipend re-reads the balances on every run, and the deposit itself is guarded by the keyed lock
and the optimistic lock.

A job runs when its next time has come **and** the scheduler polls. This worker polls every
second (`ekbatan.jobs.pollingInterval` in `ekbatan.json`). With the default of 10 seconds, a
2-second job would run about every 10 seconds.

## Run it

```bash
cd ekbatan-rs-examples/job-worker-pg
docker compose up -d
cargo run -p job-worker-pg
```

The worker logs each run (`RUST_LOG` changes how much). With no wallets nothing gets topped up,
so add one below the threshold:

```bash
docker compose exec postgres psql -U wallet -d wallet -c \
  "insert into wallets values (gen_random_uuid(), 1, 'OPENED', gen_random_uuid(), 'USD', 0, now(), now())"
```

Within a few seconds the stipend deposits 10.00, the report counts it, and the handler records
a notification for each deposit. Ctrl-C (or SIGTERM) stops it: no new slot starts, running jobs
finish, then the pools close.

## Test

```bash
cargo test -p job-worker-pg
```

Plain blocking code against a PostgreSQL in Docker. It opens one wallet at 0 and one at 500,
then checks that the stipend tops up the first only and that each top-up leaves a notification.
It also checks that `stop()` returns promptly.
