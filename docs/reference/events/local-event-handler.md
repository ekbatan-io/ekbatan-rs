# Local event handler

Handling your own events in your own service: the fan-out and handling jobs. Events are delivered to handlers inside your application, with no broker; delivery survives restarts, retries on failure, and works across several instances.

This path suits a service that does not need to send its events to other services: a small
monolith, an internal tool, anywhere a Kafka cluster would be too much. Two background jobs read
the outbox and do the delivery:

- **Fan-out** (`EventFanoutJob`) turns each new event into one *notification* row per handler that
  subscribes to it, in [`event_notifications`](/reference/tables/event-notifications/).
- **Handling** (`EventHandlingJob`) picks up the notifications that are due and calls their
  handlers, retrying with a growing wait when a handler fails.

```
Your service
   │  one transaction per action
   ▼
 ┌──────────────────────────────────────────────┐
 │  wallets, notifications, …  +  events        │  committed together,
 └──────────────────────────────────────────────┘  delivered = FALSE
   │
   ▼
 ┌──────────────────────────────────────────────┐
 │  EventFanoutJob                              │  reads undelivered events,
 │                                              │  writes one notification per
 │                                              │  (event x subscribed handler),
 │                                              │  marks the events delivered
 └──────────────────────────────────────────────┘
   │
   ▼
 ┌──────────────────────────────────────────────┐
 │  event_notifications                         │  state = PENDING
 └──────────────────────────────────────────────┘
   │
   ▼
 ┌──────────────────────────────────────────────┐
 │  EventHandlingJob                            │  reads due notifications,
 │                                              │  calls the typed handler,
 │                                              │  SUCCEEDED / FAILED / EXPIRED
 └──────────────────────────────────────────────┘
   │
   ▼
 your handlers
```

Both are ordinary [distributed jobs](/reference/jobs/distributed-jobs/) on your `JobRegistry`, so
they get the cluster-wide claim, the heartbeat and the crash recovery. Normally one instance runs
fan-out and one runs handling. That is not a promise - a stalled instance can be taken over while
it still runs - so both are safe to repeat.

## Defining a handler

Blocking, from the job worker example:

```rust
use ekbatan::blocking::{ActionExecutor, EventHandler};
use ekbatan::local_event_handler::EventEnvelope;
use ekbatan::Error;

pub struct WalletMoneyDepositedEventHandler {
    executor: Arc<ActionExecutor>,
}

impl EventHandler for WalletMoneyDepositedEventHandler {
    type Event = WalletMoneyDepositedEvent;

    fn name(&self) -> String {
        "wallet-money-deposited-notification".to_owned()
    }

    fn handle(&self, envelope: EventEnvelope<WalletMoneyDepositedEvent>) -> Result<(), Error> {
        let event = &envelope.event; // the typed event
        // ...
        Ok(())
    }
}
```

Async, the same with `ekbatan::aio::AsyncEventHandler` and an `async fn handle`.

- `type Event` is the event type it subscribes to: a `#[model_event]` type, read from the row's
  JSON with serde.
- `name()` is the handler's id. It is written on every notification row, so treat it as
  permanent; [Renaming a handler](#renaming-a-handler) shows how to change it.
- `handle(envelope)` does the work. `Ok(())` is a success; an `Err` is a failure, retried later.
- `aliases()` is optional: empty unless you override it.

A blocking `EventHandler` is also an `AsyncEventHandler`, so one registry holds both kinds.

**The envelope** carries the event and where it came from:

| field | |
| --- | --- |
| `event` | the typed event |
| `event_id` | the `events` row's id: the same on every retry of this delivery. A natural idempotency key |
| `namespace` | the service that wrote it |
| `action_id`, `action_name` | the action that produced it |
| `action_params` | that action's params, as JSON text |
| `started_date`, `completion_date`, `event_date` | when, in UTC |
| `model_id`, `model_type` | the model it belongs to, as text |

Handlers are registered by hand, where you can read them:

```rust
use ekbatan::local_event_handler::EventHandlerRegistry;

let handlers = EventHandlerRegistry::builder()
    .with_handler(WalletMoneyDepositedEventHandler::new(executor.clone()))
    .build()?;
```

`build()` refuses a blank name or alias, and a name or alias used twice - so the same handler
registered twice is refused too. Several handlers may subscribe to one event type; each gets its
own notification row and its own retries.

Subscriptions are by the event type's **short name**, which is what the outbox stores in
`events.event_type` (`WalletMoneyDepositedEvent`).

A handler runs **after** the action that emitted its event has committed: later, in the handling
job, never inside the action.

## Wiring

Three registries meet here: your `DatabaseRegistry`, the `EventHandlerRegistry`, and the
`JobRegistry` that runs the jobs. From the examples:

```rust
use ekbatan::local_event_handler::{
    EventFanoutJob, EventHandlerRegistry, EventHandlingJob, LocalEventHandlerConfig,
};

let handler_config = match &config.local_event_handler {
    Some(section) => LocalEventHandlerConfig::from_value(section)?,
    None => LocalEventHandlerConfig::default(),
};
let mut jobs = JobRegistry::builder()
    .connection_provider(pool_for("jobsConfig")?)
    .jobs_config(&jobs_config);
if handler_config.handling_enabled {
    let handlers = || {
        EventHandlerRegistry::builder()
            .with_handler(WalletMoneyDepositedEventHandler::new(executor.clone()))
            .build()
    };
    jobs = jobs
        .with_job(handler_config.fanout_job(EventFanoutJob::new(registry.clone(), handlers()?)))
        .with_job(handler_config.handling_job(EventHandlingJob::new(registry.clone(), handlers()?)));
}
let jobs = jobs.build()?;
jobs.start().await?;
```

Each job takes its own `EventHandlerRegistry`, hence the closure that builds one for each. The
executor already writes every event with `delivered = false`, so fan-out sees it with nothing
else to set.

**The jobs do not read `handling.enabled` themselves.** It is a switch for your wiring, as above.
A job you add to the registry runs. Some deployments want fan-out in one process and handling in
a separate worker: add only the job each process should run.

The jobs' own builder methods, if you do not go through the config:

| `EventFanoutJob` | `EventHandlingJob` |
| --- | --- |
| `.poll_delay(d)` (1s) | `.poll_delay(d)` (1s) |
| `.batch_size(n)` (200) | `.batch_size(n)` (100) |
| `.name("...")` (`ekbatan-event-fanout`) | `.name("...")` (`ekbatan-event-handling`) |
| `.clock(c)` | `.max_backoff_cap(d)` (5 minutes) |
| | `.retention_window(d)` (7 days) |
| | `.clock(c)` |

The name is the job's row in `scheduled_tasks`.

## Configuration

`ekbatan.local-event-handler` in the config file, read by `LocalEventHandlerConfig::from_value`;
`fanout_job(...)` and `handling_job(...)` apply what is set:

```json
"local-event-handler": {
  "fanoutPollDelay": "PT1S",
  "fanoutBatchSize": 200,
  "handlingPollDelay": "PT1S",
  "handlingBatchSize": 100,
  "handlingMaxBackoffCap": "PT5M",
  "handlingRetentionWindow": "P7D",
  "handling": { "enabled": true }
}
```

Durations are ISO-8601 text and must be positive; batch sizes are whole numbers above 0. A key
that means nothing is refused. See [Config files](/reference/config-files/#local-event-handler).

## Schema

This path needs [`event_notifications`](/reference/tables/event-notifications/) and
[`scheduled_tasks`](/reference/tables/scheduled-tasks/) beside
[`events`](/reference/tables/events/), and an index on `events` for the fan-out read - partial on
PostgreSQL, composite on MySQL and MariaDB. The examples' migrations have all of it.

## Fan-out

Each round, on **every shard at the same time**, each in one transaction on its primary:

1. Read up to `batch_size` undelivered events whose `event_type` **currently has a handler**:
   `delivered = FALSE AND event_type IN (...)`, oldest `event_date` first.
2. For each event, insert one notification row per subscribed handler, with the event's columns
   copied: `PENDING`, `attempts = 0`, `next_retry_at = event_date`. So an older event fanned out
   late is still handled first.
3. Mark those events `delivered = TRUE`, and commit.
4. Go round again at once if anything was read on any shard; otherwise wait the poll delay.

**Safe to repeat.** If two fan-outs read the same events at once, the second one's insert breaks
the `(event_id, handler_name)` unique key, its transaction rolls back, and the round fails with
an error; the job runs again on its next slot and finds those events delivered.

**Action rows are skipped.** An action that emitted no event still writes one row, with
`event_type` null. There is no handler to call for it.

**Events with no handler yet stay undelivered.** Deploy a handler for them later, and the next
round fans the older rows out too. No event is marked done while nobody could receive it.

## Handling

Each round, on **every shard at the same time**:

1. Read up to `batch_size` due notifications **from the shard's primary**:
   `state IN ('PENDING','FAILED') AND next_retry_at <= now`, oldest first.
2. **Already too old:** a row whose `event_date + retention_window` has passed goes straight to
   `EXPIRED`, without calling the handler.
3. Call the handlers for the rest of the batch:
   - it returns `Ok` → `SUCCEEDED`, `attempts + 1`;
   - it returns `Err`, and the next try would still fall inside the window → `FAILED`,
     `attempts + 1`, `next_retry_at = now + backoff`;
   - it returns `Err`, and the next try would fall past `event_date + retention_window` →
     `EXPIRED`, `attempts + 1`, at once.
4. Write the outcomes, one `UPDATE` per group of rows with the same outcome. There is no
   transaction around them: if one write fails, the ones before it stay written.
5. Go round again if anything was read; otherwise wait the poll delay.

The wait before the next try doubles each time, up to the cap: `30s x 2^(attempts - 1)`, at most 5
minutes. So 30s, 1m, 2m, 4m, then 5m each time, until the 7-day window closes. `backoff_delay`
computes it.

Rows that expire are logged at `ERROR`, grouped by handler - `DISCARDED ... event
notification(s) for handler '...'` - with an example event id: those events will never be
delivered to that handler.

**The handlers of one batch run at the same time.** An async handler's calls in one batch all run
together, up to `batch_size`, so your handler runs alongside other handlers and other calls to
itself. If it keeps state between calls, that is the thing to look at.

**A panic is a failed delivery.** A handler that panics is caught and treated as an `Err`: its
delivery is retried with backoff, then expired after the retention window. The rest of the
batch is recorded as usual. Still, return an `Err` for anything that can fail and keep panics
for bugs.

The jobs count their work in OpenTelemetry: see
[Observability](/reference/runtime/observability/).

## Handlers must be safe to repeat

Delivery is **at least once**: never less, occasionally more. A handler runs twice when:

- it succeeds, and the process dies before the row is marked `SUCCEEDED`;
- it partly succeeds (the email goes out, the next write fails) and returns `Err`;
- the handling job's run stops heartbeating for 60 seconds (on the defaults) and another instance
  takes it over. Running *long* does not cause this; the heartbeat has its own loop.

What works:

- **An insert that ignores a duplicate**, keyed on `envelope.event_id`.
- **An update whose `WHERE` checks the state it expects.** The first call moves the row on;
  repeats change nothing.
- **An idempotency key** on an outside call: pass `envelope.event_id`.
- **An action** whose params carry the source event id, so the next reader can drop duplicates.

What you must not rely on:

- the order **between** events: each shard drains its own backlog, and one batch runs together;
- the order between the handlers of one event;
- the handler being called *exactly* once. Plan for two.

## Starting another action from a handler

A handler may take the executor and run another action. From the examples, each deposit becomes
a notification:

```rust
fn handle(&self, envelope: EventEnvelope<WalletMoneyDepositedEvent>) -> Result<(), Error> {
    let wallet_id: Uuid = envelope
        .model_id
        .as_deref()
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| Error::Unexpected("a deposit event without its wallet id".to_owned()))?;
    let params: serde_json::Value = serde_json::from_str(&envelope.action_params)?;
    let Some(recipient) = params["recipient"].as_str() else {
        return Ok(());
    };
    let message = format!(
        "Deposit of {} received. New balance: {}.",
        envelope.event.amount, envelope.event.new_balance
    );
    self.executor.execute::<CreateNotificationAction>(
        Some("wallet-handler"),
        create_notification_action::Params {
            wallet_id,
            kind: "MONEY_DEPOSITED".to_owned(),
            recipient: recipient.to_owned(),
            message,
        },
    )?;
    Ok(())
}
```

This is the right place for one action to follow from another's effect. The first action has
already committed, so the new one gets its own transaction, its own retries and its own event
rows. It is what a [saga](/learn/sagas/) is built from.

What a handler must **not** do is take a connection and write directly. A write outside an action
writes no event row, so the change has no event to explain it.

## Renaming a handler

A handler's `name()` is written on every notification row. Rename the type freely; rename
`name()`, and the rows already queued under the old name match no handler. Each try on them fails
with `No handler registered with name: <old name>`, logged as a warning and retried like any
failure, until the retention window passes and they expire.

Keep the old name in `aliases()`:

```rust
fn aliases(&self) -> Vec<String> {
    vec!["wallet-deposit-alert".to_owned()] // what it used to be
}
```

New rows carry the new name; queued rows under the old one still reach this handler. Aliases are
lookup only: they do not replay history or add a second subscription. Drop the alias once no row
is left waiting under the old name:

```sql
SELECT state, COUNT(*) FROM eventlog.event_notifications
WHERE handler_name = 'wallet-deposit-alert' AND state IN ('PENDING', 'FAILED')
GROUP BY state;
```

## Tested

On PostgreSQL, MySQL and MariaDB (`wallet-example/tests/local_event_handler.rs`): one
notification per event and handler, typed delivery, a failing handler retried after its backoff,
a hopeless one expired, an old event expired without calling its handler, unique names, and both
jobs running end to end in a `JobRegistry`, ended by `stop()`.

## See also

- [The outbox](/learn/outbox/) - where these events come from
- [Consuming events](/learn/consuming-events/) - the ways to read the outbox, this one among them
- [`event_notifications`](/reference/tables/event-notifications/) - the table and its states
- [Distributed jobs](/reference/jobs/distributed-jobs/) - what both jobs are
