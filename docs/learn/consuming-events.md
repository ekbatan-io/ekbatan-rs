# Consuming events

Use committed events after the fact. Handle them in the same service with the local event handler, or stream them out. Every path is at least once.

ekbatan's job is to store events safely: when an action commits, its rows and its rows in
`eventlog.events` are written in the same transaction. What happens to the events afterwards is
up to your application. The event log is **just a table**: anything that can read it can
consume it.

```
                              eventlog.events
                                    │
               ┌────────────────────┴─────────────────────┐
               ▼                                          ▼
     local event handler                       a reader of the table
     fan-out + handling jobs                   (a CDC tool, or your own)
               │                                          │
         your handler                              a broker topic
               │                                          │
   local work, or a publish                 consumers in other services
```

You do not have to pick one. Both can read the same table at the same time.

## Choose a delivery path

### 1. Local only: listen to yourself

Use the local event handler when the consumer is part of the same service.

```
eventlog.events → fan-out job → eventlog.event_notifications → handling job → your handler
```

The simplest path: no broker, no connector, no second program to run. A good fit for
notification rows, audit rows, read models, saga steps inside one service, and "after this
action commits, do this other piece of work".

You write a handler for one event type. Whenever an event of that type is committed, the
handling job picks it up and calls your handler with an `EventEnvelope`: the typed event, the
action that produced it, and that action's parameters. The handler can write a row, call
another service, or run another action through the executor.

### 2. A local handler that publishes to a broker

Write a handler, and publish to Kafka, RabbitMQ, SQS or anything else from inside it.

```
eventlog.events → handling job → your handler → Kafka / RabbitMQ / SQS / ...
```

Choose this when publishing is more than "copy each row to one topic". Because you publish in
your own code, you choose the topic, the key, the headers and the message format. One deposit
event could go to `wallet.deposits`, also to `risk.large-deposits`, and nowhere at all when the
action's parameters say the deposit was internal. It also fits when the outgoing message needs
work first: mapping an internal event to a public contract, or removing a field.

The trade: your handler is now an at-least-once publisher. If the publish succeeds and the
process dies before the delivery is marked `SUCCEEDED`, the handler runs again. Key your
messages off `envelope.event_id` so consumers can drop the repeat.

### 3. Stream the table out

Let something outside your service read `eventlog.events` and publish its rows: a
change-data-capture (CDC) tool such as Debezium, which follows the database's own change log, or
a small reader of your own that polls the table.

```
eventlog.events → CDC tool or your own reader → broker topic → consumers
```

**ekbatan ships no connector, no transform and no reader for this.** It writes the rows; the
rest is your infrastructure. What it does promise is the row: the same columns, written the
same way every time, in plain JSON. Path B below says what a reader needs to know.

## Path A - the local event handler (start here)

Two background jobs do the work, both ordinary [distributed jobs](/reference/jobs/distributed-jobs/):
only one instance in the cluster runs each at a time, and if that instance dies, another takes
over.

- **Fan-out** (`EventFanoutJob`) reads the events nobody has delivered yet. For each one whose
  type has a handler, it writes one row per handler into `eventlog.event_notifications`, and
  marks the event `delivered`, in one transaction per shard. Events of a type with no handler
  are left as they are.
- **Handling** (`EventHandlingJob`) finds the delivery rows that are due, calls their handlers,
  and records each outcome, with retries, backoff and expiry.

### A.1 Turn it on

There is nothing to install: the local event handler is part of the crate. It is off until the
config turns it on and you add its two jobs to your `JobRegistry`:

```json
"local-event-handler": {
  "handling": { "enabled": true }
}
```

```rust
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
```

[Wiring a service](/learn/wiring-a-service/#the-jobs-and-the-event-handler) shows the lines
around it. Without it, the executor still writes every event, with `delivered = false`: exactly
the right state for a deployment that streams the table out instead. Both paths can be on at
once.

Note what the handler is given: the **executor**. A handler does its work as its own unit of
work, with its own transaction and its own events. It is not handed the transaction that
produced the event, and must not be: that transaction committed and ended before the handler
ran.

### A.2 Write the handler

**Blocking**

```rust
use ekbatan::blocking::{ActionExecutor, EventHandler};
use ekbatan::local_event_handler::EventEnvelope;

pub struct WalletMoneyDepositedEventHandler {
    executor: Arc<ActionExecutor>,
}

impl EventHandler for WalletMoneyDepositedEventHandler {
    type Event = WalletMoneyDepositedEvent;

    fn name(&self) -> String {
        "wallet-money-deposited-notification".to_owned()
    }

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
}
```

**Async**

```rust
use ekbatan::aio::{AsyncActionExecutor, AsyncEventHandler};
use ekbatan::local_event_handler::EventEnvelope;

pub struct WalletMoneyDepositedEventHandler {
    executor: Arc<AsyncActionExecutor>,
}

impl AsyncEventHandler for WalletMoneyDepositedEventHandler {
    type Event = WalletMoneyDepositedEvent;

    /// Stable across deploys: stored on each queued notification row.
    fn name(&self) -> String {
        "wallet-money-deposited-notification".to_owned()
    }

    async fn handle(
        &self,
        envelope: EventEnvelope<WalletMoneyDepositedEvent>,
    ) -> Result<(), Error> {
        let wallet_id: Uuid = envelope
            .model_id
            .as_deref()
            .and_then(|id| id.parse().ok())
            .ok_or_else(|| Error::Unexpected("a deposit event without its wallet id".to_owned()))?;
        // The recipient is not part of the domain event: it is read back from the params of
        // the action that produced it.
        let params: serde_json::Value = serde_json::from_str(&envelope.action_params)?;
        let Some(recipient) = params["recipient"].as_str() else {
            return Ok(()); // a deposit with no one to tell
        };
        let message = format!(
            "Deposit of {} received. New balance: {}.",
            envelope.event.amount, envelope.event.new_balance
        );
        self.executor
            .execute::<CreateNotificationAction>(
                Some("wallet-handler"),
                create_notification_action::Params {
                    wallet_id,
                    kind: "MONEY_DEPOSITED".to_owned(),
                    recipient: recipient.to_owned(),
                    message,
                },
            )
            .await?;
        Ok(())
    }
}
```

A handler is a type that implements `EventHandler` (blocking) or `AsyncEventHandler` (async).
No attribute, no macro.

- **`type Event`** - the event type it handles. The stored JSON is read back into it with serde.
- **`name()`** - the handler's permanent id, written on every delivery row. Keep it the same on
  every instance and across releases; see below.
- **`handle(envelope)`** - your logic. Return `Err` and the delivery is retried later.
  `envelope.event` is the typed event. Beside it: `event_id` (the event row's id, the same on
  every retry, so a natural key for spotting a repeat), `action_name`, `action_params` (the
  producing action's parameters, as JSON text), `model_id`, `model_type`, `namespace` and the
  dates.

The blocking handler works in an async service too: every `EventHandler` is also an
`AsyncEventHandler`, so it goes in the same `EventHandlerRegistry`.

#### `name()` is part of your schema

Rename the type freely. Change what `name()` returns, and the delivery rows already queued
under the old name match no handler: every attempt fails until they expire. The way through is
`aliases()`:

```rust
impl AsyncEventHandler for MoneyDepositedWelcomeHandler {
    type Event = WalletMoneyDepositedEvent;

    fn name(&self) -> String {
        "money-deposited-welcome".to_owned()
    }

    fn aliases(&self) -> Vec<String> {
        vec!["money-deposited".to_owned()] // what it used to be
    }

    async fn handle(&self, envelope: EventEnvelope<WalletMoneyDepositedEvent>) -> Result<(), Error> {
        // ...
        Ok(())
    }
}
```

An alias only finds the old rows; new rows are written under the new name. This is the most
confusing thing in the pipeline, so here it is step by step:

1. Before the deploy, delivery rows are queued under the old name, `money-deposited`.
2. After the deploy, the handler's `name()` is `money-deposited-welcome` and its `aliases()`
   keep `money-deposited`. The fan-out job writes new rows under the new name.
3. The handling job finds the handler for the old rows through the alias, and for the new rows
   through the name: both reach `MoneyDepositedWelcomeHandler`.
4. Once no row under the old name is waiting, the alias can go.

Remove the alias once no rows under the old name are still waiting. Keeping it longer does no
harm. A name or alias used twice, by one handler or two, is refused when the registry is
built.

### A.3 Run it

Start the service and make a deposit. Within a poll interval (one second by default) the
handler runs, and the delivery row records it:

```sql
SELECT event_id, handler_name, state, attempts, updated_date
FROM eventlog.event_notifications
WHERE handler_name = 'wallet-money-deposited-notification'
ORDER BY created_date DESC;
```

| state | means |
| --- | --- |
| `PENDING` | not run yet |
| `SUCCEEDED` | the handler returned `Ok` |
| `FAILED` | the handler returned `Err`; it runs again at `next_retry_at` |
| `EXPIRED` | it ran out of time: older than the retention window, 7 days by default |

Retries wait 30 seconds, then double each time, up to 5 minutes between attempts, until the
retention window ends. Both limits are settings (`handlingMaxBackoffCap`,
`handlingRetentionWindow`). See
[Local event handler](/reference/events/local-event-handler/) and
[`event_notifications`](/reference/tables/event-notifications/).

Three things the handling job does that you should know:

- **the handlers of one batch are started together.** Async handlers overlap while they wait
  on the database; blocking ones run one after another. Write handlers that are safe beside
  each other either way;
- **an `Err` is retried, and so is a panic.** A handler that panics is caught and its delivery
  counts as failed: retried with backoff, then expired, like an `Err`. The rest of the batch is
  recorded as usual. Return an `Err` for anything that can fail; keep panics for bugs;
- **an event type nobody handles is never fanned out.** Its rows stay `delivered = false`. Add a
  handler later and the fan-out job picks up the old events too; those already older than the
  retention window are expired instead of run.

## Path B - streaming the table out

When the consumers are other services, a data lake or a warehouse, let a tool outside your
service read `eventlog.events` and publish its rows.

**Nothing in this path is ekbatan code.** ekbatan has no Kafka client, no Debezium transform
and no Avro or Protobuf encoding. What your service owes the pipeline is the row, and ekbatan
writes it the same way every time.

### B.1 Where the rows are

The event tables live in `eventlog`: a schema of its own on PostgreSQL, and a database of its
own on MySQL and MariaDB. On MySQL and MariaDB that one database serves the whole server, which
is also why each shard needs a server of its own there.

| column | holds |
| --- | --- |
| `id` | the event row's id: a good key for dropping repeats |
| `namespace` | the service, from the executor's `namespace` |
| `action_id`, `action_name`, `action_params` | the action that wrote it, and its parameters as JSON |
| `model_id`, `model_type` | the model the event is about |
| `event_type`, `payload` | the event's type name, and the event as JSON |
| `started_date`, `completion_date`, `event_date` | when the action ran, and when the event was made |
| `delivered` | `false` when written; set to `true` by the fan-out job |

### B.2 Two things every reader must handle

**Rows with no event.** An action that emitted no event - one that only wrote entities, or
staged nothing at all - still writes one row, with `model_id`, `event_type` and `payload` null,
so the log says the action ran. A reader that only wants events must skip those rows.

**Updates.** The local event handler sets `delivered = true` with an `UPDATE`. A CDC tool
that publishes every change publishes that update too, so the same event would appear twice.
If both paths run, publish **inserts only**. (In Debezium that is the connector's
`skipped.operations` setting, set to `u,d,t`.)

Use `model_id` as the message key when consumers need events for one model in order: every
event for one wallet then lands on one partition.

### B.3 Fan out or re-key downstream

The reader does not have to publish to every final topic. Publish everything to one raw topic,
and let a stream processor or a small router service derive the rest: one topic per event
type, per model type, or keyed for one consumer. The raw topic stays the append-only record of
everything the service emitted, and none of this reaches back into how actions write to the
database.

## Idempotency and ordering

Every path is **at least once**: never less, sometimes more. A handler runs twice when it
succeeded and the process died before the delivery was marked `SUCCEEDED`, or when it did half
its work and returned an error. A CDC connector replays after a restart. A publisher retries.

What works:

- **a unique constraint** on a row the handler creates, keyed off the event or business id;
- **`UPDATE ... WHERE state = <expected>`** for a change of state;
- **an idempotency key** on a call to another service, such as `envelope.event_id`;
- **`model_id` as the message key** when order matters per model.

The idea to remember: **let a unique constraint, inside the transaction that does the work,
make a repeat harmless.** A check alone is not enough: between the check and the write, a
second delivery can slip in. The saga example does both - the check makes an ordinary replay
cheap, and the constraint catches the race:

```rust
// A replay: this step already ran for this transfer.
if self
    .transfer_step_repository
    .exists_step(params.transfer_id, TransferStepName::CompleteTransfer)
    .await?
{
    return self.wallet_repository.get_by_id(&params.from_wallet_id).await;
}
// Staged beside the credit. Two replays racing both get here; the table's
// UNIQUE (transfer_id, step) refuses the second marker, and its credit rolls back with it.
plan.add(TransferStep::create(params.transfer_id, TransferStepName::CompleteTransfer).build()?)?;
```

What you must **not** rely on: the order between events, the order between handlers of one
event, or a handler being called exactly once. Plan for twice.

## Listen to yourself

A handler may take the executor and run another action. This is the **only** right place for
one action to follow from another's effect: the first has already committed, so the new one
gets its own transaction and its own events. That is what a saga is built from.

## What just happened

- **The local event handler** keeps delivery inside your service: same database, no broker,
  nothing extra to run.
- **A publishing handler** lets your code send to one or many destinations, with its own keys
  and formats.
- **Streaming the table out** turns committed rows into a stream for other services, with a
  tool you run and ekbatan does not ship.
- All of them read the same rows. The [dual-write trap](/learn/the-dual-write-trap/) cannot
  happen: there is one durable write, inside the action's transaction, and everything else reads
  from it.

If you start with the local handler and stream the table out later, **no application code
changes.** The reader starts following the same table.

## Next

→ **[Adding a shard](/learn/sharding/)** - take the single-database wallet to two databases.
Your actions do not change.

## See also

- [The outbox](/learn/outbox/) - why the event table exists
- [Sagas](/learn/sagas/) - what listening to yourself becomes when a workflow has steps
- Reference: [Local event handler](/reference/events/local-event-handler/),
  [`events`](/reference/tables/events/),
  [`event_notifications`](/reference/tables/event-notifications/),
  [Observability](/reference/runtime/observability/) - the handler's metrics
- [`axum-wallet-rest-pg`](https://github.com/ekbatan-io/ekbatan-rs/tree/main/ekbatan-rs-examples/axum-wallet-rest-pg)
  (async) and [`job-worker-pg`](https://github.com/ekbatan-io/ekbatan-rs/tree/main/ekbatan-rs-examples/job-worker-pg)
  (blocking) - each runs the handler end to end, with a test that waits for the rows it writes
