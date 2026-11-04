# axum-wallet-saga-pg

A wallet-to-wallet transfer as a **saga**: three single-wallet actions chained by events through
the framework's local event handler, with forward-only compensation. No message broker: the
whole choreography runs in this process, driven by the event log. It is the Rust counterpart of
Java's `spring-boot-wallet-saga-gradle-pg`.

## What it shows

| Piece | Where |
|---|---|
| `Model` with the saga's steps | `Wallet` in `src/model.rs`: `initiate_transfer_out`, `complete_transfer_in`, `mark_transfer_failed`, `refund_transfer` |
| `Entity` marking a step done | `TransferStep`, keyed by transfer id and step |
| Actions | `InitiateTransferAction`, `CompleteTransferAction`, `RefundTransferAction`, plus the wallet ones |
| Event handlers | `TransferInitiatedEventHandler`, `TransferFailedEventHandler` in `src/handler.rs` |
| Events | `TransferInitiatedEvent`, `TransferCompletedEvent`, `TransferFailedEvent`, `TransferRefundedEvent` |
| HTTP | `POST /wallets/transfers` (202), plus the wallet endpoints |
| Test | `tests/api.rs`: the happy path, two compensations, replays |

## The saga, in three steps

```
POST /wallets/transfers
  └─ Step 1: InitiateTransferAction            (one transaction, in the request)
       debit the source, emit TransferInitiatedEvent
         └─ TransferInitiatedEventHandler
              └─ Step 2: CompleteTransferAction (one transaction)
                   destination open, same currency?
                     yes: credit it, emit TransferCompletedEvent   (done)
                     no:  emit TransferFailedEvent on the *source*
                            └─ TransferFailedEventHandler
                                 └─ Step 3: RefundTransferAction (one transaction)
                                      credit the source back, emit TransferRefundedEvent
```

Two commits on the happy path, three when it compensates. Each step is its own transaction, so
each is durable on its own.

## The ideas

1. **Forward-only compensation.** The refund is not a rollback of step 1, which committed long
   ago. It is a new action with its own commit and its own event.
2. **Choreography, not orchestration.** No class decides what runs next. Each handler knows only
   its own next step; the state is in the event log.
3. **Idempotent replay.** Handlers run at least once. Step 2 and the refund each stage a
   `TransferStep` marker beside their wallet change. On a replay the marker is already there and
   the step does nothing. The table's `UNIQUE (transfer_id, step)` refuses a second marker even
   if two replays race.
4. **Self-describing events.** Every transfer event carries the transfer id, both wallets and the
   amount, so a handler needs nothing else.
5. **The source tells the whole story.** A failed transfer is recorded on the source wallet
   (`TransferFailedEvent`, no balance change). Its history in `eventlog.events` reads
   `TransferInitiated → TransferFailed → TransferRefunded`.

## Run it

```bash
cd ekbatan-rs-examples/axum-wallet-saga-pg
docker compose up -d
cargo run -p axum-wallet-saga-pg
```

```bash
# Two wallets
curl -X POST http://localhost:8080/wallets -H 'Content-Type: application/json' \
    -d '{"ownerId":"00000000-0000-0000-0000-000000000001","currency":"USD","initialBalance":"100.00"}'
curl -X POST http://localhost:8080/wallets -H 'Content-Type: application/json' \
    -d '{"ownerId":"00000000-0000-0000-0000-000000000002","currency":"USD","initialBalance":"0.00"}'

# A transfer: 202 with the transferId; the source shows 75 at once, B gets 25 a moment later
curl -X POST http://localhost:8080/wallets/transfers -H 'Content-Type: application/json' \
    -d '{"fromWalletId":"<A.id>","toWalletId":"<B.id>","amount":"25.00"}'

# A compensation: close B first; A drops to 50, then comes back to 75
curl -X POST http://localhost:8080/wallets/<B.id>/close
curl -X POST http://localhost:8080/wallets/transfers -H 'Content-Type: application/json' \
    -d '{"fromWalletId":"<A.id>","toWalletId":"<B.id>","amount":"25.00"}'
```

## Test

```bash
cargo test -p axum-wallet-saga-pg
```

It checks Java's three scenarios against a PostgreSQL in Docker:

1. **Happy path:** 25 from A (100) to B (0) settles at A = 75, B = 25.
2. **Closed destination:** A is debited and then refunded to 100; B stays closed at 0.
3. **Missing destination:** A is refunded to 100.

The first two then replay the follow-up step with the same transfer id and check that no
balance moves again. A last check sends an overdraft, which step 1 refuses with 422 before
anything is written.
