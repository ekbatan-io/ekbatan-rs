# wallet-service-blocking-pg

**Start here.** One wallet's life on Ekbatan's blocking API, in plain code: no `async`, no
server, no configuration file. The whole walk-through is one function, `story::run` in
`src/story.rs`, and each step is one call that says what happened.

```bash
cargo run -p wallet-service-blocking-pg
```

You need only Docker. With no `DATABASE_URL` set, it starts a throwaway PostgreSQL, runs the
story, and removes the database at the end:

```
no DATABASE_URL: starting a throwaway PostgreSQL in Docker
1. migrated: 4 migration file(s) applied
2. opened wallet 01a1a5f6-... with 10.00 EUR (version 1)
3. deposited 25.50: balance 35.5000 (version 2)
4. read back: balance 35.5000, state OPENED, version 2
5. the wallet's events: WalletCreatedEvent, WalletMoneyDepositedEvent
6. closed: state CLOSED
7. a deposit of -5 is refused: Deposit amount must be positive
```

To use a database of your own, `docker compose up -d` starts one, then:

```bash
DATABASE_URL=jdbc:postgresql://localhost:5432/wallet DATABASE_USER=wallet \
    DATABASE_PASSWORD=wallet cargo run -p wallet-service-blocking-pg
```

## What each step shows

1. **Migrations** from `migrations/`, the same files Java's example uses. `block_on` runs the
   one async-only call; everything else is blocking.
2. **Opening a wallet**: `WalletCreateAction` stages a new `Wallet` on the plan, and the executor
   writes it, with its `WalletCreatedEvent`, in one transaction.
3. **A deposit**: a new version of the wallet (version 2), and an event, again in one
   transaction. `Wallet` is immutable: `deposit` returns a new wallet, it doesn't change the old.
4. **Reading back** through `WalletRepository`, which only says how a row becomes a wallet
   (`from_record` / `to_record`); `get_by_id` and the rest come with it.
5. **The event log** (`eventlog.events`): every action's events, written in the same
   transactions as the rows they describe. Other services can read it.
6. **Closing** it.
7. **A refused change**: the model refuses a negative deposit, so nothing is written.

## Where to go next

- [`axum-wallet-rest-pg`](../axum-wallet-rest-pg): the same wallet over HTTP, async, with a
  keyed lock and an event handler.
- [`job-worker-pg`](../job-worker-pg): distributed jobs on the blocking API.

## Test

```bash
cargo test -p wallet-service-blocking-pg
```

Runs the story against a PostgreSQL in Docker and checks every step's result.
