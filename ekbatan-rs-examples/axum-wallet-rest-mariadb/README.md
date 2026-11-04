# axum-wallet-rest-mariadb

A wallet service over HTTP on MariaDB, built with Ekbatan's async API (`ekbatan::aio`) and
[axum](https://github.com/tokio-rs/axum). It is the Rust counterpart of Java's
`spring-boot-wallet-rest-gradle-pg`: the same endpoints, the same domain, the same tables.

## What it shows

| Piece | Where |
|---|---|
| `Model` (versioned, emits events) | `Wallet` in `src/model.rs` |
| `Entity` (no events) | `Notification` in `src/model.rs` |
| Actions | `src/action/`: `WalletCreateAction`, `WalletDepositMoneyAction`, `WalletCloseAction`, `CreateNotificationAction` |
| Repositories | `WalletRepository`, `NotificationRepository` in `src/repository.rs` |
| Event handler | `WalletMoneyDepositedEventHandler` in `src/handler.rs` |
| HTTP endpoints | `src/web.rs` |
| Wiring from config | `src/app.rs`, reading `ekbatan.json` |
| Migrations | `migrations/`, run by the app at startup |
| Local database | `compose.yaml` |
| Generated schema | `build.rs` (needs Docker) |
| Test against a real database | `tests/api.rs` |

### What happens on a deposit

1. `POST /wallets/{id}/deposits` takes the keyed lock `wallet:{id}`, then runs
   `WalletDepositMoneyAction`. The wallet row and its `WalletMoneyDepositedEvent` commit in one
   transaction. Two deposits to the same wallet wait for each other instead of one failing on
   the optimistic lock.
2. The framework's fan-out and handling jobs pick up the event, in this process.
3. The handler reads the `recipient` from the deposit's own params and runs
   `CreateNotificationAction`, which writes a `Notification` row in a second transaction.

If the handler fails, it is retried with backoff (30s, doubling, up to 5 minutes) for up to
7 days.

## Run it

You need Docker: the build generates the schema from a throwaway database, and `compose.yaml`
starts the database the app talks to.

```bash
cd ekbatan-rs-examples/axum-wallet-rest-mariadb
docker compose up -d
cargo run -p axum-wallet-rest-mariadb
```

The app runs its migrations at startup; the API is at `http://localhost:8080/wallets`. The
database address comes from `ekbatan.json`, where `DATABASE_URL`, `DATABASE_USER` and
`DATABASE_PASSWORD` override the defaults; `PORT` changes the port.

```bash
# Open
curl -X POST http://localhost:8080/wallets \
    -H 'Content-Type: application/json' \
    -d '{"ownerId":"00000000-0000-0000-0000-000000000001","currency":"USD","initialBalance":"0.00"}'

# Deposit
curl -X POST http://localhost:8080/wallets/<id>/deposits \
    -H 'Content-Type: application/json' \
    -d '{"amount":"25.50","recipient":"alice@example.com"}'

# Read
curl http://localhost:8080/wallets/<id>

# Close
curl -X POST http://localhost:8080/wallets/<id>/close
```

Errors come back as `{"error": "..."}`: 422 for a rejected request (for example a negative
deposit), 404 for an unknown wallet, 409 when a wallet changed underneath a request. Ctrl-C
stops the server: requests in flight finish, the jobs stop, the pools close.

## Test

```bash
cargo test -p axum-wallet-rest-mariadb
```

The test starts MariaDB in Docker, builds the app from the shipped `ekbatan.json` (with the
container's address in its placeholders), calls every endpoint, sends two deposits at once, and
waits for the notifications the handler writes.

## Configuration

`ekbatan.json` holds the `ekbatan` section, with the same keys as Java's `application.yml`. One
database has three pools: `primaryConfig` for requests, `jobsConfig` for the background jobs, and
`lockConfig` for keyed locks. Kept apart, held locks and polling jobs never take a connection a
request needs. `local-event-handler.handling.enabled` turns the event handler on.
