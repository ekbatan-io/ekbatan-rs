# axum-wallet-rest-sharded-pg

The wallet service of [`axum-wallet-rest-pg`](../axum-wallet-rest-pg) over **two** PostgreSQL
servers, one per shard. It is the Rust counterpart of Java's
`spring-boot-wallet-rest-gradle-sharded-pg`.

- global shard: `ShardIdentifier(group 0, member 0)`, the default shard
- Mexico shard: `ShardIdentifier(group 1, member 0)`

## How a wallet finds its database

`Wallet`'s id is a `ShardedId`: the shard is packed into the id's bits when the wallet is
created. `WalletCreateAction` is the only place a shard is chosen (`countryCode: "MX"` goes to
Mexico, anything else to the global shard). From then on every deposit, close and read finds the
database from the id alone. `WalletRepository` says so with `ShardingStrategy::EmbeddedBits`.

`Notification`s don't shard: they all live on the default shard, wherever their wallet is.

Each action touches one shard. Cross-shard actions are not enabled, so an action that tried to
write to both databases would be refused before anything is written (`Error::CrossShard`).

## Run it

You need Docker: the build generates the schema from a throwaway database, and `compose.yaml`
starts both databases.

```bash
cd ekbatan-rs-examples/axum-wallet-rest-sharded-pg
docker compose up -d
cargo run -p axum-wallet-rest-sharded-pg
```

The app migrates both databases at startup. `GLOBAL_DATABASE_URL`, `MEXICO_DATABASE_URL`,
`DATABASE_USER` and `DATABASE_PASSWORD` override the addresses and login in `ekbatan.json`.

```bash
# A wallet on the Mexico shard: the response's shardGroup is 1
curl -X POST http://localhost:8080/wallets \
    -H 'Content-Type: application/json' \
    -d '{"countryCode":"MX","ownerId":"00000000-0000-0000-0000-000000000001","currency":"MXN","initialBalance":"0.00"}'

# Deposit, read and close work as in axum-wallet-rest-pg, routed by the id
curl -X POST http://localhost:8080/wallets/<id>/deposits \
    -H 'Content-Type: application/json' \
    -d '{"amount":"25.50","recipient":"alice@example.com"}'
```

## Test

```bash
cargo test -p axum-wallet-rest-sharded-pg
```

It starts two PostgreSQL servers in Docker and builds the app from the shipped `ekbatan.json`.
It opens a wallet on each shard and checks with `WalletRepository::exists_on_shard` that each
row is only in its own database. Then it deposits to both and waits for the two notifications
the event handler writes.
