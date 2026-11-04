# ekbatan-rs

The Rust port of [Ekbatan](https://github.com/ekbatan-io/ekbatan) (Java) and
[ekbatan-py](https://github.com/ekbatan-io/ekbatan-py) (Python): a persistence framework for
event-driven systems on PostgreSQL, MySQL and MariaDB.

- **Actions** stage changes to immutable, versioned models; the executor writes them, together
  with the events the models carry, in **one transaction per shard** (the outbox pattern, built
  in).
- **Sharding** packed into each id, with the routing done for you.
- **Optimistic locking**, retries, keyed locks, **distributed jobs** that run once per slot
  across a cluster, and a **local event handler** that delivers your own events to your own code.
- **Two APIs over one engine**: `ekbatan::blocking` (plain functions) and `ekbatan::aio`
  (`async`), so the same rules hold either way.
- Java's tables, event JSON, config keys, span names and metrics, so the three ports work side by
  side.

Early work, not yet on crates.io; see [docs/LEDGER.md](docs/LEDGER.md) for what is done.

## Try it

You need Rust (the toolchain is pinned in `rust-toolchain.toml`) and Docker:

```sh
cargo run -p wallet-service-blocking-pg   # one wallet's life, start to end
```

More in [`ekbatan-rs-examples/`](ekbatan-rs-examples/): REST services on each database (axum),
a sharded one, a saga, and a job worker.

## Develop

```sh
cargo test                                   # the framework, on PostgreSQL, MySQL and MariaDB
cargo test -p axum-wallet-rest-pg            # one example
python3 ekbatan-rs-examples/sync-examples.py --check
```

Both need Docker: the tests start their databases, and building generates each schema from a
throwaway database with the migrations applied.

## License

Apache License 2.0; see [LICENSE](LICENSE).
