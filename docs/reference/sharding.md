# Sharding

ShardIdentifier, ShardedId, the strategies, and the unknown-shard policy: how to lay out your shards, give a row an id that says which shard it lives on, and route repositories, actions and migrations by it.

This is the reference. Why there are two numbers in a shard, and when to add a shard at all, is
in [Sharding](/learn/sharding/) - read that first.

## Two numbers: group and member

A `ShardIdentifier` is a pair of numbers, `(group, member)`. They answer different questions:

- **Group** is the **policy** number: limits set from outside. Data residency ("Mexico data
  stays in Mexico"), a tenant that must be kept apart, a separate business area. Those decide how
  many groups you have. **8 bits: 0 to 255.**
- **Member** is the **capacity** number: more databases inside one group. You add a member when
  one database cannot take the writes. **6 bits: 0 to 63.**

```rust
use ekbatan::ShardIdentifier;

const GLOBAL: ShardIdentifier = ShardIdentifier::new(0, 0); // ShardIdentifier::DEFAULT
const MEXICO: ShardIdentifier = ShardIdentifier::new(1, 0);
const MEXICO_2: ShardIdentifier = ShardIdentifier::new(1, 1); // a second database in the same group
```

That is 16,384 possible shards. The numbers travel **inside each id**, not in a lookup table -
see [Ids that carry their shard](#ids-that-carry-their-shard-shardedid).

| | |
| --- | --- |
| `ShardIdentifier::new(group, member)` | for constants. A member above 63 stops the build in a `const`, and panics anywhere else |
| `ShardIdentifier::of(group, member)` | for numbers read at run time (a request, a file). Takes `u32`s, and returns `Error::Invalid` for a group above 255 or a member above 63 |
| `ShardIdentifier::DEFAULT` | `0/0` |
| `MIN_GROUP`, `MAX_GROUP`, `MIN_MEMBER`, `MAX_MEMBER` | `0`, `255`, `0`, `63` |
| `shard.group`, `shard.member` | the two numbers, as `u8`s |

A shard prints as `group/member`: `1/0`. It sorts by group, then member.

The members of one group are expected to sit close together: same region, same network. Nothing
checks it, but a read that visits every shard is slower when they are far apart.

## The layout

A layout has two levels: **groups**, each holding **members**. Each member is one shard, with its
databases listed by name.

```rust
use ekbatan::{
    DataSourceConfig, DatabaseRegistry, Dialect, ShardGroupConfig, ShardIdentifier,
    ShardMemberConfig, ShardingConfig, UnknownShardPolicy,
};

let password = std::env::var("DB_PASSWORD").ok();
let db = |url: &str, pool: u32| DataSourceConfig {
    username: Some("wallet".to_owned()),
    password: password.clone(),
    ..DataSourceConfig::new(Dialect::Postgres, url, pool)
};

let layout = ShardingConfig {
    default_shard: GLOBAL,
    unknown_shard_policy: UnknownShardPolicy::UseDefaultShard,
    groups: vec![
        ShardGroupConfig {
            group: 0,
            name: "global".to_owned(),
            members: vec![ShardMemberConfig {
                member: 0,
                name: Some("global-eu-1".to_owned()),
                configs: vec![
                    ("primaryConfig".to_owned(), db("postgres://global-db:5432/wallet", 20)),
                    ("secondaryConfig".to_owned(), db("postgres://global-replica:5432/wallet", 10)),
                    ("lockConfig".to_owned(), db("postgres://global-db:5432/wallet", 30)),
                ],
            }],
        },
        ShardGroupConfig {
            group: 1,
            name: "mexico".to_owned(),
            members: vec![ShardMemberConfig {
                member: 0,
                name: Some("mexico-1".to_owned()),
                configs: vec![("primaryConfig".to_owned(), db("postgres://mexico-db:5432/wallet", 20))],
            }],
        },
    ],
};
let registry = DatabaseRegistry::from_sharding_config(&layout)?;
```

One database is `ShardingConfig::single(config)`: the shard `0/0` in a group named `default`, as
the default shard.

The same layout can be read from a JSON file: see [Config files](/reference/config-files/).

A member's `(group, member)` pair is its `ShardIdentifier`. Its `configs` are named
`DataSourceConfig`s, with two reserved names and room for any others:

| name | |
| --- | --- |
| `"primaryConfig"` | **required.** The pool for writes, and for reads inside a transaction. Also `ShardMemberConfig::PRIMARY_CONFIG` |
| `"secondaryConfig"` | optional. A read replica, for reads that may be a little behind. Leave it out and those reads go to the primary. Also `ShardMemberConfig::SECONDARY_CONFIG` |
| anything else | yours: `"jobsConfig"`, `"lockConfig"`. The registry builds pools only for the two above. Read yours back with `member.config_for("lockConfig")` and build the pool with `ConnectionProvider::from_config(...)` |

In code, spell the names exactly as shown. A config file also accepts `primary-config` and
`primary_config`, and turns them into these.

A group needs a `name`. A member's `name` is optional; it names the member in migration logs.
The member's databases themselves are described on [Connecting](/reference/connecting/).

**What is checked.** `DatabaseRegistry::from_sharding_config` calls `layout.validate()` first,
and refuses the layout with `Error::Config` when:

- there is no group, or a group has no member, or a group's name is blank;
- the same group number appears twice, or the same member number twice in one group;
- a member has no `primaryConfig`;
- the default shard is not one of the configured shards;
- any of the member's `DataSourceConfig`s fails its own checks.

**No connection is opened** when the registry is built. Each pool opens its first connection when
it is first used, so a service can start while a database is still coming up.

### Building the registry by hand

When the pools come from somewhere else, give the registry one `TransactionManager` per shard:

```rust
use ekbatan::{ConnectionProvider, DatabaseRegistry, TransactionManager, UnknownShardPolicy};

let registry = DatabaseRegistry::builder()
    .with_default_database(TransactionManager::new(GLOBAL, global_primary, Some(global_replica)))
    .with_database(TransactionManager::new(MEXICO, mexico_primary, None))
    .with_unknown_shard_policy(UnknownShardPolicy::Fail)
    .build()?;
```

`with_default_database` names the default shard. With only one database, it is the default
without saying so. No database, several with none named the default, or one shard given twice is
refused.

## Sharded models

A model that is placed on a shard declares its id type as `ShardedId<Self>` instead of
`Id<Self>`, and picks the shard once, where the row is made:

```rust
#[model(id = ShardedId<Wallet>, state = WalletState)]
pub struct Wallet {
    pub owner_id: Uuid,
    pub currency: String,
    pub balance: Decimal,
}

impl Wallet {
    pub fn create_wallet(
        shard: ShardIdentifier,
        owner_id: Uuid,
        currency: &str,
        initial_balance: Decimal,
        created_date: Instant,
    ) -> WalletBuilder {
        WalletBuilder::new()
            .id(ShardedId::generate(shard)) // the shard goes into the id here
            .state(WalletState::Opened)
            .owner_id(owner_id)
            .currency(currency.to_owned())
            .balance(initial_balance)
            .created_date(created_date)
            .with_initial_version()
            .with_event(WalletCreatedEvent {
                owner_id,
                currency: currency.to_owned(),
                initial_balance,
            })
    }
}
```

**That is all the model does.** One id type, and one argument where the row is made. Nothing else
on the model mentions a shard, because the id now carries it. The row stays on that database for
its whole life.

No field holds the shard, on purpose. A column would be a second copy of what the id already
says, free to disagree with it. Reading the shard out of the id cannot drift.

The framework's own tables need no change. Each shard has its own `eventlog.events`, and an
action's event rows are written to the same database as the rows they describe. So the outbox is
sharded with no setting. (On MySQL and MariaDB, `eventlog` is one per server: give each shard a
server of its own. See [Multi-database](/reference/multi-database/#schema-and-database).)

## Ids that carry their shard: `ShardedId`

The shard is packed into the random part of a version-7 UUID:

```
high 64 bits: [48-bit milliseconds][4-bit version = 7][12 random bits]
low 64 bits:  [2-bit variant][8-bit GROUP][6-bit MEMBER][48 random bits]
```

Ekbatan for Java packs the shard into the same bits, so an id made by either reads the same
shard in the other.

```rust
let wallet_id = ShardedId::<Wallet>::generate(MEXICO); // on that shard for the row's whole life
wallet_id.shard();     // ShardIdentifier { group: 1, member: 0 }
wallet_id.instant();   // when it was made: the millisecond timestamp in its first 48 bits
wallet_id.uuid();      // the plain uuid::Uuid
ShardedId::<Wallet>::from_uuid(uuid); // back from a uuid read from a request or a row
```

The timestamp is in the high bits, so ids made later sort later in the database's own order.
That keeps a primary-key index from scattering its writes the way random (version 4) uuids do.

`ShardedId` prints as the plain uuid; its `Debug` form adds the shard:
`0190...-... (shard 1/0)`.

## Sharded repositories

A repository opts in by returning `ShardingStrategy::EmbeddedBits` from `sharding_strategy`. The
default is `ShardingStrategy::NoSharding`.

```rust
impl AbstractRepository for WalletRepository {
    type Persistable = Wallet;
    type Record = WalletsRecord;

    fn registry(&self) -> &DatabaseRegistry {
        &self.registry
    }

    fn sharding_strategy(&self) -> ShardingStrategy {
        ShardingStrategy::EmbeddedBits
    }

    fn from_record(record: WalletsRecord) -> Result<Wallet, Error> { /* ... */ }
    fn to_record(wallet: &Wallet) -> WalletsRecord { /* ... */ }
}
```

That one method is the whole difference from an unsharded repository. `from_record` and
`to_record` stay the same, because the id type already carries the shard.

| strategy | an id names | `dbs()` and `readonly_dbs()` visit |
| --- | --- | --- |
| `NoSharding` (the default) | nothing: every id goes to the default shard | the default shard only |
| `EmbeddedBits` | the shard in its bits, for a version-7 uuid. Any other uuid goes to the default shard | every shard |

What it changes about the ready-made reads and writes (the same in `blocking::Repository` and
`aio::AsyncRepository`):

| | routes by |
| --- | --- |
| `find_by_id`, `get_by_id`, `exists_by_id`, `add`, `add_no_result`, `update`, `update_no_result` | the shard in the id |
| `add_all`, `update_all` and their `_no_result` forms | the shard of the models' ids. All of them must be on one shard; two shards is `Error::CrossShard` |
| `find_all_by_ids` | each id's shard. The ids are grouped by shard and each shard is asked once. Repeated ids are dropped; an empty list touches no database |
| `find_all`, `find_all_where`, `find_one_where`, `exists_where`, `count`, `count_where` | **every shard's primary, one after another** - a condition says nothing about where a row is |

`find_one_where` and `exists_where` stop at the first shard that has a match, so the shards after
it are not asked. If a shard fails, the whole read fails with that shard's error. There are no
partial results.

### Picking a database for your own queries

When no ready-made method answers your question, write the query and pick its database with a
selector. Each takes a *shard key*: `DefaultShard`, a `ShardIdentifier`, an `Id`, or a
`ShardedId` (or a reference to one). An id names its shard through the repository's strategy.

| selector | what you get |
| --- | --- |
| `db(key)` | the shard's primary, outside any transaction |
| `readonly_db(key)` | the shard's **replica**, for a read that may be a little behind; the primary where there is no replica |
| `tx_db(tx, key)` | the caller's open transaction. Refused with `Error::WrongShard` when the key names another shard |
| `tx_db_else_db(tx, key)` | the caller's transaction when given one (`Some(tx)`), else the primary |
| `dbs()` / `readonly_dbs()` | every shard's primary or replica: the default shard only under `NoSharding` |

From the sharded example: a count on every shard's replica, asked at the same time, added up.

```rust
pub async fn total_opened_wallets(&self) -> Result<i64, Error> {
    let count = Query::select()
        .expr(Func::count(Expr::col(Asterisk)))
        .from(Wallets::Table)
        .and_where(Expr::col(Wallets::State).eq(WalletState::Opened.as_str()))
        .to_owned();
    Ok(self.readonly_dbs().fetch_scalars::<i64>(&count).await?.into_iter().sum())
}
```

And one shard, named outright:

```rust
AsyncRepository::readonly_db(self, MEXICO)?.fetch_exists(&select).await
```

`fetch_all` and `fetch_scalars` on `dbs()` or `readonly_dbs()` ask every shard at the same time.
The ready-made reads above do not: they go one shard at a time.

Which selector you call is the decision, and you make it. Nothing joins a transaction behind your
back. See [Repository](/reference/repository/).

### Reaching a specific shard

You rarely need these: a repository routes for you. They are for an admin task, a check, or a test
that looks at where a row landed:

```rust
registry.transaction_manager(MEXICO)?;   // that shard's transactions
registry.default_transaction_manager();  // the default shard's
registry.effective_shard(MEXICO)?;       // the shard actually used, after the unknown-shard policy
registry.shards();                       // every configured shard, in order
registry.default_shard();
registry.primary(MEXICO)?;               // statements on that shard's primary (blocking)
registry.as_async().secondary(MEXICO)?;  // statements on its replica (async)
```

To ask where an id lives: `wallet_id.shard()`, or `repository.effective_shard(&wallet_id)?`,
which also applies the policy below.

## Shards this deployment does not have

An id naming a shard this deployment has no database for **goes to the default shard**. That is
what lets a staging database with one shard read ids made where there are four.

A mistyped shard number does exactly the same thing, and that one writes to the wrong database.
Nothing at the call site can tell the two apart. **No warning is logged** when it happens, so a
deployment that holds the **whole** layout should make it an error instead:

```rust
ShardingConfig {
    unknown_shard_policy: UnknownShardPolicy::Fail, // the default is UseDefaultShard
    ..layout
}
```

or `with_unknown_shard_policy(UnknownShardPolicy::Fail)` on the registry or its builder. Then:

```
no database for shard 9/9
```

That is `Error::UnknownShard(shard)`. The policy applies wherever an id or a shard is routed, so
it covers **reads and writes alike**: `find_by_id` on an unknown shard fails just as `add` does.
Leave it at the default if your deployment holds only **part** of the layout. There it would
refuse exactly the reads of other deployments' ids that the fallback exists for.

In a config file the policy is a word, read by `UnknownShardPolicy`'s `FromStr`. Case, `_` and
`-` do not matter, so `FAIL`, `fail`, `USE_DEFAULT_SHARD`, `useDefaultShard` and
`use-default-shard` all work, and a blank value means the default.

## There are no custom strategies

`ShardingStrategy` is an enum with two values, `NoSharding` and `EmbeddedBits`. You cannot add a
third. So two things are out of reach:

- **Moving live data between shards.** The shard is in the id for the row's whole life, so a
  moved row's id would name the wrong shard.
- **Placing rows by a key the id does not carry**, decided after the row was made.

Both would make the id stop saying where its row is, and every read that starts from an id would
need a lookup first. A query of your own can still go to any shard: pass the `ShardIdentifier` to
a selector.

## Cross-shard actions

An action whose changes land on more than one shard is refused by default:

```
one call can't write to shard 0/0 and shard 1/0
```

That is `Error::CrossShard`, and nothing is written. To allow it, say so for that one call:

```rust
let allow = ExecutionConfiguration::builder().allow_cross_shard(true);
let opened = executor
    .execute_with::<OpenOnBothShards>(None, params, &allow)
    .await?;
```

With it on, each shard gets its own transaction, and each commits on its own, one after another.
Each shard's event rows carry the same `action_id`, so a consumer reading all of them can tell it
was one action. If a later shard fails, the shards that already committed stay committed, and the
framework logs an error that starts `CRITICAL: Cross-shard action ... PARTIALLY COMMITTED!` and
names them.

**This is not a two-phase commit.** There is no such thing across separate databases here. A
partial commit is what the setting means, and the setting exists to make you say you accept it.

Set it per call, so the risk is visible where it is taken. Making it the executor's default
(`default_execution_configuration(...)` on its builder) is only for an executor whose actions are
*all* written for per-shard commits.

If a partial commit would need undoing, you want a [saga](/learn/sagas/): one action per shard,
chained through the outbox, with a compensating action when a later step fails.

## Migrating every shard

```rust
use ekbatan::migration::Migrator;

Migrator::builder()
    .with_sharding_config(&layout) // every shard's primaryConfig, in the configured order
    .locations(["migrations"])
    .migrate()
    .await?;
```

Only some shards:

```rust
let only_mexico = Migrator::targets(&layout).into_iter().filter(|t| t.shard == MEXICO);
Migrator::builder().with_targets(only_mexico).locations(["migrations"]).migrate().await?;
```

Each target is named `<group name>/<member name>` (`member-<n>` when the member has none). See
[Migrations](/reference/migrations/).

## What sharding does not do

- **No warning on a fallback** to the default shard. Use `UnknownShardPolicy::Fail` where you can.
- **No custom strategy**, and no moving rows between shards (above).
- **The ready-made reads that visit every shard go one shard at a time**, in blocking and async
  code alike: on eight shards, eight round trips. A query of your own on `dbs()` or
  `readonly_dbs()` asks them all at once. The event jobs also work through every shard at once.
- **One transaction is one shard.** A repository write given a transaction on another shard is
  refused with `Error::WrongShard`.

## See also

- [Repository](/reference/repository/) - the selectors, and the mapping you write
- [Sharding](/learn/sharding/) - the same thing step by step, against a running example
- [Action executor](/reference/action-executor/) - `ExecutionConfiguration`, retries and
  cross-shard
- [Sagas](/learn/sagas/) - for a workflow that cannot accept a partial commit
- [Config files](/reference/config-files/) - the layout read from JSON
- [Migrations](/reference/migrations/) - one call migrates every shard
- [`axum-wallet-rest-sharded-pg`](https://github.com/ekbatan-io/ekbatan-rs/tree/main/ekbatan-rs-examples/axum-wallet-rest-sharded-pg) -
  two databases, running, with each row checked on the database its id names
