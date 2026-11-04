# Your first action

Build a wallet deposit end to end: the event, the model, the action, the call that runs it, and the event row you never wrote.

This lesson builds on [Getting started](/learn/getting-started/). We write real code: a `Wallet`
that emits a `WalletMoneyDepositedEvent`, and a `WalletDepositMoneyAction` that changes its
balance.

The code is taken from the examples in `ekbatan-rs-examples/`. The blocking version is
`wallet-service-blocking-pg`, which runs from plain code with no server. The async version is
`axum-wallet-rest-pg`, a REST service on axum. The event, the model, the table and the repository
are the same in both; the action and the call are shown both ways, with the switch above each
sample. Not sure which to use? See [Blocking or async?](/learn/blocking-or-async/).

What we are building:

```
              a deposit of 25.50 to wallet <id>
                        ▼
            executor.execute::<WalletDepositMoneyAction>(
                Some("alice"),
                Params { wallet_id, amount, recipient })
                        ▼
        ┌─── Step 1 — perform() ───────────────────────────┐
        │  wallet = wallet_repository.get_by_id(...)       │
        │  updated = wallet.deposit(amount)?               │
        │      ↳ attaches WalletMoneyDepositedEvent        │
        │  plan.update(updated)                            │
        └──────────────────────────────────────────────────┘
                        ▼
        ┌─── Step 2 — the executor commits ────────────────┐
        │  one transaction:                                │
        │     UPDATE wallets  WHERE id=? AND version=?     │
        │     INSERT INTO eventlog.events (1 row)          │
        │  commit                                          │
        └──────────────────────────────────────────────────┘
```

## 1. The event

An event is a plain struct with the fields a reader needs, and nothing else:

```rust
#[model_event(Wallet)]
pub struct WalletMoneyDepositedEvent {
    pub amount: Decimal,
    pub new_balance: Decimal,
}
```

`#[model_event(Wallet)]` says which model the event is about. The event does not hold the
wallet's id: it is taken from the wallet the event is attached to. The id and the model's name go
into the event row's `model_id` and `model_type` columns, so a reader can find all of one
wallet's events without opening the payload.

Notice what is *not* in it. The deposit also takes a `recipient`, for a notification, and it is
not here: an address has no place in a money-deposited event. It travels in the action's
parameters instead, which are saved on the same row, and a handler that needs it reads it from
there.

## 2. The model

A model is immutable, has a version, and emits events through `with_event(...)`:

```rust
use ekbatan::{Error, Model, ShardedId, model, model_event};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalletState {
    Opened,
    Settled,
    Closed,
    Deleted,
}

impl WalletState {
    /// The text in the `state` column.
    pub fn as_str(self) -> &'static str {
        match self {
            WalletState::Opened => "OPENED",
            WalletState::Settled => "SETTLED",
            WalletState::Closed => "CLOSED",
            WalletState::Deleted => "DELETED",
        }
    }

    pub fn parse(text: &str) -> Option<WalletState> {
        match text {
            "OPENED" => Some(WalletState::Opened),
            "SETTLED" => Some(WalletState::Settled),
            "CLOSED" => Some(WalletState::Closed),
            "DELETED" => Some(WalletState::Deleted),
            _ => None,
        }
    }
}

#[model(id = ShardedId<Wallet>, state = WalletState)]
pub struct Wallet {
    pub owner_id: Uuid,
    pub currency: String,
    pub balance: Decimal,
}

impl Wallet {
    pub fn deposit(&self, amount: Decimal) -> Result<Wallet, Error> {
        if amount <= Decimal::ZERO {
            return Err(Error::Invalid("Deposit amount must be positive".to_owned()));
        }
        let new_balance = self.balance + amount;
        self.copy()
            .with_event(WalletMoneyDepositedEvent {
                amount,
                new_balance,
            })
            .balance(new_balance)
            .build()
    }
}
```

`deposit` returns a *new* wallet; the old one is untouched, and its fields cannot be set in place.
That is what makes the two steps safe: the action builds values freely, the plan holds them, and
nothing is half written.

`deposit` is the only place this event is ever attached. That is the whole habit: events are made
inside the model, staged inside the action's `perform`, and saved by the framework.

**Time is UTC, and stored as given.** A model's dates come from a `Clock` you pass in
(`SystemClock` in production), as UTC. They are stored exactly as they are: the framework never
shifts a time. Keep the database in UTC as well.

What `#[model]` writes for you, and the rules a builder checks, are in
[Models and entities](/learn/models-and-entities/).

## 3. The table and its row type

The table is a migration, a SQL file with a Flyway-style name, run by your app at startup:

```sql
-- migrations/V0003__wallets.sql
CREATE TABLE wallets (
    id           UUID           PRIMARY KEY,
    version      BIGINT         NOT NULL,
    state        VARCHAR(24)    NOT NULL,
    owner_id     UUID           NOT NULL,
    currency     CHAR(3)        NOT NULL,
    balance      DECIMAL(19, 4) NOT NULL,
    created_date TIMESTAMP      NOT NULL,
    updated_date TIMESTAMP      NOT NULL
);
```

`V0001__eventlog.sql`, which creates `eventlog.events`, comes first. Copy it from an example.

The Rust types for your tables are generated when you build. `build.rs` starts a throwaway
database in Docker, applies your migrations to it, and writes one module from what it finds:

```rust
// build.rs
fn main() {
    ekbatan_schemagen::Schemagen::new(ekbatan::Dialect::Postgres)
        .migrations("migrations")
        .write_for_build()
        .expect("schema generated from the migrations");
}
```

```rust
// src/schema.rs
ekbatan::include_schema!();
```

That gives `Wallets`, the table and its columns, and `WalletsRecord`, one row. So **building
needs Docker**. The generated file is not committed; it is made again when a migration changes.
More in [Migrations](/reference/migrations/) and [Schema generation](/reference/schemagen/).

## 4. The repository

A repository implements `AbstractRepository`: where its databases are, how a row becomes a wallet,
and how a wallet becomes a row.

```rust
use std::sync::Arc;

use ekbatan::{
    AbstractRepository, DatabaseRegistry, DbUuid, Error, Model, Persistable, ShardedId,
    ShardingStrategy,
};

use crate::model::{Wallet, WalletBuilder, WalletState};
use crate::schema::{Wallets, WalletsRecord};

pub struct WalletRepository {
    registry: Arc<DatabaseRegistry>,
}

impl WalletRepository {
    pub fn new(registry: Arc<DatabaseRegistry>) -> Self {
        WalletRepository { registry }
    }
}

impl AbstractRepository for WalletRepository {
    type Persistable = Wallet;
    type Record = WalletsRecord;

    fn registry(&self) -> &DatabaseRegistry {
        &self.registry
    }

    fn sharding_strategy(&self) -> ShardingStrategy {
        ShardingStrategy::EmbeddedBits
    }

    fn from_record(record: WalletsRecord) -> Result<Wallet, Error> {
        let state = WalletState::parse(&record.state)
            .ok_or_else(|| Error::Mapping(format!("unknown wallet state {:?}", record.state)))?;
        WalletBuilder::new()
            .id(ShardedId::from_uuid(record.id.0))
            .version(record.version)
            .state(state)
            .owner_id(record.owner_id.0)
            .currency(record.currency)
            .balance(record.balance)
            .created_date(record.created_date.and_utc())
            .updated_date(record.updated_date.and_utc())
            .build()
    }

    fn to_record(wallet: &Wallet) -> WalletsRecord {
        WalletsRecord {
            id: DbUuid(wallet.id().uuid()),
            version: wallet.version(),
            state: wallet.state().as_str().to_owned(),
            owner_id: DbUuid(*wallet.owner_id()),
            currency: wallet.currency().clone(),
            balance: *wallet.balance(),
            created_date: wallet.created_date().naive_utc(),
            updated_date: wallet.updated_date().naive_utc(),
        }
    }
}
```

That is the whole repository. `get_by_id`, `find_by_id`, `find_all_where`, `count_where`,
`exists_by_id`, `add`, `update` and the rest come with it, for both APIs:
`ekbatan::blocking::Repository` and `ekbatan::aio::AsyncRepository` are given to every
`AbstractRepository`. A module that imports both must say which one it means.

`EmbeddedBits` means the wallet's id names its shard. With one database it changes nothing, and
the day you add a second, no wallet code changes. See [Adding a shard](/learn/sharding/).

The two mapping functions are always yours. `from_record` goes through the builder, so a row
that cannot make a valid wallet is refused when it is read, and an unknown state is an
`Error::Mapping`.

What you add are the questions your business asks:

**Blocking**

```rust
use ekbatan::blocking::Repository;
use rust_decimal::Decimal;
use sea_query::{Expr, ExprTrait, Query};

impl WalletRepository {
    /// Open wallets under `threshold`, from every shard's replica.
    pub fn find_open_with_balance_below(&self, threshold: Decimal) -> Result<Vec<Wallet>, Error> {
        let select = Query::select()
            .column(sea_query::Asterisk)
            .from(Wallets::Table)
            .and_where(Expr::col(Wallets::Balance).lt(threshold))
            .and_where(Expr::col(Wallets::State).eq(WalletState::Opened.as_str()))
            .to_owned();
        self.readonly_dbs()
            .fetch_all::<WalletsRecord>(&select)?
            .into_iter()
            .map(Self::from_record)
            .collect()
    }
}
```

**Async**

```rust
use ekbatan::aio::AsyncRepository;
use sea_query::{Asterisk, Expr, ExprTrait, Query};
use uuid::Uuid;

impl WalletRepository {
    /// An owner's wallets, from every shard's replica.
    pub async fn find_all_by_owner_id(&self, owner_id: Uuid) -> Result<Vec<Wallet>, Error> {
        let select = Query::select()
            .column(Asterisk)
            .from(Wallets::Table)
            .and_where(Expr::col(Wallets::OwnerId).eq(owner_id))
            .and_where(Expr::col(Wallets::State).ne(WalletState::Deleted.as_str()))
            .to_owned();
        let records = self
            .readonly_dbs()
            .fetch_all::<WalletsRecord>(&select)
            .await?;
        records.into_iter().map(Self::from_record).collect()
    }
}
```

Queries are built with sea-query, against the generated columns, so a renamed column is a compile
error. A query you write does not leave out deleted rows unless you say so, as the async one does.
See [Repository](/reference/repository/).

## 5. The action

The action does three things in `perform`: read, change, stage. **No database write happens
here.** The executor opens the transaction *after* `perform` returns.

**Blocking**

```rust
use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::blocking::{Action, Repository};
use ekbatan::{Error, ShardedId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::model::Wallet;
use crate::repository::WalletRepository;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub wallet_id: ShardedId<Wallet>,
    pub amount: Decimal,
    pub recipient: Option<String>,
}

pub struct WalletDepositMoneyAction {
    wallet_repository: Arc<WalletRepository>,
}

impl WalletDepositMoneyAction {
    pub fn new(wallet_repository: Arc<WalletRepository>) -> Self {
        WalletDepositMoneyAction { wallet_repository }
    }
}

impl Action for WalletDepositMoneyAction {
    type Params = Params;
    type Result = Wallet;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Wallet, Error> {
        let wallet = self.wallet_repository.get_by_id(&params.wallet_id)?;
        let updated = wallet.deposit(params.amount)?;
        plan.update(updated)
    }
}
```

**Async**

```rust
use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::aio::{AsyncAction, AsyncRepository};
use ekbatan::{Error, ShardedId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::model::Wallet;
use crate::repository::WalletRepository;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub wallet_id: ShardedId<Wallet>,
    pub amount: Decimal,
    pub recipient: Option<String>,
}

pub struct WalletDepositMoneyAction {
    wallet_repository: Arc<WalletRepository>,
}

impl WalletDepositMoneyAction {
    pub fn new(wallet_repository: Arc<WalletRepository>) -> Self {
        WalletDepositMoneyAction { wallet_repository }
    }
}

impl AsyncAction for WalletDepositMoneyAction {
    type Params = Params;
    type Result = Wallet;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Wallet, Error> {
        let wallet = self.wallet_repository.get_by_id(&params.wallet_id).await?;
        let updated = wallet.deposit(params.amount)?;
        plan.update(updated)
    }
}
```

`plan.update` **writes nothing.** It stages. Nothing finds this action by itself either: you
register it when you build the executor, as in the next step. See [Actions](/learn/actions/).

## 6. Run it

The caller can be an HTTP handler, a message handler, a scheduled job or a command-line tool. What
matters is that it goes through the executor. First the executor, built once at startup with
every action and repository:

**Blocking**

```rust
let sharding = ShardingConfig::single(database);
let registry = Arc::new(DatabaseRegistry::from_sharding_config(&sharding)?);
let wallets = Arc::new(WalletRepository::new(registry.clone()));
let executor = ActionExecutor::builder()
    .namespace("example.wallet")
    .database_registry(registry.clone())
    .action_registry(
        ActionRegistry::builder()
            .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
            .with_action(WalletDepositMoneyAction::new(wallets.clone()))
            .build(),
    )
    .repository_registry(
        RepositoryRegistry::builder()
            .with_repository(wallets.clone())
            .build(),
    )
    .build()?;
```

**Async**

```rust
let registry = Arc::new(DatabaseRegistry::from_sharding_config(sharding)?);
let wallets = Arc::new(WalletRepository::new(registry.clone()));
let executor = Arc::new(
    AsyncActionExecutor::builder()
        .namespace(config.namespace.clone().unwrap_or_default())
        .database_registry(registry.clone())
        .action_registry(
            ActionRegistry::builder()
                .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                .with_action(WalletDepositMoneyAction::new(wallets.clone()))
                .build(),
        )
        .repository_registry(
            RepositoryRegistry::builder()
                .with_repository(wallets.clone())
                .build(),
        )
        .build()?,
);
```

The blocking example builds its database settings in code; the REST one reads them from
`ekbatan.json`. Both run the migrations before this. [Wiring a service](/learn/wiring-a-service/)
covers that part.

Then the deposit:

**Blocking**

```rust
let deposited = executor.execute::<WalletDepositMoneyAction>(
    Some("alice"),
    wallet_deposit_money_action::Params {
        wallet_id: id,
        amount: Decimal::new(2550, 2),
        recipient: None,
    },
)?;
println!(
    "deposited 25.50: balance {} (version {})",
    deposited.balance(),
    deposited.version()
);
```

**Async**

```rust
// POST /wallets/{id}/deposits, in axum
async fn deposit(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<DepositRequest>,
) -> Result<Json<WalletResponse>, ApiError> {
    let deposited = state
        .executor
        .execute::<WalletDepositMoneyAction>(
            Some(PRINCIPAL),
            wallet_deposit_money_action::Params {
                wallet_id: ShardedId::from_uuid(id),
                amount: body.amount,
                recipient: body.recipient,
            },
        )
        .await?;
    Ok(Json(WalletResponse::from(&deposited)))
}
```

The REST example's real handler also takes a [keyed lock](/reference/keyed-locks/) on the wallet
around the call, so two deposits to one wallet wait for each other instead of one losing the race.
It is left out here to keep the shape plain.

`ApiError` is the example's own: it turns `Error::NotFound` into 404, `Error::Invalid` into 422
and a stale record into 409. Who is acting is your concern too: the executor takes a principal,
which in a real service comes from your login layer.

Do not call the blocking API from an async handler. It blocks the thread while it waits, and on a
single-threaded runtime it panics. An axum service uses `ekbatan::aio`.

To run the blocking example, with only Docker needed:

```bash
cargo run -p wallet-service-blocking-pg
```

```
no DATABASE_URL: starting a throwaway PostgreSQL in Docker
1. migrated: 4 migration file(s) applied
2. opened wallet 01a1a602-3a48-77b0-8000-6e42fb18d855 with 10.00 EUR (version 1)
3. deposited 25.50: balance 35.5000 (version 2)
4. read back: balance 35.5000, state OPENED, version 2
5. the wallet's events: WalletCreatedEvent, WalletMoneyDepositedEvent
6. closed: state CLOSED
7. a deposit of -5 is refused: Deposit amount must be positive
```

The balance reads back as `35.5000` because the column is `DECIMAL(19, 4)`.

## 7. Look at the event log

Against a database you keep (the example's README shows how), read the event table:

```sql
SELECT action_name, event_type, payload
FROM eventlog.events
ORDER BY event_date;
```

From a real run on PostgreSQL:

```text
 action_name              | event_type                | payload
--------------------------+---------------------------+--------------------------------------------
 WalletCreateAction       | WalletCreatedEvent        | {"modelId": "01a1a602-...", "ownerId": ...,
                          |                           |  "currency": "EUR", "modelName": "Wallet",
                          |                           |  "initialBalance": 10.00}
 WalletDepositMoneyAction | WalletMoneyDepositedEvent | {"amount": 25.50, "modelId": "01a1a602-...",
                          |                           |  "modelName": "Wallet", "newBalance": 35.5000}
 WalletCloseAction        | WalletClosedEvent         | {"modelId": "01a1a602-...",
                          |                           |  "modelName": "Wallet"}
```

Three actions, three rows. The refused deposit left none: it failed in `perform`, so no
transaction was opened. The `wallets` row shows the new balance, and its `version` went up by one
with each change. Each wallet row and its event row changed inside **one** transaction. Every
column is described in [`events`](/reference/tables/events/).

The payload was written as `modelId`, `modelName`, then the event's fields in order, camelCase,
with decimals as JSON numbers. PostgreSQL's `jsonb` stores the keys in its own order, which is why
they look shuffled above. The `action_params` column holds the deposit's params the same way,
`recipient` included.

## What just happened

- **One** database transaction touched two tables: an `UPDATE` of `wallets` and an `INSERT` into
  `eventlog.events`. There is no second commit you could forget.
- Your code never opened a transaction. The executor did, after `perform` returned.
- The event holds no infrastructure: no topic, no broker, no delivery flag. It is a business
  fact.
- The next deposit on this wallet uses the new version in its `WHERE version = ?`. A deposit
  that read the old version matches no row, fails with `Error::StaleRecord`, and the executor runs
  it again from the start. See [Actions](/learn/actions/).

Look at what you did not write: no `BEGIN`, no `COMMIT`, no publish, and no mention of the event
table anywhere.

## Test it without a database

An action's logic needs no database when it reads none. The `ekbatan-test-support` crate, in the
same repository, runs `perform` on a fresh plan and checks what it staged and emitted. Nothing is
written. Here with the wallet-opening action, which reads nothing:

**Blocking**

```rust
use ekbatan_test_support::blocking::ActionSpec;

let action = WalletCreateAction::new(Arc::new(SystemClock));
let owner = Uuid::new_v4();
ActionSpec::of(&action)
    .with_principal("alice")
    .execute(wallet_create_action::Params {
        country_code: None,
        owner_id: owner,
        currency: "EUR".to_owned(),
        initial_balance: Decimal::TEN,
    })
    .assert_added_one::<Wallet>(|wallet| assert_eq!(*wallet.balance(), Decimal::TEN))
    .assert_emitted_one::<WalletCreatedEvent>(|event| assert_eq!(event.owner_id, owner))
    .assert_no_updates();
```

**Async**

```rust
use ekbatan_test_support::aio::AsyncActionSpec;

let action = WalletCreateAction::new(Arc::new(SystemClock));
let owner = Uuid::new_v4();
AsyncActionSpec::of(&action)
    .with_principal("alice")
    .execute(wallet_create_action::Params {
        country_code: None,
        owner_id: owner,
        currency: "EUR".to_owned(),
        initial_balance: Decimal::TEN,
    })
    .await
    .assert_added_one::<Wallet>(|wallet| assert_eq!(*wallet.balance(), Decimal::TEN))
    .assert_emitted_one::<WalletCreatedEvent>(|event| assert_eq!(event.owner_id, owner))
    .assert_no_updates();
```

`execute_err` and `assert_fails_with::<E>` check a failure instead. A model's rules are plain
methods, so they test directly: `wallet.deposit(Decimal::NEGATIVE_ONE)` is an `Err`. More in
[Testing](/reference/testing/).

That is also the limit of it. **That state and events land *together* is a claim about a
transaction**, and only a test against a real database checks it. Each example has one, run with
`cargo test -p <example>`; it needs Docker.

## Next

**[Blocking or async?](/learn/blocking-or-async/)**: pick the API for your service. After that,
[Wiring a service](/learn/wiring-a-service/) builds everything from a config file, and
[Consuming events](/learn/consuming-events/) turns the event row you just saw into work.

## See also

- [Actions](/learn/actions/): the two steps, retries, and what an action may not do
- [The outbox](/learn/outbox/): what just got written, and why it is one table
- [Models and entities](/learn/models-and-entities/): immutable structs, versions, soft deletion
- Reference: [Action](/reference/action/), [Model](/reference/model/),
  [Repository](/reference/repository/), [`events`](/reference/tables/events/)
- `ekbatan-rs-examples/wallet-service-blocking-pg` and `ekbatan-rs-examples/axum-wallet-rest-pg`:
  the runnable projects this lesson is taken from, with copies of the REST one for MySQL and
  MariaDB, and examples for sharding, sagas and jobs
