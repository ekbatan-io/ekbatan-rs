# Model

A model is a saved domain object that emits events. Every change worth telling the rest of the system about - opened, deposited, closed - is attached to the new version of the model as an event, and written with the row when the action commits. #[model] turns a plain struct into one.

The same model serves blocking and async code: nothing on a model waits on the database.

## Declaring a model

```rust
use ekbatan::{Error, Instant, Model, ShardIdentifier, ShardedId, model, model_event};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalletState {
    Opened,
    Settled,
    Closed,
    Deleted,
}

#[model(id = ShardedId<Wallet>, state = WalletState)]
pub struct Wallet {
    pub owner_id: Uuid,
    pub currency: String,
    pub balance: Decimal,
}
```

`#[model(...)]` takes:

| argument | | |
|---|---|---|
| `id = ...` | required | the id type: `Id<Wallet>` for a plain UUID, or [`ShardedId<Wallet>`](/reference/sharding/) when the id names the row's shard |
| `state = ...` | required | the state type. Any `Copy + Eq + Debug + Send + Sync + 'static` type, in practice an enum of the model's lifecycle |
| `validate = path` | optional | a `fn(&Wallet) -> Result<(), Error>` that `build()` runs last. See [Validation](#validation) |

The struct must have named fields, no generic parameters, and no field called `base`. Each
field's type must be `Clone`, `Debug` and `PartialEq`.

## What `#[model]` writes

Rust has no base class to inherit fields from, so the macro writes them in:

- **A `base` field**, holding what every model has: id, state, version, created date, updated
  date, and the events of this version. It is private.
- **Your fields become private.** Each gets a getter with the field's own visibility, which
  returns a reference: `wallet.balance()` is a `&Decimal`. Nothing can change a field in place;
  see [Changing a model](#changing-a-model).
- **`#[derive(Clone, Debug, PartialEq)]`** on the struct. Don't derive these yourself: they
  would be derived twice.
- **A builder, `WalletBuilder`**, in the same module, with the struct's visibility.
- **`impl Persistable for Wallet`** and **`impl Model for Wallet`**.

The trait methods need their trait in scope: `use ekbatan::{Model, Persistable};`.

```rust
// ekbatan::Persistable (models and entities)
fn id(&self) -> &Self::Id;
fn version(&self) -> i64;
fn next_version(&self) -> Self;          // the same values at version + 1
fn is_model() -> bool;                   // true for a model
fn model_name() -> &'static str;         // "Wallet": the event rows' model_type

// ekbatan::Model
fn copy(&self) -> Self::Builder;         // a builder holding this model's values
fn state(&self) -> Self::State;
fn created_date(&self) -> Instant;
fn updated_date(&self) -> Instant;
fn events(&self) -> &[Arc<dyn ModelEvent<Self>>];
```

## Building one

A new model is built with its builder, usually in a function on the model:

```rust
impl Wallet {
    pub fn create_wallet(
        shard: ShardIdentifier,
        owner_id: Uuid,
        currency: &str,
        initial_balance: Decimal,
        created_date: Instant,
    ) -> WalletBuilder {
        WalletBuilder::new()
            .id(ShardedId::generate(shard))
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

// in an action:
let wallet = Wallet::create_wallet(shard, owner, "EUR", Decimal::ONE, self.clock.instant())
    .build()?;
```

`build()` returns `Result<Wallet, Error>`. It refuses, with `Error::Invalid` naming the field:

- any field not set, yours or the base's: `owner_id cannot be null`;
- a version below 1.

Then it runs your `validate` function, if you named one.

### Dates

- **`created_date` is required.** Pass it from the action's clock, `self.clock.instant()`, so a
  test with a frozen clock gets a known date.
- **`updated_date` defaults to `created_date`**: a row never changed was last updated when it was
  made.
- **Both are cut to whole microseconds**, because PostgreSQL, MySQL and MariaDB keep no finer
  detail. A date read back is then equal to the one written.
- Both are `Instant` (`chrono::DateTime<Utc>`), stored as given: nothing shifts their zone or
  value on the way in or out.

**Nothing moves `updated_date` but you.** Neither the plan nor the repository touches it on a
write. Set it in the changes where it means something:

```rust
pub fn deposit(&self, amount: Decimal, at: Instant) -> Result<Wallet, Error> {
    let new_balance = self.balance + amount;
    self.copy()
        .with_event(WalletMoneyDepositedEvent { amount, new_balance })
        .balance(new_balance)
        .updated_date(at)
        .build()
}
```

A row you never mark still reads as last updated when it was created.

**The version is different, and moves on its own**: the repository raises it inside the write,
under `WHERE version = ?`. That is the optimistic lock.

## Changing a model

A model never changes. A change makes a **new** value: `copy()` gives a builder holding every
value of this one, you set what changed, and `build()` checks it all again.

```rust
pub fn deposit(&self, amount: Decimal) -> Result<Wallet, Error> {
    if amount <= Decimal::ZERO {
        return Err(Error::Invalid("Deposit amount must be positive".to_owned()));
    }
    let new_balance = self.balance + amount;
    self.copy()
        .with_event(WalletMoneyDepositedEvent { amount, new_balance })
        .balance(new_balance)
        .build()
}
```

Setting a field in place does not compile:

```rust
wallet.balance = Decimal::TEN;   // error[E0616]: field `balance` of struct `Wallet` is private
wallet.base = other.base;        // error[E0616]: field `base` of struct `Wallet` is private
```

Inside the model's own module the fields are still reachable, because Rust's privacy is per
module. Methods there take `&self`, so they can't change them either.

## Events

`with_event(event)` attaches one to the new version. It takes `impl ModelEvent<Wallet>`, so
attaching another model's event is a compile error. The events go with the model into the plan,
and become [event rows](/reference/tables/events/) when the action commits. See
[ModelEvent](/reference/model-event/).

- A model **read from the database has no events**. Events are not a column.
- **`copy()` keeps the events already attached.** Two changes in a row carry both events, which
  is what you want when you compose them:
  `wallet.deposit(Decimal::TEN)?.deposit(Decimal::ONE)?`.
- So a value that has already been written can still carry its events: the result of
  `plan.update(...)`, or of `executor.execute(...)`. Stage it again in a later action and its old
  events are written again. Read the model fresh from its repository instead, or clear them with
  `.events(Vec::new())`.

## Optimistic locking

Every model has a `version`. Every update is written with `WHERE version = ?` and raises it by
one. If someone else saved first, no row matches, and the write fails with
`Error::StaleRecord`. In an action this rolls everything back, and the executor
[retries once](/reference/action-executor/#retries) by default.

**No row is ever locked to make a write.** Losing the race is caught by failing. When failing is
the wrong outcome, take a [keyed lock](/reference/keyed-locks/) first, so the second caller waits
and then succeeds.

## Soft deletion

To delete a model, change its state to a deleted state and stage the update, like any other
change, with an event if you want one. Every ready-made repository read skips rows whose `state`
column holds the text **`DELETED`**, so your state's text for it must be exactly that. A query you
write yourself does not skip them: add the condition. There is no delete method on the repository.

## Equality

The derived `PartialEq` compares the base and every field of yours. The base counts the id,
state, version and both dates. **Events don't count**: a row read back equals the value that was
written with events.

## Validation

`validate = path` names a function that checks the model's own rules, beyond its types:

```rust
#[model(id = ShardedId<Account>, state = AccountState, validate = Account::validate)]
pub struct Account {
    pub balance: Decimal,
}

impl Account {
    fn validate(&self) -> Result<(), Error> {
        if self.balance.is_sign_negative() {
            return Err(Error::Invalid("balance cannot be negative".to_owned()));
        }
        Ok(())
    }
}
```

`build()` runs it last, after its own checks. So it runs on every value made through the builder:
a new one, a `copy()...build()`, and a row read back, when your `from_record` uses the builder.
`next_version()` does not run it: the values are the same.

## Builder API

| | model | entity |
| --- | :-: | :-: |
| `new()` · `build()` | yes | yes |
| `id(...)` · `state(...)` | yes | yes |
| `version(...)` · `with_initial_version()` · `increase_version()` | yes | yes |
| `copy_base(&other)` | yes | yes |
| one setter per field: `balance(...)` | yes | yes |
| `created_date(...)` · `updated_date(...)` | yes | - |
| `with_event(...)` · `events(...)` | yes | - |

`with_initial_version()` sets version 1, for something new. `increase_version()` adds one to a
version already set. `copy_base(&other)` takes id, state, version, dates and events from another
value. `events(...)` replaces the attached events wholesale.

## See also

- [Entity](/reference/entity/) - the same, without events or dates
- [ModelEvent](/reference/model-event/) - what `with_event` takes
- [Repository](/reference/repository/) - how a model becomes a row and back
- [Learn: Models and entities](/learn/models-and-entities/)
