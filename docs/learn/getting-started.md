# Getting started

Start here. Follow one wallet change through the dual-write problem, model events, action staging, and what the executor commits.

ekbatan is for a service that saves a business change and also needs that change as an event.
The event might be handled inside the same service, or sent on later to Kafka, RabbitMQ or another
broker.

The examples use a wallet. A wallet is an account with a balance. A deposit adds money to the
balance. The example is small on purpose, so the flow of state and events is easy to see.

Read the page in order. It starts with what goes wrong, then the usual fix, then how ekbatan
writes that fix with models, actions and an executor.

## 1. The problem

Many event-driven services start like this:

```text
request
  -> write row to database
  -> publish event to Kafka / RabbitMQ / ...
```

That is two writes to two systems. If the database commit succeeds and the publish fails, the
database says the change happened but nobody downstream hears about it. If the publish succeeds
and the commit fails, other services react to a change that never happened.

That is the dual-write problem.

```text
two writes, two systems

  app --> database   (state saved)
  app --> broker     (publish failed)
              |
          consumer   (no events)

a crash between the writes: the database and the broker disagree
```

If an event must describe what the database holds, it has to be saved with the database change.
A separate publish can fail on its own.

The longer version is [The dual-write trap](/learn/the-dual-write-trap/).

## 2. The fix

The outbox pattern stores the event in the database first. An outbox is a table of committed
events that something reads and delivers later.

The request becomes:

```text
request
  -> one database transaction
       -> write state row
       -> write event row
  -> commit
  -> deliver later, by reading the event row
```

The database transaction is the boundary. Either both rows commit, or neither does.

```text
one write, plus the outbox

  app
   |
  database, one transaction:  [ state ] [ events ]
   |
  broker   (later, by reading the events table)
   |
  consumer
```

The longer version is [The outbox](/learn/outbox/).

## 3. Where the event comes from

In ekbatan, a business object whose changes are events is a **model**. When money is deposited,
the wallet does not change in place. Its `deposit` method returns a new wallet with the new
balance and an event that says what happened:

```rust
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

`#[model]` writes the rest for you: the id, state, version and dates every model has, a getter
for each field, and the builder that `copy()` returns. The two names in the brackets are the
wallet's id type and the enum of states a wallet can be in.

The event is not a Kafka message. It is a plain fact: "money was deposited into this wallet". It
is made next to the change it describes, before anything is saved or sent.

More in [Models and entities](/learn/models-and-entities/).

## 4. What an action does

An **action** is one unit of business work. The deposit action reads the wallet, asks it for the
new wallet, and stages the result on the **plan**: a list of what the executor should save once
the action is done.

**Blocking**

```rust
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

`principal` is who is acting, such as a user id, or `None`. `params` is what the caller passed in.

Samples that differ come twice. **Blocking** is the blocking API, `ekbatan::blocking`: plain
functions. **Async** is `ekbatan::aio`, where database calls are awaited. Both run the
same engine with the same rules. [Blocking or async?](/learn/blocking-or-async/) helps you
choose.

What does **not** happen in `perform`:

- no transaction is opened
- no `INSERT INTO eventlog.events`
- no publish to a broker
- no second write to remember

`perform` only says what should change. The executor then saves the staged wallet and its event
together, in one transaction.

More in [Actions](/learn/actions/).

## 5. What the executor commits

The caller runs the action through the executor:

**Blocking**

```rust
let wallet = executor.execute::<WalletDepositMoneyAction>(
    Some("alice"),
    wallet_deposit_money_action::Params {
        wallet_id: id,
        amount: Decimal::new(2550, 2),
        recipient: None,
    },
)?;
```

**Async**

```rust
let wallet = executor
    .execute::<WalletDepositMoneyAction>(
        Some("alice"),
        wallet_deposit_money_action::Params {
            wallet_id: id,
            amount: Decimal::new(2550, 2),
            recipient: None,
        },
    )
    .await?;
```

The executor makes a fresh plan, calls `perform`, and only then opens the transaction that
writes everything:

```text
executor.execute(...)
        |
        v
perform(...)
  - read the wallet
  - build the new wallet
  - attach WalletMoneyDepositedEvent
  - plan.update(new wallet)
        |
        v
one database transaction
  - UPDATE wallets ... WHERE id = ? AND version = ?
  - INSERT INTO eventlog.events
  - commit
```

If another request changed the same wallet first, the update matches no row, and the whole
transaction rolls back. If the event row cannot be written, the wallet row rolls back too.

The outbox guarantee comes from this commit path: the event row is written in the same
transaction as the row it describes.

API details: [ActionExecutor](/reference/action-executor/).

## 6. What happens after commit

After the commit, `eventlog.events` holds a row that will not go away. You choose how to use it:

```text
eventlog.events
      |
      +--> local event handler (your code, in your service, run by jobs on the database)
      |
      +--> a handler of yours that publishes to Kafka / RabbitMQ / ...
      |
      +--> a change-data-capture tool that follows the table
```

Each of these starts from the committed row. They run after the transaction, so they cannot leave
the original change half saved.

Each also delivers **at least once**: after a crash or a retry, a handler can see the same event
twice. Make handling a repeat harmless, for example with the event's id or a unique constraint.

ekbatan's job is to save events with the state they describe. Delivering them is a separate step
that reads the saved rows. See [Consuming events](/learn/consuming-events/).

## 7. Where the rest fits

The rest of the framework builds on the same few pieces:

- **Repositories** turn rows into models and back. You write `from_record` and `to_record`;
  reads and writes come ready-made. The row types are generated from your migrations when you
  build.
- **Models** emit events; **entities** do not.
- **Actions** stage new and changed objects.
- **The executor** commits staged rows and their events together.
- **The local event handler** runs your code for events after they commit.
- **Sharding** spreads the same models over several databases, with each id naming its own.
- **Sagas** split a long workflow into several committed actions.
- **Two APIs**, `ekbatan::blocking` and `ekbatan::aio`, follow the same rules.

You do not need all of it on day one. The rule that matters stays the same:

```text
business state + event row commit together
```

ekbatan-rs runs on PostgreSQL, MySQL and MariaDB. It is not on crates.io yet.

## Suggested next steps

To keep learning the model:

1. [The dual-write trap](/learn/the-dual-write-trap/): the failure ekbatan removes.
2. [The outbox](/learn/outbox/): why state and event rows commit together.
3. [Models and entities](/learn/models-and-entities/): which objects emit events and which
   don't.
4. [Actions](/learn/actions/): the two steps of every action.
5. [Your first action](/learn/your-first-action/): the wallet deposit end to end, and the event
   row it leaves.

To build a real service:

- [Blocking or async?](/learn/blocking-or-async/): which of the two APIs to use.
- [Wiring a service](/learn/wiring-a-service/): config file, migrations, schema, pools, and
  shutting down.

To see it run, the `wallet-service-blocking-pg` example in `ekbatan-rs-examples/` walks one
wallet through its life. It needs only Docker:

```bash
cargo run -p wallet-service-blocking-pg
```
