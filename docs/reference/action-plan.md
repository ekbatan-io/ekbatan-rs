# ActionPlan

Where perform() stages its changes. The executor makes a new plan for each call, and for each retry, and hands it to perform(). After perform() returns, the executor writes what is on it.

## Type signature

```rust
// ekbatan::action::ActionPlan
impl ActionPlan {
    pub fn new() -> Self;

    pub fn add<E: Persistable>(&mut self, persistable: E) -> Result<E, Error>;
    pub fn add_all<E: Persistable>(&mut self, persistables: Vec<E>) -> Result<Vec<E>, Error>;
    pub fn update<E: Persistable>(&mut self, persistable: E) -> Result<E, Error>;
    pub fn update_all<E: Persistable>(&mut self, persistables: Vec<E>) -> Result<Vec<E>, Error>;

    pub fn additions<E: Persistable>(&self) -> &[E];
    pub fn updates<E: Persistable>(&self) -> &[E];
    pub fn has_changes(&self) -> bool;
    pub fn changes(&self) -> impl Iterator<Item = &dyn AnyChanges>;
    pub fn events(&self) -> Vec<&dyn AnyModelEvent>;
}
```

`Persistable` is what both a [model](/reference/model/) and an [entity](/reference/entity/)
are: `#[model]` and `#[entity]` implement it. The plan holds both kinds the same way. The only
difference shows when the executor writes: a model's events become event rows, and an entity has
none.

The staging methods are generic, so `plan.add(wallet)` gives back a `Wallet`, not something you
have to convert.

The plan is the same in both kinds of action. An `AsyncAction` stages on it exactly as an
`Action` does, and nothing on it is awaited.

## One writer at a time

`perform()` gets the plan as `&mut ActionPlan`, so Rust's borrow rules allow one writer at a
time. You can't hand the plan to several tasks or threads at once. If an action reads in
parallel, wait for all the reads, then stage from `perform()` itself.

## Staging

### `add(model)` / `add_all(models)`

Stages an INSERT. Returns the value **as given**. The plan fills in nothing: the id, the
version (`with_initial_version()`, which is 1) and the dates come from your builder.

### `update(model)` / `update_all(models)`

Stages an UPDATE. Pass the value at the version it was read at. `update` returns it at
**version + 1**, which is the version the row will have once written, so the action can return
it or keep using it.

`add_all` and `update_all` stage each value in order and return them in the same order. They
stop at the first error.

### One row, one change

Each id can be staged once per call, per type. A second `add` or `update` of the same id
returns `Error::Unexpected`:

```
Wallet 0193... is already registered for addition operation, cannot update. ...
```

The error stops the action, and nothing is written. Make all the changes to the value first, then
stage it once:

```rust
let wallet = wallet.deposit(Decimal::TEN)?.deposit(Decimal::ONE)?;
plan.update(wallet)?;
```

Staging twice can't mean two writes: the second write would need a version the database never
held.

## Reading the plan

### `additions::<E>()` / `updates::<E>()`

What was staged for one type, in staging order. An empty slice for a type nothing was staged
for. `updates` gives the values as staged, at the version they were read at, not version + 1.

### `has_changes()`

Whether anything was staged. A plan with nothing on it is not an error: the call still writes
one event row, as below.

### `changes()`

Every staged type, as `&dyn AnyChanges`, in the order the types were first staged. Each one
gives `model_name()`, `addition_count()`, `update_count()` and `events()`.

### `events()`

Every event carried by the staged models: for each type, its additions first, then its updates.
Entities add nothing here.

These are mostly for tests. [`ActionAssert`](/reference/testing/) is built on them.

## What the executor does with it

```
execute()
   +-- ActionPlan::new()
   +-- perform(&mut plan, ...)      stages on it
   +-- group the staged rows by shard
   +-- on each shard, in one transaction:
         per type: one multi-row INSERT for the additions,
                   one UPDATE for the updates (each row checked on its version)
         then the event rows
```

- Every staged type needs a repository in the executor's `RepositoryRegistry`. One with none is
  refused with `Error::Unexpected` ("No repository found for entity type: ..."), before any
  transaction opens.
- Types are written in the order they were first staged.
- If any update finds its row at another version, the whole shard's transaction rolls back with
  `Error::StaleRecord` (one row staged) or `Error::StaleRecords` (several). The default retry
  policy retries both.

A retry gets a **new** plan, so nothing from a failed attempt carries over.

A call that stages nothing still writes **one** event row, with no event in it, on the default
shard. So "this ran and found nothing to do" is in the log, and can be told apart from "this
never ran".

## See also

- [Action](/reference/action/) - what stages on the plan
- [ActionExecutor](/reference/action-executor/) - what writes it
- [`events`](/reference/tables/events/) - where the event rows go
