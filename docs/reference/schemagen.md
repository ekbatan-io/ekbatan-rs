# schemagen

Table definitions generated in build.rs from a migrated throwaway database. ekbatan-schemagen starts a database in Docker, runs your migrations into it, reads the tables back, and writes one Rust module: a column enum and a row struct per table. It comes from the real database, so it cannot drift from your migrations.

**Docker has to be running** to build a crate that uses it. There is no way around that: the
module always comes from a real database with your migrations applied.

## Wiring it in

The generator runs in your crate's `build.rs`, so every build matches its migrations:

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

The crates are not on crates.io yet; the examples take them by path:

```toml
# Cargo.toml
[build-dependencies]
ekbatan = { path = "../ekbatan" }
ekbatan-schemagen = { path = "../ekbatan-schemagen" }
```

The generated code names `sea_query`, `sqlx`, `rust_decimal` and `chrono` by their full paths, so
the crate needs those as ordinary dependencies, as the examples have them.

What `write_for_build()` does, in order:

1. starts a throwaway database in Docker (Testcontainers): PostgreSQL `17-alpine`, MySQL `8.4` or
   MariaDB `11.8`, or the tag you give with `.image_tag("...")`;
2. runs your migrations into it with the framework's own `Migrator` - the same code your service
   runs at startup;
3. reads every table of the default schema from `information_schema`;
4. maps each column to a Rust type;
5. writes `$OUT_DIR/schema.rs`, which `include_schema!()` pulls in;
6. tells cargo to run it again when a migration folder changes, and stops the container.

**The output is not committed.** It lives in cargo's build folder and is made again when a
migration changes. An editor that runs build scripts (rust-analyzer does) starts a container when
the migrations change, too.

If Docker is not running, the build fails with:

```
can't start the database (is Docker running?): ...
```

## What you get

For each table, a column enum for building SQL with sea-query, and a row struct that sqlx reads.
For the example's `wallets` table:

```rust
#[derive(::sea_query::Iden, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wallets {
    #[iden = "wallets"]
    Table,
    #[iden = "id"]
    Id,
    #[iden = "version"]
    Version,
    #[iden = "state"]
    State,
    #[iden = "owner_id"]
    OwnerId,
    #[iden = "currency"]
    Currency,
    #[iden = "balance"]
    Balance,
    #[iden = "created_date"]
    CreatedDate,
    #[iden = "updated_date"]
    UpdatedDate,
}

impl ::ekbatan::Table for Wallets { /* TABLE, ID, VERSION, STATE, COLUMNS */ }

#[derive(Clone, Debug, PartialEq, ::sqlx::FromRow)]
pub struct WalletsRecord {
    pub id: ::ekbatan::DbUuid,
    pub version: i64,
    pub state: ::std::string::String,
    pub owner_id: ::ekbatan::DbUuid,
    pub currency: ::std::string::String,
    pub balance: ::rust_decimal::Decimal,
    pub created_date: ::chrono::NaiveDateTime,
    pub updated_date: ::chrono::NaiveDateTime,
}

impl ::ekbatan::TableRecord for WalletsRecord { /* values(): each column with its value */ }
```

- The enum is named after the table in UpperCamelCase, with a `Table` variant for the table
  itself and one variant per column, in column order.
- The struct is `<Table>Record`, one field per column in snake_case. A nullable column is an
  `Option<...>`.
- `Table` and `TableRecord` - what a repository's ready-made methods need - are written **only for
  a table that has `id`, `version` and `state` columns**. Any other table gets the enum and the
  struct, for your own queries.
- A column named like a Rust keyword becomes a raw name (`r#type`); a column named `table`
  becomes the variant `TableColumn`. The struct keeps the column's real name for sqlx.

A repository then names these in its `AbstractRepository` implementation
(`type Record = WalletsRecord;`) and builds queries with `Wallets::Table`, `Wallets::OwnerId` and
so on. See [Repository](/reference/repository/).

## The default mapping

### PostgreSQL

| database type | Rust type |
| --- | --- |
| `uuid` | `ekbatan::DbUuid` |
| `bigint`, `integer`, `smallint` | `i64`, `i32`, `i16` |
| `boolean` | `bool` |
| `character varying`, `character`, `text` | `String` |
| `numeric` | `rust_decimal::Decimal` |
| `timestamp without time zone` | `chrono::NaiveDateTime` |
| `timestamp with time zone` | `chrono::DateTime<chrono::Utc>` |
| `date`, `time without time zone` | `chrono::NaiveDate`, `chrono::NaiveTime` |
| `double precision`, `real` | `f64`, `f32` |
| `bytea` | `Vec<u8>` |

### MySQL and MariaDB

| database type | Rust type |
| --- | --- |
| `char(36)` on a column named `id` or ending `_id` | `ekbatan::DbUuid` |
| `uuid` (MariaDB) | `ekbatan::DbUuid` |
| `bigint`, `int`, `smallint`, `tinyint` | `i64`, `i32`, `i16`, `i8`; `u64`, `u32`, `u16`, `u8` when `unsigned` |
| `tinyint(1)` | `bool` |
| `varchar`, `char`, `text`, `tinytext`, `mediumtext`, `longtext`, `enum` | `String` |
| `decimal` | `rust_decimal::Decimal` |
| `datetime`, `timestamp` | `chrono::NaiveDateTime` |
| `date`, `time` | `chrono::NaiveDate`, `chrono::NaiveTime` |
| `double`, `float` | `f64`, `f32` |
| `blob`, `tinyblob`, `mediumblob`, `longblob`, `binary`, `varbinary` | `Vec<u8>` |

**A `CHAR(36)` is a uuid only on `id` and `*_id` columns**, the same rule as Ekbatan for Java's
build. Anywhere else it is text. To read another `CHAR(36)` column as a uuid, map the `String`
yourself in `from_record`.

`DbUuid` reads a uuid the way each database keeps it: PostgreSQL's `uuid`, MySQL's `CHAR(36)`
text, and MariaDB's `UUID`. So the example's migrations give **the same module, byte for byte,**
from PostgreSQL, MySQL and MariaDB (`ekbatan-schemagen/tests/generate.rs`), and one schema serves
every database. The example generates from PostgreSQL.

**Times are `NaiveDateTime`**: the database's zone-less column, holding UTC digits. The repository
turns them into the model's UTC `Instant` (`record.created_date.and_utc()`) and back
(`wallet.created_date().naive_utc()`). See [Always UTC](/reference/multi-database/#always-utc).

**A type not in these tables stops the build**, naming the column:

```
audit.payload has type jsonb, which schemagen has no Rust type for: add a ForcedType for it
```

JSON columns are among them: there is no Rust type for them by default.

## Settings

| | |
| --- | --- |
| `Schemagen::new(dialect)` | which database to generate from: `Dialect::Postgres`, `MySql` or `MariaDb` |
| `.migrations(location)` | a migration folder, relative to the crate, or `embed_migrations!(...)`. Call it more than once for several; they are applied together. At least one |
| `.image_tag("16-alpine")` | another image tag for the throwaway database |
| `.includes("wallets\|notifications")` | only tables whose name matches. A regular expression, matched against the whole name, any letter case |
| `.excludes("tmp_.*")` | leave out tables whose name matches |
| `.forced_type(ForcedType::new(...))` | a column's Rust type, chosen by you - below |
| `.write_for_build()` | from `build.rs`: write `$OUT_DIR/schema.rs` |
| `.write_to(path)` | write the module somewhere else |
| `.generate()` / `.generate_async()` | the module as a `String`. `generate()` runs its own runtime, so call it from code that is not async |

## Forced types

A `ForcedType` picks the Rust type of the columns it matches, before the default mapping is
looked at. It matches on two regular expressions, **both** of which must match the whole text, in
any letter case:

- `include_expression`: `table.column`;
- `include_types`: the database type as `information_schema` gives it - PostgreSQL's `data_type`
  (`numeric`, `jsonb`), MySQL's and MariaDB's `column_type` (`char(36)`,
  `bigint(20) unsigned`).

Either left out matches everything. The first forced type that matches wins.

```rust
use ekbatan_schemagen::{ForcedType, Schemagen};

Schemagen::new(ekbatan::Dialect::Postgres)
    .migrations("migrations")
    .forced_type(
        ForcedType::new("crate::money::Cents")
            .include_expression(r"payments\.amount_cents")
            .include_types("bigint"),
    )
    .write_for_build()
    .expect("schema generated from the migrations");
```

The type is written into the module as given, so name it by a path that resolves from where the
module is included. It must be a type that sqlx reads on **both** drivers (the row struct derives
`sqlx::FromRow` and is read from PostgreSQL and MySQL rows alike), and that converts into a
sea-query `Value` with `.into()`.

## The framework's own tables

Left out of the module: `scheduled_tasks`, the migration history (`_sqlx_migrations`, and
`flyway_schema_history` if one is there), and everything outside the default schema - which
leaves out `eventlog.events` and `eventlog.event_notifications`. The framework reads and writes
those itself; your code does not need their definitions.

## See also

- [Migrations](/reference/migrations/) - the files schemagen applies
- [Repository](/reference/repository/) - where the record and the model meet
- [Multi-database](/reference/multi-database/#column-type-cheatsheet) - the column types to write
  in a migration
