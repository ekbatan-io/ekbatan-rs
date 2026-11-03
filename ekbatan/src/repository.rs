use sea_query::{
    Asterisk, Condition, Expr, ExprTrait, Func, Iden, InsertStatement, Query, SelectStatement,
    UpdateStatement, Value, Values,
};
use uuid::Uuid;

use crate::aio::AsyncRepository;
use crate::runtime::block_on;
use crate::{
    DatabaseRegistry, Dialect, Error, Persistable, Record, ShardIdentifier, ShardKey,
    ShardingStrategy,
};
use crate::{Db, Dbs, Transaction};

/// A table, as schemagen writes it: its name and the columns the ready-made methods use.
pub trait Table: Iden + PartialEq + Copy + Send + Sync + 'static {
    const TABLE: Self;
    const ID: Self;
    const VERSION: Self;
    const STATE: Self;
    const COLUMNS: &'static [Self];
}

/// One row of a table, as schemagen writes it: read by sqlx, written as column-value pairs.
pub trait TableRecord: Record + 'static {
    type Table: Table;

    fn values(&self) -> Vec<(Self::Table, Value)>;
}

pub(crate) type TableOf<R> = <<R as AbstractRepository>::Record as TableRecord>::Table;
pub(crate) type IdOf<R> = <<R as AbstractRepository>::Persistable as Persistable>::Id;

/// `state <> 'DELETED'`: Java's private `notDeleted()`. Only the ready-made methods use it;
/// custom queries write their own condition when they want one.
pub(crate) fn not_deleted<T: Table>() -> Expr {
    Expr::col(T::STATE).ne("DELETED")
}

/// `SELECT * FROM <table>`: jOOQ's `selectFrom(table)`.
pub(crate) fn select_from<T: Table>() -> SelectStatement {
    Query::select()
        .columns(T::COLUMNS.iter().copied())
        .from(T::TABLE)
        .to_owned()
}

/// `SELECT 1 FROM <table>`, for `EXISTS`: jOOQ's `selectOne().from(table)`.
pub(crate) fn select_one_from<T: Table>() -> SelectStatement {
    Query::select().expr(Expr::val(1)).from(T::TABLE).to_owned()
}

/// `SELECT COUNT(*) FROM <table>`: jOOQ's `fetchCount(table, ...)`.
pub(crate) fn count_from<T: Table>() -> SelectStatement {
    Query::select()
        .expr(Func::count(Expr::col(Asterisk)))
        .from(T::TABLE)
        .to_owned()
}

/// One multi-row INSERT of every record: jOOQ's `insertInto(table, fields).values(...)...`.
pub(crate) fn insert_of<R: TableRecord>(records: &[R]) -> InsertStatement {
    let columns: Vec<_> = records[0]
        .values()
        .into_iter()
        .map(|(column, _)| column)
        .collect();
    let mut insert = Query::insert();
    insert.into_table(R::Table::TABLE).columns(columns);
    for record in records {
        insert.values_panic(
            record
                .values()
                .into_iter()
                .map(|(_, value)| Expr::val(value)),
        );
    }
    insert
}

/// `UPDATE <table> SET <every column>, version = <current> + 1 WHERE id = ? AND version =
/// <current>`: jOOQ's `update(table).set(record).where(id).and(version)`.
pub(crate) fn update_of<R: TableRecord>(
    record: &R,
    id: Uuid,
    current_version: i64,
) -> UpdateStatement {
    let table = R::Table::TABLE;
    let mut update = Query::update();
    update.table(table);
    for (column, value) in record.values() {
        if column == R::Table::VERSION {
            update.value(column, current_version + 1);
        } else {
            update.value(column, Expr::val(value));
        }
    }
    update
        .and_where(Expr::col(R::Table::ID).eq(id))
        .and_where(Expr::col(R::Table::VERSION).eq(current_version));
    update
}

/// One UPDATE for many rows, each matched on its id and version, as Java's `updateAll` writes it:
///
/// - PostgreSQL: `UPDATE t SET version = v.version + 1, c = v.c, ... FROM (VALUES (...), ...)
///   AS v (c, ...) WHERE t.id = v.id AND t.version = v.version RETURNING t.*`
/// - MySQL and MariaDB: `UPDATE t JOIN (SELECT ? AS c, ... UNION ALL SELECT ...) AS v ON t.id =
///   v.id AND t.version = v.version SET t.version = v.version + 1, t.c = v.c, ...`
///
/// Every value is a bound parameter. sea-query can't build either shape, so the text is written
/// here for the shard's database, as jOOQ writes it for each dialect.
pub(crate) fn update_all_sql<R: TableRecord>(dialect: Dialect, records: &[R]) -> (String, Values) {
    let quote = |name: String| match dialect {
        Dialect::Postgres => format!("\"{name}\""),
        Dialect::MySql | Dialect::MariaDb => format!("`{name}`"),
    };
    let rows: Vec<Vec<(R::Table, Value)>> = records.iter().map(TableRecord::values).collect();
    let columns: Vec<R::Table> = rows[0].iter().map(|(column, _)| *column).collect();
    let name = |column: R::Table| quote(column.to_string());
    let table = name(R::Table::TABLE);
    let (id, version) = (name(R::Table::ID), name(R::Table::VERSION));
    let others: Vec<R::Table> = columns
        .iter()
        .copied()
        .filter(|column| *column != R::Table::VERSION)
        .collect();
    let mut values = Vec::new();
    let mut next_placeholder = |value: Value| {
        values.push(value);
        match dialect {
            Dialect::Postgres => format!("${}", values.len()),
            Dialect::MySql | Dialect::MariaDb => "?".to_owned(),
        }
    };
    let sql = match dialect {
        Dialect::Postgres => {
            let tuples: Vec<String> = rows
                .into_iter()
                .map(|row| {
                    let placeholders: Vec<String> = row
                        .into_iter()
                        .map(|(_, value)| next_placeholder(value))
                        .collect();
                    format!("({})", placeholders.join(", "))
                })
                .collect();
            let column_list: Vec<String> = columns.iter().map(|column| name(*column)).collect();
            let sets: Vec<String> = others
                .iter()
                .map(|column| format!("{0} = \"v\".{0}", name(*column)))
                .collect();
            format!(
                "UPDATE {table} SET {version} = \"v\".{version} + 1, {sets} FROM (VALUES {tuples}) \
                 AS \"v\" ({columns}) WHERE {table}.{id} = \"v\".{id} AND {table}.{version} = \
                 \"v\".{version} RETURNING {table}.*",
                sets = sets.join(", "),
                tuples = tuples.join(", "),
                columns = column_list.join(", "),
            )
        }
        Dialect::MySql | Dialect::MariaDb => {
            let selects: Vec<String> = rows
                .into_iter()
                .map(|row| {
                    let fields: Vec<String> = row
                        .into_iter()
                        .map(|(column, value)| {
                            format!("{} AS {}", next_placeholder(value), name(column))
                        })
                        .collect();
                    format!("SELECT {}", fields.join(", "))
                })
                .collect();
            let sets: Vec<String> = others
                .iter()
                .map(|column| format!("{table}.{0} = `v`.{0}", name(*column)))
                .collect();
            format!(
                "UPDATE {table} JOIN ({selects}) AS `v` ON {table}.{id} = `v`.{id} AND \
                 {table}.{version} = `v`.{version} SET {table}.{version} = `v`.{version} + 1, \
                 {sets}",
                selects = selects.join(" UNION ALL "),
                sets = sets.join(", "),
            )
        }
    };
    (sql, Values(values))
}

/// Java's `AbstractRepository` (and its `ModelRepository` / `EntityRepository`, which differ
/// only in what they store). An implementation says where the databases are, how a row becomes
/// a model or entity and back (`from_record` / `to_record`, as in Java) and, if it shards, its
/// `sharding_strategy`; the selectors and the ready-made methods below work from that, with
/// Java's SQL.
pub trait AbstractRepository: Send + Sync {
    type Persistable: Persistable;
    type Record: TableRecord;

    fn registry(&self) -> &DatabaseRegistry;
    fn from_record(record: Self::Record) -> Result<Self::Persistable, Error>;
    fn to_record(persistable: &Self::Persistable) -> Self::Record;

    /// How ids name shards: `NoSharding` unless overridden, as Java's repositories default to
    /// `NoShardingStrategy`.
    fn sharding_strategy(&self) -> ShardingStrategy {
        ShardingStrategy::NoSharding
    }

    /// The shard a key names through this repository's strategy, or the default shard; then the
    /// registry's unknown-shard policy. Java's `effectiveShard(...)`.
    fn effective_shard(&self, key: impl ShardKey) -> Result<ShardIdentifier, Error> {
        let registry = self.registry();
        let shard = key
            .resolve(self.sharding_strategy())
            .unwrap_or_else(|| registry.default_shard());
        registry.effective_shard(shard)
    }

    /// The one shard all `models` are on: Java's `effectiveShard(Collection)`, which refuses
    /// models on two shards with `CrossShardException`.
    fn shard_of_all(&self, models: &[Self::Persistable]) -> Result<ShardIdentifier, Error> {
        if self.sharding_strategy() == ShardingStrategy::NoSharding {
            return Ok(self.registry().default_shard());
        }
        let mut first = None;
        for model in models {
            let shard = self.effective_shard(model.id())?;
            match first {
                None => first = Some(shard),
                Some(first) if first != shard => {
                    return Err(Error::CrossShard {
                        first,
                        second: shard,
                    });
                }
                Some(_) => {}
            }
        }
        Ok(first.unwrap_or_else(|| self.registry().default_shard()))
    }
}

/// Every repository's selectors and ready-made methods, with Java's SQL: Java's
/// `AbstractRepository` methods. Implemented for every `AbstractRepository`; the async twin is
/// `AsyncRepository`. A module that imports both traits must say which one it means.
pub trait Repository: AbstractRepository {
    // ---- The database selectors, for code that is not async (the async ones are on
    // ---- `AsyncRepository`). Each takes a shard key: `DefaultShard`, a `ShardIdentifier`, or
    // ---- an id, which covers Java's overloads.

    /// The primary: for writes, and reads that must be current. Java's `db(...)`.
    fn db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        AsyncRepository::db(self, key).map(Db::new)
    }

    /// Every shard's primary; only the default shard's under `NoSharding`. Java's `dbs()`.
    fn dbs(&self) -> Dbs<'_> {
        Dbs::new(AsyncRepository::dbs(self))
    }

    /// The replica: for reads that may lag. Java's `readonlyDb(...)`.
    fn readonly_db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        AsyncRepository::readonly_db(self, key).map(Db::new)
    }

    /// Every shard's replica. Java's `readonlyDbs()`.
    fn readonly_dbs(&self) -> Dbs<'_> {
        Dbs::new(AsyncRepository::readonly_dbs(self))
    }

    /// The caller's transaction, refused if it is on another shard than the key.
    fn tx_db<'a>(&'a self, tx: &'a mut Transaction, key: impl ShardKey) -> Result<Db<'a>, Error> {
        AsyncRepository::tx_db(self, tx.as_async(), key).map(Db::new)
    }

    /// The caller's transaction if given one, else the primary.
    fn tx_db_else_db<'a>(
        &'a self,
        tx: Option<&'a mut Transaction>,
        key: impl ShardKey,
    ) -> Result<Db<'a>, Error> {
        AsyncRepository::tx_db_else_db(self, tx.map(Transaction::as_async), key).map(Db::new)
    }

    fn add(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<Self::Persistable, Error> {
        block_on(AsyncRepository::add(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn add_no_result(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<(), Error> {
        block_on(AsyncRepository::add_no_result(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn add_all(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::add_all(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn add_all_no_result(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<(), Error> {
        block_on(AsyncRepository::add_all_no_result(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn update(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<Self::Persistable, Error> {
        block_on(AsyncRepository::update(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn update_no_result(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> Result<(), Error> {
        block_on(AsyncRepository::update_no_result(
            self,
            tx.map(Transaction::as_async),
            model,
        ))
    }

    fn update_all(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::update_all(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn update_all_no_result(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> Result<(), Error> {
        block_on(AsyncRepository::update_all_no_result(
            self,
            tx.map(Transaction::as_async),
            models,
        ))
    }

    fn find_by_id(&self, id: &IdOf<Self>) -> Result<Option<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_by_id(self, id))
    }

    fn get_by_id(&self, id: &IdOf<Self>) -> Result<Self::Persistable, Error> {
        block_on(AsyncRepository::get_by_id(self, id))
    }

    fn find_all_by_ids(&self, ids: &[IdOf<Self>]) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_all_by_ids(self, ids))
    }

    fn find_all(&self) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_all(self))
    }

    fn exists_by_id(&self, id: &IdOf<Self>) -> Result<bool, Error> {
        block_on(AsyncRepository::exists_by_id(self, id))
    }

    fn count(&self) -> Result<i64, Error> {
        block_on(AsyncRepository::count(self))
    }

    fn count_where(&self, condition: Condition) -> Result<i64, Error> {
        block_on(AsyncRepository::count_where(self, condition))
    }

    fn find_one_where(&self, condition: Condition) -> Result<Option<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_one_where(self, condition))
    }

    fn find_all_where(&self, condition: Condition) -> Result<Vec<Self::Persistable>, Error> {
        block_on(AsyncRepository::find_all_where(self, condition))
    }

    fn exists_where(&self, condition: Condition) -> Result<bool, Error> {
        block_on(AsyncRepository::exists_where(self, condition))
    }
}

impl<R: AbstractRepository + ?Sized> Repository for R {}
