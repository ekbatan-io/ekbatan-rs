use std::collections::BTreeSet;
use std::future::Future;

use sea_query::extension::postgres::PgFunc;
use sea_query::{
    ArrayType, Asterisk, Condition, Expr, ExprTrait, Func, Iden, InsertStatement, Query,
    SelectStatement, UpdateStatement, Value, Values,
};
use uuid::Uuid;

use crate::{
    DatabaseRegistry, Db, Dbs, Dialect, Error, ModelId, Persistable, Record, ShardIdentifier,
    ShardKey, ShardingStrategy, Transaction,
};

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

type TableOf<R> = <<R as Repository>::Record as TableRecord>::Table;
type IdOf<R> = <<R as Repository>::Persistable as Persistable>::Id;

/// `state <> 'DELETED'`: Java's private `notDeleted()`. Only the ready-made methods use it;
/// custom queries write their own condition when they want one.
fn not_deleted<T: Table>() -> Expr {
    Expr::col(T::STATE).ne("DELETED")
}

/// `SELECT * FROM <table>`: jOOQ's `selectFrom(table)`.
fn select_from<T: Table>() -> SelectStatement {
    Query::select()
        .columns(T::COLUMNS.iter().copied())
        .from(T::TABLE)
        .to_owned()
}

/// `SELECT 1 FROM <table>`, for `EXISTS`: jOOQ's `selectOne().from(table)`.
fn select_one_from<T: Table>() -> SelectStatement {
    Query::select().expr(Expr::val(1)).from(T::TABLE).to_owned()
}

/// `SELECT COUNT(*) FROM <table>`: jOOQ's `fetchCount(table, ...)`.
fn count_from<T: Table>() -> SelectStatement {
    Query::select()
        .expr(Func::count(Expr::col(Asterisk)))
        .from(T::TABLE)
        .to_owned()
}

/// One multi-row INSERT of every record: jOOQ's `insertInto(table, fields).values(...)...`.
fn insert_of<R: TableRecord>(records: &[R]) -> InsertStatement {
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
fn update_of<R: TableRecord>(record: &R, id: Uuid, current_version: i64) -> UpdateStatement {
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
fn update_all_sql<R: TableRecord>(dialect: Dialect, records: &[R]) -> (String, Values) {
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
pub trait Repository: Send + Sync {
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

    // ---- The database selectors. Each takes a shard key: `DefaultShard`, a `ShardIdentifier`,
    // ---- or an id, which covers Java's overloads.

    /// The primary: for writes, and reads that must be current. Java's `db(...)`.
    fn db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        self.registry().primary(self.effective_shard(key)?)
    }

    /// Every shard's primary; only the default shard's under `NoSharding`. Java's `dbs()`.
    fn dbs(&self) -> Dbs<'_> {
        let registry = self.registry();
        match self.sharding_strategy() {
            ShardingStrategy::NoSharding => Dbs(registry
                .primary(registry.default_shard())
                .into_iter()
                .collect()),
            ShardingStrategy::EmbeddedBits => registry.primaries(),
        }
    }

    /// The replica: for reads that may lag. Java's `readonlyDb(...)`.
    fn readonly_db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        self.registry().secondary(self.effective_shard(key)?)
    }

    /// Every shard's replica; only the default shard's under `NoSharding`. Java's
    /// `readonlyDbs()`.
    fn readonly_dbs(&self) -> Dbs<'_> {
        let registry = self.registry();
        match self.sharding_strategy() {
            ShardingStrategy::NoSharding => Dbs(registry
                .secondary(registry.default_shard())
                .into_iter()
                .collect()),
            ShardingStrategy::EmbeddedBits => registry.secondaries(),
        }
    }

    /// The caller's transaction, refused if it is on another shard than the key. Java's
    /// `txDb(...)`, which returns an empty `Optional` instead.
    fn tx_db<'a>(&'a self, tx: &'a mut Transaction, key: impl ShardKey) -> Result<Db<'a>, Error> {
        let shard = self.effective_shard(key)?;
        if tx.shard() != shard {
            return Err(Error::WrongShard {
                transaction: tx.shard(),
                row: shard,
            });
        }
        Ok(tx.db())
    }

    /// The caller's transaction if given one, else the primary. Java's `txDbElseDb(...)`.
    fn tx_db_else_db<'a>(
        &'a self,
        tx: Option<&'a mut Transaction>,
        key: impl ShardKey,
    ) -> Result<Db<'a>, Error> {
        match tx {
            Some(tx) => self.tx_db(tx, key),
            None => self.db(key),
        }
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

    // ---- Writes. Each joins `tx` when given one, as Java's do through `txDbElseDb`.

    /// INSERT, giving back the row as the database wrote it (`RETURNING`); on MySQL, which has
    /// no `RETURNING`, the model as given. Java's `add`.
    fn add(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<Self::Persistable, Error>> + Send {
        async move {
            let insert = insert_of(&[Self::to_record(model)]);
            let mut db = self.tx_db_else_db(tx, model.id())?;
            if db.dialect() == Dialect::MySql {
                db.execute(&insert).await?;
                return Ok(model.clone());
            }
            let mut returned = db.insert_returning::<Self::Record>(&insert).await?;
            match returned.pop() {
                Some(record) if returned.is_empty() => Self::from_record(record),
                _ => Err(Error::Unexpected(format!(
                    "Failed to insert {} with ID: {}",
                    TableOf::<Self>::TABLE.to_string(),
                    model.id().uuid()
                ))),
            }
        }
    }

    /// INSERT, giving back nothing. Java's `addNoResult`.
    fn add_no_result(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            let insert = insert_of(&[Self::to_record(model)]);
            self.tx_db_else_db(tx, model.id())?.execute(&insert).await?;
            Ok(())
        }
    }

    /// One multi-row INSERT for every model, all on one shard. Java's `addAll`.
    fn add_all(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<Vec<Self::Persistable>, Error>> + Send {
        async move {
            match models {
                [] => return Ok(Vec::new()),
                [model] => return Ok(vec![self.add(tx, model).await?]),
                _ => {}
            }
            let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
            let shard = self.shard_of_all(models)?;
            let insert = insert_of(&records);
            let mut db = self.tx_db_else_db(tx, shard)?;
            if db.dialect() == Dialect::MySql {
                db.execute(&insert).await?;
                return Ok(models.to_vec());
            }
            let returned = db.insert_returning::<Self::Record>(&insert).await?;
            if returned.len() != models.len() {
                return Err(Error::Unexpected(format!(
                    "{} {} were queried for insert, while {} rows were inserted",
                    models.len(),
                    TableOf::<Self>::TABLE.to_string(),
                    returned.len()
                )));
            }
            returned.into_iter().map(Self::from_record).collect()
        }
    }

    /// One multi-row INSERT, giving back nothing. Java's `addAllNoResult`.
    fn add_all_no_result(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            if models.is_empty() {
                return Ok(());
            }
            let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
            let shard = self.shard_of_all(models)?;
            self.tx_db_else_db(tx, shard)?
                .execute(&insert_of(&records))
                .await?;
            Ok(())
        }
    }

    /// UPDATE of every column, raising the version, only if the row is still at the model's
    /// version (Java's optimistic lock). Gives back the row as the database wrote it
    /// (`RETURNING`); where UPDATE has no `RETURNING` (MySQL, MariaDB), the model's next version.
    /// Java's `update`.
    fn update(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<Self::Persistable, Error>> + Send {
        async move {
            let stale = || Error::StaleRecord {
                table: TableOf::<Self>::TABLE.to_string(),
                id: model.id().uuid().to_string(),
                version: model.version(),
            };
            let update = update_of(&Self::to_record(model), model.id().uuid(), model.version());
            let mut db = self.tx_db_else_db(tx, model.id())?;
            if db.dialect() == Dialect::Postgres {
                let returned = db
                    .fetch_optional::<Self::Record, _>(&update.clone().returning_all().to_owned())
                    .await?;
                return returned.map_or_else(|| Err(stale()), Self::from_record);
            }
            if db.execute(&update).await? == 0 {
                return Err(stale());
            }
            Ok(model.next_version())
        }
    }

    /// UPDATE with the version check, giving back nothing. Java's `updateNoResult`.
    fn update_no_result(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            let update = update_of(&Self::to_record(model), model.id().uuid(), model.version());
            let affected = self.tx_db_else_db(tx, model.id())?.execute(&update).await?;
            match affected {
                1 => Ok(()),
                0 => Err(Error::StaleRecord {
                    table: TableOf::<Self>::TABLE.to_string(),
                    id: model.id().uuid().to_string(),
                    version: model.version(),
                }),
                _ => Err(Error::Unexpected(
                    "More than one row was updated, which was not expected".to_owned(),
                )),
            }
        }
    }

    /// One UPDATE for every model, all on one shard, each matched on its id and version (see
    /// `update_all_sql`). If fewer rows matched than models, some were saved by someone else
    /// first. Java's `updateAll`.
    fn update_all(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<Vec<Self::Persistable>, Error>> + Send {
        async move {
            match models {
                [] => return Ok(Vec::new()),
                [model] => return Ok(vec![self.update(tx, model).await?]),
                _ => {}
            }
            let stale = |updated: usize| Error::StaleRecords {
                table: TableOf::<Self>::TABLE.to_string(),
                expected: models.len(),
                updated,
            };
            let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
            let shard = self.shard_of_all(models)?;
            let mut db = self.tx_db_else_db(tx, shard)?;
            let (sql, values) = update_all_sql(db.dialect(), &records);
            if db.dialect() == Dialect::Postgres {
                let returned = db.fetch_all_sql::<Self::Record>(sql, values).await?;
                if returned.len() != models.len() {
                    return Err(stale(returned.len()));
                }
                return returned.into_iter().map(Self::from_record).collect();
            }
            let affected = db.execute_sql(sql, values).await? as usize;
            if affected < models.len() {
                return Err(stale(affected));
            }
            Ok(models.iter().map(Persistable::next_version).collect())
        }
    }

    /// One UPDATE for every model, giving back nothing. Java's `updateAllNoResult`.
    fn update_all_no_result(
        &self,
        tx: Option<&mut Transaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            match models {
                [] => return Ok(()),
                [model] => return self.update_no_result(tx, model).await,
                _ => {}
            }
            let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
            let shard = self.shard_of_all(models)?;
            let mut db = self.tx_db_else_db(tx, shard)?;
            let (mut sql, values) = update_all_sql(db.dialect(), &records);
            if db.dialect() == Dialect::Postgres {
                let returning = sql.rfind(" RETURNING ").expect("written above");
                sql.truncate(returning);
            }
            let affected = db.execute_sql(sql, values).await? as usize;
            if affected != models.len() {
                return Err(Error::StaleRecords {
                    table: TableOf::<Self>::TABLE.to_string(),
                    expected: models.len(),
                    updated: affected,
                });
            }
            Ok(())
        }
    }

    // ---- Reads. Each skips soft-deleted rows; by id on the id's shard, otherwise every shard
    // ---- in turn, as Java's do.

    /// Java's `findById`.
    fn find_by_id(
        &self,
        id: &IdOf<Self>,
    ) -> impl Future<Output = Result<Option<Self::Persistable>, Error>> + Send {
        async move {
            let select = select_from::<TableOf<Self>>()
                .and_where(Expr::col(TableOf::<Self>::ID).eq(id.uuid()))
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            let record = self
                .db(id)?
                .fetch_optional::<Self::Record, _>(&select)
                .await?;
            record.map(Self::from_record).transpose()
        }
    }

    /// `find_by_id`, failing when there is none: Java's `getById`.
    fn get_by_id(
        &self,
        id: &IdOf<Self>,
    ) -> impl Future<Output = Result<Self::Persistable, Error>> + Send {
        async move {
            self.find_by_id(id).await?.ok_or_else(|| Error::NotFound {
                table: TableOf::<Self>::TABLE.to_string(),
                id: id.uuid().to_string(),
            })
        }
    }

    /// The ids grouped by shard, one query per shard: `id = ANY(?)` with one array parameter on
    /// PostgreSQL, `id IN (...)` on MySQL and MariaDB. Java's `findAllByIds`.
    fn find_all_by_ids(
        &self,
        ids: &[IdOf<Self>],
    ) -> impl Future<Output = Result<Vec<Self::Persistable>, Error>> + Send {
        async move {
            let mut seen = BTreeSet::new();
            let mut by_shard: Vec<(ShardIdentifier, Vec<Uuid>)> = Vec::new();
            for id in ids {
                if !seen.insert(id.uuid()) {
                    continue;
                }
                let shard = self.effective_shard(id)?;
                match by_shard.iter_mut().find(|(known, _)| *known == shard) {
                    Some((_, uuids)) => uuids.push(id.uuid()),
                    None => by_shard.push((shard, vec![id.uuid()])),
                }
            }
            let mut models = Vec::new();
            for (shard, uuids) in by_shard {
                let mut db = self.db(shard)?;
                let id = Expr::col(TableOf::<Self>::ID);
                let matches = match db.dialect() {
                    Dialect::Postgres => {
                        let array = uuids.into_iter().map(Value::from).collect();
                        id.eq(PgFunc::any(Expr::val(Value::Array(
                            ArrayType::Uuid,
                            Some(Box::new(array)),
                        ))))
                    }
                    Dialect::MySql | Dialect::MariaDb => id.is_in(uuids),
                };
                let select = select_from::<TableOf<Self>>()
                    .and_where(matches)
                    .and_where(not_deleted::<TableOf<Self>>())
                    .to_owned();
                for record in db.fetch_all::<Self::Record, _>(&select).await? {
                    models.push(Self::from_record(record)?);
                }
            }
            Ok(models)
        }
    }

    /// Every row of every shard. Java's `findAll`.
    fn find_all(&self) -> impl Future<Output = Result<Vec<Self::Persistable>, Error>> + Send {
        async move {
            let select = select_from::<TableOf<Self>>()
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            let mut models = Vec::new();
            for mut db in self.dbs().into_vec() {
                for record in db.fetch_all::<Self::Record, _>(&select).await? {
                    models.push(Self::from_record(record)?);
                }
            }
            Ok(models)
        }
    }

    /// `SELECT EXISTS(SELECT 1 ... WHERE id = ?)` on the id's shard. Java's `existsById`.
    fn exists_by_id(&self, id: &IdOf<Self>) -> impl Future<Output = Result<bool, Error>> + Send {
        async move {
            let select = select_one_from::<TableOf<Self>>()
                .and_where(Expr::col(TableOf::<Self>::ID).eq(id.uuid()))
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            self.db(id)?.fetch_exists(&select).await
        }
    }

    /// Rows on every shard, added up. Java's `count`.
    fn count(&self) -> impl Future<Output = Result<i64, Error>> + Send {
        self.count_where(Condition::all())
    }

    /// Matching rows on every shard, added up. Java's `countWhere`.
    fn count_where(&self, condition: Condition) -> impl Future<Output = Result<i64, Error>> + Send {
        async move {
            let count = count_from::<TableOf<Self>>()
                .cond_where(condition)
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            let mut total = 0;
            for mut db in self.dbs().into_vec() {
                total += db.fetch_scalar::<i64>(&count).await?;
            }
            Ok(total)
        }
    }

    /// The matching row of the first shard that has one; more than one on a shard is an error,
    /// as jOOQ's `fetchOne` makes it. Java's `findOneWhere`.
    fn find_one_where(
        &self,
        condition: Condition,
    ) -> impl Future<Output = Result<Option<Self::Persistable>, Error>> + Send {
        async move {
            let select = select_from::<TableOf<Self>>()
                .cond_where(condition)
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            for mut db in self.dbs().into_vec() {
                let mut records = db.fetch_all::<Self::Record, _>(&select).await?;
                if records.len() > 1 {
                    return Err(Error::TooManyRows {
                        table: TableOf::<Self>::TABLE.to_string(),
                    });
                }
                if let Some(record) = records.pop() {
                    return Self::from_record(record).map(Some);
                }
            }
            Ok(None)
        }
    }

    /// Matching rows of every shard. Java's `findAllWhere`.
    fn find_all_where(
        &self,
        condition: Condition,
    ) -> impl Future<Output = Result<Vec<Self::Persistable>, Error>> + Send {
        async move {
            let select = select_from::<TableOf<Self>>()
                .cond_where(condition)
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            let mut models = Vec::new();
            for mut db in self.dbs().into_vec() {
                for record in db.fetch_all::<Self::Record, _>(&select).await? {
                    models.push(Self::from_record(record)?);
                }
            }
            Ok(models)
        }
    }

    /// Whether any shard has a matching row, stopping at the first. Java's `existsWhere`.
    fn exists_where(
        &self,
        condition: Condition,
    ) -> impl Future<Output = Result<bool, Error>> + Send {
        async move {
            let select = select_one_from::<TableOf<Self>>()
                .cond_where(condition)
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            for mut db in self.dbs().into_vec() {
                if db.fetch_exists(&select).await? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}
