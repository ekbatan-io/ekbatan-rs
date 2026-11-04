//! `AsyncRepository`: every repository's selectors and ready-made methods, waited on with
//! `.await`. `blocking::Repository` is its blocking twin; both are given to every `AbstractRepository`.

use std::collections::BTreeSet;
use std::future::Future;

use sea_query::extension::postgres::PgFunc;
use sea_query::{ArrayType, Condition, Expr, ExprTrait, Iden, Value};
use uuid::Uuid;

use crate::repository::sql::{
    count_from, insert_of, not_deleted, select_from, select_one_from, update_all_sql, update_of,
};
use opentelemetry::KeyValue;

use crate::repository::{IdOf, TableOf};
use crate::telemetry::Span;
use crate::{
    AbstractRepository, AsyncDb, AsyncDbs, AsyncTransaction, Dialect, Error, ModelId, Persistable,
    ShardIdentifier, ShardKey, ShardingStrategy, Table,
};

/// Every repository's async selectors and ready-made methods, waited on with `.await`: the
/// async twin of `Repository` (Python's `AsyncRepository`), with the same SQL. Implemented for
/// every `AbstractRepository`; a module that imports both traits must say which one it means.
pub trait AsyncRepository: AbstractRepository {
    // ---- The database selectors. Each takes a shard key: `DefaultShard`, a `ShardIdentifier`,
    // ---- or an id, which covers Java's overloads.

    /// The primary: for writes, and reads that must be current. Java's `db(...)`.
    fn db(&self, key: impl ShardKey) -> Result<AsyncDb<'_>, Error> {
        self.registry()
            .as_async()
            .primary(self.effective_shard(key)?)
    }

    /// Every shard's primary; only the default shard's under `NoSharding`. Java's `dbs()`.
    fn dbs(&self) -> AsyncDbs<'_> {
        let registry = self.registry().as_async();
        match self.sharding_strategy() {
            ShardingStrategy::NoSharding => AsyncDbs(
                registry
                    .primary(registry.default_shard())
                    .into_iter()
                    .collect(),
            ),
            ShardingStrategy::EmbeddedBits => registry.primaries(),
        }
    }

    /// The replica: for reads that may lag. Java's `readonlyDb(...)`.
    fn readonly_db(&self, key: impl ShardKey) -> Result<AsyncDb<'_>, Error> {
        self.registry()
            .as_async()
            .secondary(self.effective_shard(key)?)
    }

    /// Every shard's replica; only the default shard's under `NoSharding`. Java's
    /// `readonlyDbs()`.
    fn readonly_dbs(&self) -> AsyncDbs<'_> {
        let registry = self.registry().as_async();
        match self.sharding_strategy() {
            ShardingStrategy::NoSharding => AsyncDbs(
                registry
                    .secondary(registry.default_shard())
                    .into_iter()
                    .collect(),
            ),
            ShardingStrategy::EmbeddedBits => registry.secondaries(),
        }
    }

    /// The caller's transaction, refused if it is on another shard than the key. Java's
    /// `txDb(...)`, which returns an empty `Optional` instead.
    fn tx_db<'a>(
        &'a self,
        tx: &'a mut AsyncTransaction,
        key: impl ShardKey,
    ) -> Result<AsyncDb<'a>, Error> {
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
        tx: Option<&'a mut AsyncTransaction>,
        key: impl ShardKey,
    ) -> Result<AsyncDb<'a>, Error> {
        match tx {
            Some(tx) => AsyncRepository::tx_db(self, tx, key),
            None => AsyncRepository::db(self, key),
        }
    }

    // ---- Writes. Each joins `tx` when given one, as Java's do through `txDbElseDb`.

    /// INSERT, giving back the row as the database wrote it (`RETURNING`); on MySQL, which has
    /// no `RETURNING`, the model as given. Java's `add`.
    fn add(
        &self,
        tx: Option<&mut AsyncTransaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<Self::Persistable, Error>> + Send {
        async move {
            let insert = insert_of(&[Self::to_record(model)]);
            let mut db = AsyncRepository::tx_db_else_db(self, tx, model.id())?;
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
        tx: Option<&mut AsyncTransaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            let insert = insert_of(&[Self::to_record(model)]);
            AsyncRepository::tx_db_else_db(self, tx, model.id())?
                .execute(&insert)
                .await?;
            Ok(())
        }
    }

    /// One multi-row INSERT for every model, all on one shard. Java's `addAll`.
    fn add_all(
        &self,
        tx: Option<&mut AsyncTransaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<Vec<Self::Persistable>, Error>> + Send {
        async move {
            match models {
                [] => return Ok(Vec::new()),
                [model] => return Ok(vec![AsyncRepository::add(self, tx, model).await?]),
                _ => {}
            }
            let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
            let shard = self.shard_of_all(models)?;
            let insert = insert_of(&records);
            let mut db = AsyncRepository::tx_db_else_db(self, tx, shard)?;
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
        tx: Option<&mut AsyncTransaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            if models.is_empty() {
                return Ok(());
            }
            let span = batch_span::<Self>("BATCH_INSERT", models.len());
            let written = async {
                let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
                let shard = self.shard_of_all(models)?;
                AsyncRepository::tx_db_else_db(self, tx, shard)?
                    .execute(&insert_of(&records))
                    .await?;
                Ok(())
            }
            .await;
            span.finish(written)
        }
    }

    /// UPDATE of every column, raising the version, only if the row is still at the model's
    /// version (Java's optimistic lock). Gives back the row as the database wrote it
    /// (`RETURNING`); where UPDATE has no `RETURNING` (MySQL, MariaDB), the model's next version.
    /// Java's `update`.
    fn update(
        &self,
        tx: Option<&mut AsyncTransaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<Self::Persistable, Error>> + Send {
        async move {
            let stale = || Error::StaleRecord {
                table: TableOf::<Self>::TABLE.to_string(),
                id: model.id().uuid().to_string(),
                version: model.version(),
            };
            let update = update_of(&Self::to_record(model), model.id().uuid(), model.version());
            let mut db = AsyncRepository::tx_db_else_db(self, tx, model.id())?;
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
        tx: Option<&mut AsyncTransaction>,
        model: &Self::Persistable,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            let update = update_of(&Self::to_record(model), model.id().uuid(), model.version());
            let affected = AsyncRepository::tx_db_else_db(self, tx, model.id())?
                .execute(&update)
                .await?;
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
        tx: Option<&mut AsyncTransaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<Vec<Self::Persistable>, Error>> + Send {
        async move {
            match models {
                [] => return Ok(Vec::new()),
                [model] => return Ok(vec![AsyncRepository::update(self, tx, model).await?]),
                _ => {}
            }
            let stale = |updated: usize| Error::StaleRecords {
                table: TableOf::<Self>::TABLE.to_string(),
                expected: models.len(),
                updated,
            };
            let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
            let shard = self.shard_of_all(models)?;
            let mut db = AsyncRepository::tx_db_else_db(self, tx, shard)?;
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
        tx: Option<&mut AsyncTransaction>,
        models: &[Self::Persistable],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            if models.is_empty() {
                return Ok(());
            }
            let span = batch_span::<Self>("BATCH_UPDATE", models.len());
            let written = async {
                if let [model] = models {
                    return AsyncRepository::update_no_result(self, tx, model).await;
                }
                let records: Vec<Self::Record> = models.iter().map(Self::to_record).collect();
                let shard = self.shard_of_all(models)?;
                let mut db = AsyncRepository::tx_db_else_db(self, tx, shard)?;
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
            .await;
            span.finish(written)
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
            let record = AsyncRepository::db(self, id)?
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
            AsyncRepository::find_by_id(self, id)
                .await?
                .ok_or_else(|| Error::NotFound {
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
                let mut db = AsyncRepository::db(self, shard)?;
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
            for mut db in AsyncRepository::dbs(self).into_vec() {
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
            AsyncRepository::db(self, id)?.fetch_exists(&select).await
        }
    }

    /// Rows on every shard, added up. Java's `count`.
    fn count(&self) -> impl Future<Output = Result<i64, Error>> + Send {
        AsyncRepository::count_where(self, Condition::all())
    }

    /// Matching rows on every shard, added up. Java's `countWhere`.
    fn count_where(&self, condition: Condition) -> impl Future<Output = Result<i64, Error>> + Send {
        async move {
            let count = count_from::<TableOf<Self>>()
                .cond_where(condition)
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            let mut total = 0;
            for mut db in AsyncRepository::dbs(self).into_vec() {
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
            for mut db in AsyncRepository::dbs(self).into_vec() {
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
            for mut db in AsyncRepository::dbs(self).into_vec() {
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
            for mut db in AsyncRepository::dbs(self).into_vec() {
                if db.fetch_exists(&select).await? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

impl<R: AbstractRepository + ?Sized> AsyncRepository for R {}

/// `ekbatan.repository`, for a batch write: Java's span on `addAllNoResult` / `updateAllNoResult`.
fn batch_span<R: AbstractRepository + ?Sized>(operation: &'static str, size: usize) -> Span {
    Span::start(
        "ekbatan.repository",
        vec![
            KeyValue::new("db.operation.name", operation),
            KeyValue::new(
                "ekbatan.entity.type",
                <R::Persistable as Persistable>::model_name(),
            ),
            KeyValue::new("ekbatan.batch.size", size as i64),
        ],
    )
}
