use std::future::Future;

use chrono::{SubsecRound, Utc};
use sea_query::{Asterisk, Condition, Expr, ExprTrait, Func, Iden, Query, UpdateStatement, Value};
use uuid::Uuid;

use crate::{DatabaseRegistry, Db, Dbs, Error, Record, ShardKey, ShardedId, Transaction};

/// What the ready-made methods need from a model.
pub trait Model: Send + Sync + 'static {
    type Id: ModelId;

    fn id(&self) -> &Self::Id;
    fn version(&self) -> i64;
}

/// A model's id: a UUID that knows its shard.
pub trait ModelId: ShardKey + Send + Sync {
    fn uuid(&self) -> Uuid;
}

impl<M> ModelId for ShardedId<M> {
    fn uuid(&self) -> Uuid {
        ShardedId::uuid(self)
    }
}

/// A table, as schemagen writes it: its name and the columns the ready-made methods use.
pub trait Table: Iden + PartialEq + Copy + Send + Sync + 'static {
    const TABLE: Self;
    const ID: Self;
    const VERSION: Self;
    const STATE: Self;
    const UPDATED_DATE: Self;
    const COLUMNS: &'static [Self];
}

/// One row of a table, as schemagen writes it: read by sqlx, written as column-value pairs.
pub trait TableRecord: Record + 'static {
    type Table: Table;

    fn values(&self) -> Vec<(Self::Table, Value)>;
}

type TableOf<R> = <<R as ModelRepository>::Record as TableRecord>::Table;
type IdOf<R> = <<R as ModelRepository>::Model as Model>::Id;

/// `state <> 'DELETED'`: Java's private `notDeleted()`. Only the ready-made methods use it;
/// custom queries write their own condition when they want one.
fn not_deleted<T: Table>() -> Expr {
    Expr::col(T::STATE).ne("DELETED")
}

/// Java's `AbstractRepository`. An implementation says where the databases are and how a row
/// becomes a model and back (`from_record` / `to_record`, as in Java); the database selectors
/// and the ready-made methods below work from that.
pub trait ModelRepository: Send + Sync {
    type Model: Model;
    type Record: TableRecord;

    fn registry(&self) -> &DatabaseRegistry;
    fn from_record(record: Self::Record) -> Result<Self::Model, Error>;
    fn to_record(model: &Self::Model) -> Self::Record;

    // ---- The database selectors. Each takes a shard key: `DefaultShard`, a `ShardIdentifier`,
    // ---- or an id, which covers Java's overloads.

    /// The primary: for writes, and reads that must be current. Java's `db(...)`.
    fn db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        self.registry().primary(key)
    }

    /// Every shard's primary. Java's `dbs()`.
    fn dbs(&self) -> Dbs<'_> {
        self.registry().primaries()
    }

    /// The replica: for reads that may lag. Java's `readonlyDb(...)`.
    fn readonly_db(&self, key: impl ShardKey) -> Result<Db<'_>, Error> {
        self.registry().secondary(key)
    }

    /// Every shard's replica. Java's `readonlyDbs()`.
    fn readonly_dbs(&self) -> Dbs<'_> {
        self.registry().secondaries()
    }

    /// The caller's transaction, refused if it is on another shard than the key. Java's
    /// `txDb(...)`, which returns an empty `Optional` instead.
    fn tx_db<'a>(&'a self, tx: &'a mut Transaction, key: impl ShardKey) -> Result<Db<'a>, Error> {
        let shard = self.registry().effective_shard(key)?;
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

    // ---- Ready-made methods.

    /// An UPDATE of this table that raises each changed row's version and sets its
    /// `updated_date`, as `update` does, so a copy read before it gets `StaleRecord` instead of
    /// quietly undoing it. The start of custom bulk updates.
    fn bulk_update(&self) -> UpdateStatement {
        let version = TableOf::<Self>::VERSION;
        Query::update()
            .table(TableOf::<Self>::TABLE)
            .value(version, Expr::col(version).add(1))
            .value(
                TableOf::<Self>::UPDATED_DATE,
                Utc::now().trunc_subsecs(6).naive_utc(),
            )
            .to_owned()
    }

    fn find_by_id(
        &self,
        id: &IdOf<Self>,
    ) -> impl Future<Output = Result<Option<Self::Model>, Error>> + Send {
        async move {
            let select = Query::select()
                .columns(TableOf::<Self>::COLUMNS.iter().copied())
                .from(TableOf::<Self>::TABLE)
                .and_where(Expr::col(TableOf::<Self>::ID).eq(id.uuid()))
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            let record = self.db(id)?.fetch_optional::<Self::Record>(&select).await?;
            record.map(Self::from_record).transpose()
        }
    }

    fn get_by_id(
        &self,
        id: &IdOf<Self>,
    ) -> impl Future<Output = Result<Self::Model, Error>> + Send {
        async move {
            self.find_by_id(id).await?.ok_or_else(|| Error::NotFound {
                table: TableOf::<Self>::TABLE.to_string(),
                id: id.uuid().to_string(),
            })
        }
    }

    fn exists_by_id(&self, id: &IdOf<Self>) -> impl Future<Output = Result<bool, Error>> + Send {
        async move {
            let count = Query::select()
                .expr(Func::count(Expr::col(Asterisk)))
                .from(TableOf::<Self>::TABLE)
                .and_where(Expr::col(TableOf::<Self>::ID).eq(id.uuid()))
                .and_where(not_deleted::<TableOf<Self>>())
                .to_owned();
            Ok(self.db(id)?.fetch_scalar::<i64>(&count).await? > 0)
        }
    }

    /// Every shard at once, since a condition can match rows anywhere.
    fn find_all_where(
        &self,
        condition: Condition,
    ) -> impl Future<Output = Result<Vec<Self::Model>, Error>> + Send {
        async move {
            let select = Query::select()
                .columns(TableOf::<Self>::COLUMNS.iter().copied())
                .from(TableOf::<Self>::TABLE)
                .and_where(not_deleted::<TableOf<Self>>())
                .cond_where(condition)
                .to_owned();
            let records = self.dbs().fetch_all::<Self::Record>(&select).await?;
            records.into_iter().map(Self::from_record).collect()
        }
    }

    /// Every shard at once, added up.
    fn count_where(&self, condition: Condition) -> impl Future<Output = Result<i64, Error>> + Send {
        async move {
            let count = Query::select()
                .expr(Func::count(Expr::col(Asterisk)))
                .from(TableOf::<Self>::TABLE)
                .and_where(not_deleted::<TableOf<Self>>())
                .cond_where(condition)
                .to_owned();
            Ok(self
                .dbs()
                .fetch_scalars::<i64>(&count)
                .await?
                .into_iter()
                .sum())
        }
    }

    /// INSERT. Joins `tx` when given one, as Java's inherited writes do.
    fn add(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Model,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            let (columns, values): (Vec<_>, Vec<_>) =
                Self::to_record(model).values().into_iter().unzip();
            let insert = Query::insert()
                .into_table(TableOf::<Self>::TABLE)
                .columns(columns)
                .values_panic(values.into_iter().map(Expr::val))
                .to_owned();
            self.tx_db_else_db(tx, model.id())?.execute(&insert).await?;
            Ok(())
        }
    }

    /// UPDATE ... WHERE id = ? AND version = ?, raising the version: Java's optimistic lock.
    /// Nothing matched means someone saved a newer version first.
    fn update(
        &self,
        tx: Option<&mut Transaction>,
        model: &Self::Model,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            let version = model.version();
            let mut update = Query::update();
            update.table(TableOf::<Self>::TABLE);
            for (column, value) in Self::to_record(model).values() {
                if column == TableOf::<Self>::ID {
                    continue;
                } else if column == TableOf::<Self>::VERSION {
                    update.value(column, version + 1);
                } else if column == TableOf::<Self>::UPDATED_DATE {
                    update.value(column, Utc::now().trunc_subsecs(6).naive_utc());
                } else {
                    update.value(column, Expr::val(value));
                }
            }
            update
                .and_where(Expr::col(TableOf::<Self>::ID).eq(model.id().uuid()))
                .and_where(Expr::col(TableOf::<Self>::VERSION).eq(version));
            let changed = self.tx_db_else_db(tx, model.id())?.execute(&update).await?;
            if changed == 0 {
                return Err(Error::StaleRecord {
                    table: TableOf::<Self>::TABLE.to_string(),
                    id: model.id().uuid().to_string(),
                    version,
                });
            }
            Ok(())
        }
    }
}
