//! Java's `WalletRepository`: `from_record` / `to_record`, and the wallet's own queries, each
//! picking its database with a selector:
//!
//! - `readonly_dbs()`: every shard's replica, for reads that may lag (scatter-gather);
//! - `tx_db_else_db(tx, key)`: the caller's transaction if given one, else the primary.
//!
//! The ready-made methods (`find_by_id`, `get_by_id`, `exists_by_id`, `find_all_where`,
//! `count_where`, `add`, `update`, ...) come from `ekbatan::blocking::Repository` and `ekbatan::aio::AsyncRepository` (async, used
//! here); a repository implements `AbstractRepository`.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Arc;

use ekbatan::aio::{AsyncRepository, AsyncTransaction};
use ekbatan::{
    AbstractRepository, DatabaseRegistry, DbUuid, Error, Id, Model, Persistable, ShardIdentifier,
    ShardedId, ShardingStrategy, TableRecord,
};
use rust_decimal::Decimal;
use sea_query::{Asterisk, Expr, ExprTrait, Func, Order, Query};
use uuid::Uuid;

use crate::model::{
    Notification, NotificationBuilder, NotificationState, Wallet, WalletBuilder, WalletState,
};
use crate::schema::{NotificationsRecord, Wallets, WalletsRecord};

/// Java's sharded `WalletRepository`: ids name their shard (`EmbeddedBitsShardingStrategy`).
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

    /// Java's `fromRecord`: the builder, field by field.
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

    /// Java's `toRecord`.
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

/// Java's `NotificationRepository`: an entity repository, unsharded (`NoShardingStrategy`).
pub struct NotificationRepository {
    registry: Arc<DatabaseRegistry>,
}

impl NotificationRepository {
    pub fn new(registry: Arc<DatabaseRegistry>) -> Self {
        NotificationRepository { registry }
    }
}

impl AbstractRepository for NotificationRepository {
    type Persistable = Notification;
    type Record = NotificationsRecord;

    fn registry(&self) -> &DatabaseRegistry {
        &self.registry
    }

    fn from_record(record: NotificationsRecord) -> Result<Notification, Error> {
        let state = NotificationState::parse(&record.state).ok_or_else(|| {
            Error::Mapping(format!("unknown notification state {:?}", record.state))
        })?;
        NotificationBuilder::new()
            .id(Id::of(record.id.0))
            .version(record.version)
            .state(state)
            .wallet_id(record.wallet_id.0)
            .kind(record.kind)
            .recipient(record.recipient)
            .message(record.message)
            .build()
    }

    fn to_record(notification: &Notification) -> NotificationsRecord {
        NotificationsRecord {
            id: DbUuid(notification.id().value()),
            version: notification.version(),
            state: ekbatan::Entity::state(notification).as_str().to_owned(),
            wallet_id: DbUuid(*notification.wallet_id()),
            kind: notification.kind().clone(),
            recipient: notification.recipient().clone(),
            message: notification.message().clone(),
        }
    }
}

/// One row of `total_balance_by_currency`: a currency and its sum on one shard.
#[derive(sqlx::FromRow)]
struct CurrencyTotal {
    currency: String,
    total: Decimal,
}

impl WalletRepository {
    /// Java's `existsOnShard`: whether this one shard's database (its replica) has the wallet's
    /// row, whatever shard the id names. The test uses it to see each row landed where its id
    /// says.
    pub async fn exists_on_shard(
        &self,
        shard: ShardIdentifier,
        wallet_id: Uuid,
    ) -> Result<bool, Error> {
        let select = Query::select()
            .expr(Expr::val(1))
            .from(Wallets::Table)
            .and_where(Expr::col(Wallets::Id).eq(wallet_id))
            .to_owned();
        AsyncRepository::readonly_db(self, shard)?
            .fetch_exists(&select)
            .await
    }

    /// Java's `findAllByOwnerId`: from the replicas, of every shard, since an owner's wallets
    /// can be on any of them.
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

    /// Java's `markAllSettled`: joins the caller's transaction when given one, else runs on its
    /// own on the primary. The wallets must share a shard: one transaction can't span two.
    pub async fn mark_all_settled(
        &self,
        tx: Option<&mut AsyncTransaction>,
        ids: &[ShardedId<Wallet>],
    ) -> Result<u64, Error> {
        let Some(first) = ids.first() else {
            return Ok(0);
        };
        if let Some(other) = ids.iter().find(|id| id.shard() != first.shard()) {
            return Err(Error::WrongShard {
                transaction: first.shard(),
                row: other.shard(),
            });
        }
        let update = Query::update()
            .table(Wallets::Table)
            .value(Wallets::State, WalletState::Settled.as_str())
            .and_where(Expr::col(Wallets::Id).is_in(ids.iter().map(ShardedId::uuid)))
            .to_owned();
        self.tx_db_else_db(tx, first)?.execute(&update).await
    }

    /// Java's `totalActiveWallets`: a count on every shard's replica, at the same time, added up.
    pub async fn total_opened_wallets(&self) -> Result<i64, Error> {
        let count = Query::select()
            .expr(Func::count(Expr::col(Asterisk)))
            .from(Wallets::Table)
            .and_where(Expr::col(Wallets::State).eq(WalletState::Opened.as_str()))
            .to_owned();
        Ok(self
            .readonly_dbs()
            .fetch_scalars::<i64>(&count)
            .await?
            .into_iter()
            .sum())
    }

    /// Java's idempotent insert: adding the same wallet twice is not an error. Returns whether a
    /// row was added. (`ON CONFLICT DO NOTHING` on PostgreSQL, `INSERT IGNORE` on the others.)
    pub async fn add_if_absent(
        &self,
        tx: Option<&mut AsyncTransaction>,
        wallet: &Wallet,
    ) -> Result<bool, Error> {
        let (columns, values): (Vec<_>, Vec<_>) =
            Self::to_record(wallet).values().into_iter().unzip();
        let insert = Query::insert()
            .into_table(Wallets::Table)
            .columns(columns)
            .values_panic(values.into_iter().map(Expr::val))
            .to_owned();
        let mut db = self.tx_db_else_db(tx, wallet.id())?;
        Ok(db.insert_ignoring_duplicates(&insert, Wallets::Id).await? > 0)
    }

    /// The money in each currency, from every shard's replica: each shard sums its own wallets
    /// (GROUP BY), and the shards' sums are added up here.
    pub async fn total_balance_by_currency(&self) -> Result<BTreeMap<String, Decimal>, Error> {
        let select = Query::select()
            .column(Wallets::Currency)
            .expr_as(Func::sum(Expr::col(Wallets::Balance)), "total")
            .from(Wallets::Table)
            .and_where(Expr::col(Wallets::State).ne(WalletState::Deleted.as_str()))
            .group_by_col(Wallets::Currency)
            .to_owned();
        let mut totals = BTreeMap::new();
        for row in self
            .readonly_dbs()
            .fetch_all::<CurrencyTotal>(&select)
            .await?
        {
            *totals.entry(row.currency).or_insert(Decimal::ZERO) += row.total;
        }
        Ok(totals)
    }

    /// The `limit` richest wallets in `currency`, over every shard: each shard sends its own top
    /// `limit` (ORDER BY ... LIMIT), and the top `limit` of those is the answer.
    pub async fn richest_wallets(
        &self,
        currency: &str,
        limit: usize,
    ) -> Result<Vec<Wallet>, Error> {
        let select = Query::select()
            .column(Asterisk)
            .from(Wallets::Table)
            .and_where(Expr::col(Wallets::Currency).eq(currency))
            .and_where(Expr::col(Wallets::State).ne(WalletState::Deleted.as_str()))
            .order_by(Wallets::Balance, Order::Desc)
            .limit(limit as u64)
            .to_owned();
        let mut wallets = self
            .readonly_dbs()
            .fetch_all::<WalletsRecord>(&select)
            .await?
            .into_iter()
            .map(Self::from_record)
            .collect::<Result<Vec<_>, _>>()?;
        wallets.sort_by_key(|wallet| Reverse(*wallet.balance()));
        wallets.truncate(limit);
        Ok(wallets)
    }

    /// Closes every opened wallet with nothing in it on one shard, in one statement. Joins the
    /// caller's transaction when given one. Raises each version itself, so a copy read before
    /// it gets `StaleRecord`. Returns how many it closed.
    pub async fn close_empty_wallets(
        &self,
        tx: Option<&mut AsyncTransaction>,
        shard: ShardIdentifier,
    ) -> Result<u64, Error> {
        let update = Query::update()
            .table(Wallets::Table)
            .value(Wallets::Version, Expr::col(Wallets::Version).add(1))
            .value(Wallets::State, WalletState::Closed.as_str())
            .and_where(Expr::col(Wallets::State).eq(WalletState::Opened.as_str()))
            .and_where(Expr::col(Wallets::Balance).eq(Decimal::ZERO))
            .to_owned();
        self.tx_db_else_db(tx, shard)?.execute(&update).await
    }
}
