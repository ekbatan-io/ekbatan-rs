//! The repositories: how a wallet and a notification become rows and back, and the one query the
//! stipend job asks. Everything here is blocking: the worker has no async code of its own.

use std::sync::Arc;

use ekbatan::blocking::Repository;
use ekbatan::{
    AbstractRepository, DatabaseRegistry, DbUuid, Error, Id, Model, Persistable, ShardedId,
    ShardingStrategy,
};
use rust_decimal::Decimal;
use sea_query::{Expr, ExprTrait, Query};

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

impl WalletRepository {
    /// Java's `findOpenWithBalanceBelow`: open wallets under `threshold`, from every shard's
    /// replica. Blocking, as the job that asks is.
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
