//! What ekbatan's schemagen would write for the `wallets` table, read from a database with the
//! migrations applied. Written by hand here; do not edit in a real project.

use chrono::NaiveDateTime;
use ekbatan::{DbUuid, Table, TableRecord};
use rust_decimal::Decimal;
use sea_query::{Iden, Value};

/// The table and its columns, for building SQL: jOOQ's `WALLETS`, `WALLETS.ID`, ...
#[derive(Iden, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wallets {
    Table,
    Id,
    Version,
    State,
    OwnerId,
    Currency,
    Balance,
    CreatedDate,
    UpdatedDate,
}

impl Table for Wallets {
    const TABLE: Self = Wallets::Table;
    const ID: Self = Wallets::Id;
    const VERSION: Self = Wallets::Version;
    const STATE: Self = Wallets::State;
    const COLUMNS: &'static [Self] = &[
        Wallets::Id,
        Wallets::Version,
        Wallets::State,
        Wallets::OwnerId,
        Wallets::Currency,
        Wallets::Balance,
        Wallets::CreatedDate,
        Wallets::UpdatedDate,
    ];
}

/// One row: jOOQ's `WalletsRecord`. Each field reads the same from every database.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct WalletsRecord {
    pub id: DbUuid,
    pub version: i64,
    pub state: String,
    pub owner_id: DbUuid,
    pub currency: String,
    pub balance: Decimal,
    pub created_date: NaiveDateTime,
    pub updated_date: NaiveDateTime,
}

impl TableRecord for WalletsRecord {
    type Table = Wallets;

    fn values(&self) -> Vec<(Wallets, Value)> {
        vec![
            (Wallets::Id, self.id.0.into()),
            (Wallets::Version, self.version.into()),
            (Wallets::State, self.state.clone().into()),
            (Wallets::OwnerId, self.owner_id.0.into()),
            (Wallets::Currency, self.currency.clone().into()),
            (Wallets::Balance, self.balance.into()),
            (Wallets::CreatedDate, self.created_date.into()),
            (Wallets::UpdatedDate, self.updated_date.into()),
        ]
    }
}

/// The `notifications` table.
#[derive(Iden, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Notifications {
    Table,
    Id,
    Version,
    State,
    WalletId,
    Kind,
    Recipient,
    Message,
}

impl Table for Notifications {
    const TABLE: Self = Notifications::Table;
    const ID: Self = Notifications::Id;
    const VERSION: Self = Notifications::Version;
    const STATE: Self = Notifications::State;
    const COLUMNS: &'static [Self] = &[
        Notifications::Id,
        Notifications::Version,
        Notifications::State,
        Notifications::WalletId,
        Notifications::Kind,
        Notifications::Recipient,
        Notifications::Message,
    ];
}

/// One `notifications` row.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct NotificationsRecord {
    pub id: DbUuid,
    pub version: i64,
    pub state: String,
    pub wallet_id: DbUuid,
    pub kind: String,
    pub recipient: String,
    pub message: String,
}

impl TableRecord for NotificationsRecord {
    type Table = Notifications;

    fn values(&self) -> Vec<(Notifications, Value)> {
        vec![
            (Notifications::Id, self.id.0.into()),
            (Notifications::Version, self.version.into()),
            (Notifications::State, self.state.clone().into()),
            (Notifications::WalletId, self.wallet_id.0.into()),
            (Notifications::Kind, self.kind.clone().into()),
            (Notifications::Recipient, self.recipient.clone().into()),
            (Notifications::Message, self.message.clone().into()),
        ]
    }
}
