//! A plain Wallet model: just what the repository needs. (The `#[model]` macros from the
//! earlier sketch would write most of it.)

use chrono::{DateTime, SubsecRound, Utc};
use ekbatan::{Model, ShardIdentifier, ShardedId};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalletState {
    Opened,
    Settled,
    Closed,
    Deleted,
}

impl WalletState {
    /// The text in the `state` column, the same as Java's enum names.
    pub fn as_str(self) -> &'static str {
        match self {
            WalletState::Opened => "OPENED",
            WalletState::Settled => "SETTLED",
            WalletState::Closed => "CLOSED",
            WalletState::Deleted => "DELETED",
        }
    }

    pub fn parse(text: &str) -> Option<WalletState> {
        match text {
            "OPENED" => Some(WalletState::Opened),
            "SETTLED" => Some(WalletState::Settled),
            "CLOSED" => Some(WalletState::Closed),
            "DELETED" => Some(WalletState::Deleted),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Wallet {
    pub id: ShardedId<Wallet>,
    pub version: i64,
    pub state: WalletState,
    pub owner_id: Uuid,
    pub currency: String,
    pub balance: Decimal,
    pub created_date: DateTime<Utc>,
    pub updated_date: DateTime<Utc>,
}

impl Wallet {
    /// A new wallet on `shard`. Times keep microseconds, as the databases do (Java truncates
    /// them the same way).
    pub fn open(
        shard: ShardIdentifier,
        owner_id: Uuid,
        currency: &str,
        balance: Decimal,
    ) -> Wallet {
        let now = Utc::now().trunc_subsecs(6);
        Wallet {
            id: ShardedId::generate(shard),
            version: 1,
            state: WalletState::Opened,
            owner_id,
            currency: currency.to_owned(),
            balance,
            created_date: now,
            updated_date: now,
        }
    }
}

impl Model for Wallet {
    type Id = ShardedId<Wallet>;

    fn id(&self) -> &ShardedId<Wallet> {
        &self.id
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn next_version(&self) -> Wallet {
        Wallet {
            version: self.version + 1,
            ..self.clone()
        }
    }
}
