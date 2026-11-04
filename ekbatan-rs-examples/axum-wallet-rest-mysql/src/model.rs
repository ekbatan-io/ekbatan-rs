//! The domain, as Java's example has it: a wallet (a model, which emits events) and the
//! notification a deposit leaves behind (an entity, which doesn't).

use ekbatan::{
    Entity, Error, Id, Instant, Model, ShardIdentifier, ShardedId, entity, model, model_event,
};
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

/// Java's `Wallet extends Model<Wallet, ShardedId<Wallet>, WalletState>`.
///
/// Immutable, as Java's `final` fields: each value is read through a getter, and a change makes
/// a new wallet through `copy()...build()`. This reads and copies:
///
/// ```
/// # use ekbatan::{Clock, Model, ShardIdentifier, SystemClock};
/// # use rust_decimal::Decimal;
/// # use axum_wallet_rest_mysql::model::Wallet;
/// let wallet = Wallet::create_wallet(ShardIdentifier::new(0, 0), uuid::Uuid::new_v4(), "EUR",
///     Decimal::ONE, SystemClock.instant()).build().unwrap();
/// let richer = wallet.copy().balance(Decimal::TEN).build().unwrap();
/// assert_eq!(*richer.balance(), Decimal::TEN);
/// ```
///
/// and this, the same with the value set in place, does not compile:
///
/// ```compile_fail,E0616
/// # use ekbatan::{Clock, Model, ShardIdentifier, SystemClock};
/// # use rust_decimal::Decimal;
/// # use axum_wallet_rest_mysql::model::Wallet;
/// let mut wallet = Wallet::create_wallet(ShardIdentifier::new(0, 0), uuid::Uuid::new_v4(), "EUR",
///     Decimal::ONE, SystemClock.instant()).build().unwrap();
/// wallet.balance = Decimal::TEN;
/// ```
///
/// nor does reaching the base fields (id, version, state, dates, events):
///
/// ```compile_fail,E0616
/// # use ekbatan::{Clock, ShardIdentifier, SystemClock};
/// # use rust_decimal::Decimal;
/// # use axum_wallet_rest_mysql::model::Wallet;
/// let mut wallet = Wallet::create_wallet(ShardIdentifier::new(0, 0), uuid::Uuid::new_v4(), "EUR",
///     Decimal::ONE, SystemClock.instant()).build().unwrap();
/// wallet.base = wallet.base.clone();
/// ```
#[model(id = ShardedId<Wallet>, state = WalletState)]
pub struct Wallet {
    pub owner_id: Uuid,
    pub currency: String,
    pub balance: Decimal,
}

impl Wallet {
    /// A new wallet on `shard`, with the event that says so. Java's `createWallet`.
    pub fn create_wallet(
        shard: ShardIdentifier,
        owner_id: Uuid,
        currency: &str,
        initial_balance: Decimal,
        created_date: Instant,
    ) -> WalletBuilder {
        WalletBuilder::new()
            .id(ShardedId::generate(shard))
            .state(WalletState::Opened)
            .owner_id(owner_id)
            .currency(currency.to_owned())
            .balance(initial_balance)
            .created_date(created_date)
            .with_initial_version()
            .with_event(WalletCreatedEvent {
                owner_id,
                currency: currency.to_owned(),
                initial_balance,
            })
    }

    /// Java's `deposit`.
    pub fn deposit(&self, amount: Decimal) -> Result<Wallet, Error> {
        if amount <= Decimal::ZERO {
            return Err(Error::Invalid("Deposit amount must be positive".to_owned()));
        }
        let new_balance = self.balance + amount;
        self.copy()
            .with_event(WalletMoneyDepositedEvent {
                amount,
                new_balance,
            })
            .balance(new_balance)
            .build()
    }

    /// Java's `close`: closing a closed wallet changes nothing and emits nothing.
    pub fn close(&self) -> Result<Wallet, Error> {
        if self.state() == WalletState::Closed {
            return Ok(self.clone());
        }
        self.copy()
            .with_event(WalletClosedEvent {})
            .state(WalletState::Closed)
            .build()
    }
}

#[model_event(Wallet)]
pub struct WalletCreatedEvent {
    pub owner_id: Uuid,
    pub currency: String,
    pub initial_balance: Decimal,
}

#[model_event(Wallet)]
pub struct WalletMoneyDepositedEvent {
    pub amount: Decimal,
    pub new_balance: Decimal,
}

#[model_event(Wallet)]
pub struct WalletClosedEvent {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationState {
    Pending,
    Sent,
    Failed,
    Deleted,
}

impl NotificationState {
    pub fn as_str(self) -> &'static str {
        match self {
            NotificationState::Pending => "PENDING",
            NotificationState::Sent => "SENT",
            NotificationState::Failed => "FAILED",
            NotificationState::Deleted => "DELETED",
        }
    }

    pub fn parse(text: &str) -> Option<NotificationState> {
        match text {
            "PENDING" => Some(NotificationState::Pending),
            "SENT" => Some(NotificationState::Sent),
            "FAILED" => Some(NotificationState::Failed),
            "DELETED" => Some(NotificationState::Deleted),
            _ => None,
        }
    }
}

/// Java's `Notification extends Entity<Notification, Id<Notification>, NotificationState>`:
/// tracked like a model, but writing one emits nothing.
#[entity(id = Id<Notification>, state = NotificationState, validate = Notification::validate)]
pub struct Notification {
    pub wallet_id: Uuid,
    pub kind: String,
    pub recipient: String,
    pub message: String,
}

impl Notification {
    /// Java's `createNotification`.
    pub fn create_notification(
        wallet_id: Uuid,
        kind: &str,
        recipient: &str,
        message: &str,
    ) -> NotificationBuilder {
        NotificationBuilder::new()
            .id(Id::random())
            .state(NotificationState::Pending)
            .wallet_id(wallet_id)
            .kind(kind.to_owned())
            .recipient(recipient.to_owned())
            .message(message.to_owned())
            .with_initial_version()
    }

    /// Java's constructor checks: `Validate.notBlank(recipient)`, `Validate.notBlank(message)`.
    fn validate(&self) -> Result<(), Error> {
        if self.recipient.trim().is_empty() {
            return Err(Error::Invalid("recipient cannot be blank".to_owned()));
        }
        if self.message.trim().is_empty() {
            return Err(Error::Invalid("message cannot be blank".to_owned()));
        }
        Ok(())
    }

    /// Java's `markSent`.
    pub fn mark_sent(&self) -> Result<Notification, Error> {
        self.copy().state(NotificationState::Sent).build()
    }

    /// Java's `markFailed`.
    pub fn mark_failed(&self) -> Result<Notification, Error> {
        self.copy().state(NotificationState::Failed).build()
    }
}
