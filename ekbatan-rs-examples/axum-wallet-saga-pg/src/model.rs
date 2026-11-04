//! The domain, as Java's saga example has it: a wallet (a model, which emits events), with the
//! four steps of a transfer, and `TransferStep`, the marker that makes a step run once (an
//! entity, which emits nothing).

use ekbatan::{
    Error, GenericState, Id, Instant, Model, Persistable, ShardIdentifier, ShardedId, entity,
    model, model_event,
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
/// # use axum_wallet_saga_pg::model::Wallet;
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
/// # use axum_wallet_saga_pg::model::Wallet;
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
/// # use axum_wallet_saga_pg::model::Wallet;
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

    // ---- The transfer saga: Java's four saga methods. Each step is its own action and its
    // ---- own transaction; the events chain them.

    /// Step 1: debit the source, with `TransferInitiatedEvent`. Java's `initiateTransferOut`.
    pub fn initiate_transfer_out(
        &self,
        transfer_id: Uuid,
        to_wallet_id: ShardedId<Wallet>,
        amount: Decimal,
    ) -> Result<Wallet, Error> {
        if self.state() != WalletState::Opened {
            return Err(Error::Invalid(
                "Source wallet must be OPEN to initiate a transfer".to_owned(),
            ));
        }
        if amount <= Decimal::ZERO {
            return Err(Error::Invalid(
                "Transfer amount must be positive".to_owned(),
            ));
        }
        if self.balance < amount {
            return Err(Error::Invalid(
                "Insufficient balance for transfer".to_owned(),
            ));
        }
        let new_balance = self.balance - amount;
        self.copy()
            .with_event(TransferInitiatedEvent {
                transfer_id,
                from_wallet_id: self.id().uuid(),
                to_wallet_id: to_wallet_id.uuid(),
                amount,
                new_source_balance: new_balance,
            })
            .balance(new_balance)
            .build()
    }

    /// Step 2, the happy path: credit the destination, with `TransferCompletedEvent`. Java's
    /// `completeTransferIn`.
    pub fn complete_transfer_in(
        &self,
        transfer_id: Uuid,
        from_wallet_id: ShardedId<Wallet>,
        amount: Decimal,
    ) -> Result<Wallet, Error> {
        if self.state() != WalletState::Opened {
            return Err(Error::Invalid(
                "Destination wallet must be OPEN to receive a transfer".to_owned(),
            ));
        }
        let new_balance = self.balance + amount;
        self.copy()
            .with_event(TransferCompletedEvent {
                transfer_id,
                from_wallet_id: from_wallet_id.uuid(),
                to_wallet_id: self.id().uuid(),
                amount,
                new_destination_balance: new_balance,
            })
            .balance(new_balance)
            .build()
    }

    /// Step 2, the failure path: record on the *source* that the transfer can't complete. No
    /// balance change; the update still raises the version, so the event reaches the log.
    /// Java's `markTransferFailed`.
    pub fn mark_transfer_failed(
        &self,
        transfer_id: Uuid,
        to_wallet_id: Uuid,
        amount: Decimal,
        reason: &str,
    ) -> Result<Wallet, Error> {
        self.copy()
            .with_event(TransferFailedEvent {
                transfer_id,
                from_wallet_id: self.id().uuid(),
                to_wallet_id,
                amount,
                reason: reason.to_owned(),
            })
            .build()
    }

    /// The compensation: credit the source back, with `TransferRefundedEvent`. A new step of its
    /// own, not a rollback of step 1, which committed long ago. Java's `refundTransfer`.
    pub fn refund_transfer(&self, transfer_id: Uuid, amount: Decimal) -> Result<Wallet, Error> {
        let new_balance = self.balance + amount;
        self.copy()
            .with_event(TransferRefundedEvent {
                transfer_id,
                from_wallet_id: self.id().uuid(),
                amount,
                new_source_balance: new_balance,
            })
            .balance(new_balance)
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

/// Every transfer event carries the whole transfer, so a handler needs nothing else.
#[model_event(Wallet)]
pub struct TransferInitiatedEvent {
    pub transfer_id: Uuid,
    pub from_wallet_id: Uuid,
    pub to_wallet_id: Uuid,
    pub amount: Decimal,
    pub new_source_balance: Decimal,
}

#[model_event(Wallet)]
pub struct TransferCompletedEvent {
    pub transfer_id: Uuid,
    pub from_wallet_id: Uuid,
    pub to_wallet_id: Uuid,
    pub amount: Decimal,
    pub new_destination_balance: Decimal,
}

#[model_event(Wallet)]
pub struct TransferFailedEvent {
    pub transfer_id: Uuid,
    pub from_wallet_id: Uuid,
    pub to_wallet_id: Uuid,
    pub amount: Decimal,
    pub reason: String,
}

#[model_event(Wallet)]
pub struct TransferRefundedEvent {
    pub transfer_id: Uuid,
    pub from_wallet_id: Uuid,
    pub amount: Decimal,
    pub new_source_balance: Decimal,
}

/// The saga steps that must run once per transfer: Java's `TransferStepName`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferStepName {
    CompleteTransfer,
    RefundTransfer,
}

impl TransferStepName {
    pub fn as_str(self) -> &'static str {
        match self {
            TransferStepName::CompleteTransfer => "COMPLETE_TRANSFER",
            TransferStepName::RefundTransfer => "REFUND_TRANSFER",
        }
    }

    pub fn parse(text: &str) -> Option<TransferStepName> {
        match text {
            "COMPLETE_TRANSFER" => Some(TransferStepName::CompleteTransfer),
            "REFUND_TRANSFER" => Some(TransferStepName::RefundTransfer),
            _ => None,
        }
    }
}

/// Java's `TransferStep`: the marker a step stages beside its wallet change. Handlers deliver at
/// least once; on a replay the marker is already there and the step does nothing. The table's
/// `UNIQUE (transfer_id, step)` refuses a second one even if two replays race.
#[entity(id = Id<TransferStep>, state = GenericState)]
pub struct TransferStep {
    pub transfer_id: Uuid,
    pub step: TransferStepName,
}

impl TransferStep {
    /// Java's `TransferStep.create`.
    pub fn create(transfer_id: Uuid, step: TransferStepName) -> TransferStepBuilder {
        TransferStepBuilder::new()
            .id(Id::random())
            .state(GenericState::Active)
            .transfer_id(transfer_id)
            .step(step)
            .with_initial_version()
    }
}
