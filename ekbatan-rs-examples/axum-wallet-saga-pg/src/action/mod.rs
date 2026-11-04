//! The example's actions, one per module as in Java's example (`InitiateTransferAction` and its
//! `Params`, ...). Each stages its change on the plan; the executor writes it with its events.
//!
//! A transfer is three of them, chained by events: `InitiateTransferAction` (in the request),
//! then `CompleteTransferAction` or, when that can't credit the destination,
//! `RefundTransferAction` (both run by event handlers).

pub mod complete_transfer_action;
pub mod initiate_transfer_action;
pub mod refund_transfer_action;
pub mod wallet_close_action;
pub mod wallet_create_action;
pub mod wallet_deposit_money_action;

pub use complete_transfer_action::CompleteTransferAction;
pub use initiate_transfer_action::InitiateTransferAction;
pub use refund_transfer_action::RefundTransferAction;
pub use wallet_close_action::WalletCloseAction;
pub use wallet_create_action::WalletCreateAction;
pub use wallet_deposit_money_action::WalletDepositMoneyAction;
