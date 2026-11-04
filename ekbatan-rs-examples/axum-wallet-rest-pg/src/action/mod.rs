//! The example's actions, one per module as in Java's example (`WalletCreateAction` and its
//! `Params`, ...). Each stages its change on the plan; the executor writes it with its events.

pub mod create_notification_action;
pub mod wallet_close_action;
pub mod wallet_create_action;
pub mod wallet_deposit_money_action;

pub use create_notification_action::CreateNotificationAction;
pub use wallet_close_action::WalletCloseAction;
pub use wallet_create_action::WalletCreateAction;
pub use wallet_deposit_money_action::WalletDepositMoneyAction;
