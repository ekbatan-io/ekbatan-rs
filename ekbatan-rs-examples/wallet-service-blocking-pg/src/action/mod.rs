//! The service's actions, blocking (`ekbatan::blocking::Action`): `perform` is a plain `fn` that
//! reads what it needs and stages its change on the plan. The executor then writes the change
//! and its events in one transaction.

pub mod wallet_close_action;
pub mod wallet_create_action;
pub mod wallet_deposit_money_action;

pub use wallet_close_action::WalletCloseAction;
pub use wallet_create_action::WalletCreateAction;
pub use wallet_deposit_money_action::WalletDepositMoneyAction;
