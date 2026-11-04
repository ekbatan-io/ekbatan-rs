//! Java's `WalletMoneyDepositedEventHandler`, blocking (`ekbatan::blocking::EventHandler`): each
//! committed deposit, whether a request or the stipend job made it, becomes a `Notification`
//! through a second action, in its own transaction.

use std::sync::Arc;

use ekbatan::Error;
use ekbatan::blocking::{ActionExecutor, EventHandler};
use ekbatan::local_event_handler::EventEnvelope;
use tracing::info;
use uuid::Uuid;

use crate::action::{CreateNotificationAction, create_notification_action};
use crate::model::WalletMoneyDepositedEvent;

pub struct WalletMoneyDepositedEventHandler {
    executor: Arc<ActionExecutor>,
}

impl WalletMoneyDepositedEventHandler {
    pub fn new(executor: Arc<ActionExecutor>) -> Self {
        WalletMoneyDepositedEventHandler { executor }
    }
}

impl EventHandler for WalletMoneyDepositedEventHandler {
    type Event = WalletMoneyDepositedEvent;

    fn name(&self) -> String {
        "wallet-money-deposited-notification".to_owned()
    }

    fn handle(&self, envelope: EventEnvelope<WalletMoneyDepositedEvent>) -> Result<(), Error> {
        let wallet_id: Uuid = envelope
            .model_id
            .as_deref()
            .and_then(|id| id.parse().ok())
            .ok_or_else(|| Error::Unexpected("a deposit event without its wallet id".to_owned()))?;
        let params: serde_json::Value = serde_json::from_str(&envelope.action_params)?;
        let Some(recipient) = params["recipient"].as_str() else {
            return Ok(());
        };
        let message = format!(
            "Deposit of {} received. New balance: {}.",
            envelope.event.amount, envelope.event.new_balance
        );
        info!("Creating notification for wallet={wallet_id} recipient={recipient}");
        self.executor.execute::<CreateNotificationAction>(
            Some("wallet-handler"),
            create_notification_action::Params {
                wallet_id,
                kind: "MONEY_DEPOSITED".to_owned(),
                recipient: recipient.to_owned(),
                message,
            },
        )?;
        Ok(())
    }
}
