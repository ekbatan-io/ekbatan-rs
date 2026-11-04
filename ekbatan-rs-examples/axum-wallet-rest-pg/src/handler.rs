//! Java's `WalletMoneyDepositedEventHandler`: the listen-to-yourself path.
//!
//! The deposit commits its wallet row and its `WalletMoneyDepositedEvent` together. The
//! framework's fan-out and handling jobs then hand the event to this handler, in this process,
//! which records a `Notification` through a second action, in a second transaction. The event
//! log guarantees the handler eventually sees every deposit; a failure is retried with backoff.

use std::sync::Arc;

use ekbatan::Error;
use ekbatan::aio::{AsyncActionExecutor, AsyncEventHandler};
use ekbatan::local_event_handler::EventEnvelope;
use tracing::info;
use uuid::Uuid;

use crate::action::{CreateNotificationAction, create_notification_action};
use crate::model::WalletMoneyDepositedEvent;

pub struct WalletMoneyDepositedEventHandler {
    executor: Arc<AsyncActionExecutor>,
}

impl WalletMoneyDepositedEventHandler {
    pub fn new(executor: Arc<AsyncActionExecutor>) -> Self {
        WalletMoneyDepositedEventHandler { executor }
    }
}

impl AsyncEventHandler for WalletMoneyDepositedEventHandler {
    type Event = WalletMoneyDepositedEvent;

    /// Stable across deploys: stored on each queued notification row.
    fn name(&self) -> String {
        "wallet-money-deposited-notification".to_owned()
    }

    async fn handle(
        &self,
        envelope: EventEnvelope<WalletMoneyDepositedEvent>,
    ) -> Result<(), Error> {
        let wallet_id: Uuid = envelope
            .model_id
            .as_deref()
            .and_then(|id| id.parse().ok())
            .ok_or_else(|| Error::Unexpected("a deposit event without its wallet id".to_owned()))?;
        // The recipient is not part of the domain event: it is read back from the params of the
        // action that produced it, as Java's handler does.
        let params: serde_json::Value = serde_json::from_str(&envelope.action_params)?;
        let Some(recipient) = params["recipient"].as_str() else {
            return Ok(()); // a deposit with no one to tell
        };
        let message = format!(
            "Deposit of {} received. New balance: {}.",
            envelope.event.amount, envelope.event.new_balance
        );
        info!("Creating notification for wallet={wallet_id} recipient={recipient}");
        self.executor
            .execute::<CreateNotificationAction>(
                Some("wallet-handler"),
                create_notification_action::Params {
                    wallet_id,
                    kind: "MONEY_DEPOSITED".to_owned(),
                    recipient: recipient.to_owned(),
                    message,
                },
            )
            .await?;
        Ok(())
    }
}
