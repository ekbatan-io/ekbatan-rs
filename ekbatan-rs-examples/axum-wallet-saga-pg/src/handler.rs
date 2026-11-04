//! The saga's choreography: no central state machine; each handler knows only its own next
//! step. Java's `TransferInitiatedEventHandler` and `TransferFailedEventHandler`.
//!
//! A committed `TransferInitiatedEvent` runs step 2 (`CompleteTransferAction`); a committed
//! `TransferFailedEvent` runs the compensation (`RefundTransferAction`). Delivery is at least
//! once: a step's `TransferStep` marker makes a replay do nothing.

use std::sync::Arc;

use ekbatan::aio::{AsyncActionExecutor, AsyncEventHandler};
use ekbatan::local_event_handler::EventEnvelope;
use ekbatan::{Error, ShardedId};
use tracing::info;

use crate::action::{
    CompleteTransferAction, RefundTransferAction, complete_transfer_action, refund_transfer_action,
};
use crate::model::{TransferFailedEvent, TransferInitiatedEvent};

const PRINCIPAL: &str = "transfer-saga";

pub struct TransferInitiatedEventHandler {
    executor: Arc<AsyncActionExecutor>,
}

impl TransferInitiatedEventHandler {
    pub fn new(executor: Arc<AsyncActionExecutor>) -> Self {
        TransferInitiatedEventHandler { executor }
    }
}

impl AsyncEventHandler for TransferInitiatedEventHandler {
    type Event = TransferInitiatedEvent;

    fn name(&self) -> String {
        "transfer-initiated-handler".to_owned()
    }

    async fn handle(&self, envelope: EventEnvelope<TransferInitiatedEvent>) -> Result<(), Error> {
        let e = envelope.event;
        info!(
            "Transfer {} initiated; invoking CompleteTransferAction",
            e.transfer_id
        );
        self.executor
            .execute::<CompleteTransferAction>(
                Some(PRINCIPAL),
                complete_transfer_action::Params {
                    transfer_id: e.transfer_id,
                    from_wallet_id: ShardedId::from_uuid(e.from_wallet_id),
                    to_wallet_id: ShardedId::from_uuid(e.to_wallet_id),
                    amount: e.amount,
                },
            )
            .await?;
        Ok(())
    }
}

pub struct TransferFailedEventHandler {
    executor: Arc<AsyncActionExecutor>,
}

impl TransferFailedEventHandler {
    pub fn new(executor: Arc<AsyncActionExecutor>) -> Self {
        TransferFailedEventHandler { executor }
    }
}

impl AsyncEventHandler for TransferFailedEventHandler {
    type Event = TransferFailedEvent;

    fn name(&self) -> String {
        "transfer-failed-handler".to_owned()
    }

    async fn handle(&self, envelope: EventEnvelope<TransferFailedEvent>) -> Result<(), Error> {
        let e = envelope.event;
        info!(
            "Transfer {} failed ({}); invoking RefundTransferAction",
            e.transfer_id, e.reason
        );
        self.executor
            .execute::<RefundTransferAction>(
                Some(PRINCIPAL),
                refund_transfer_action::Params {
                    transfer_id: e.transfer_id,
                    from_wallet_id: ShardedId::from_uuid(e.from_wallet_id),
                    amount: e.amount,
                },
            )
            .await?;
        Ok(())
    }
}
