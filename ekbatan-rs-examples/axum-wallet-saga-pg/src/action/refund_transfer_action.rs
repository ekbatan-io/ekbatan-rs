//! The compensation, run by `TransferFailedEventHandler`: credit the source back. Forward-only:
//! a new step with its own commit and its own event, not a rollback of step 1. Java's
//! `RefundTransferAction`.

use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::aio::{AsyncAction, AsyncRepository};
use ekbatan::{Error, ShardedId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{TransferStep, TransferStepName, Wallet};
use crate::repository::{TransferStepRepository, WalletRepository};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub transfer_id: Uuid,
    pub from_wallet_id: ShardedId<Wallet>,
    pub amount: Decimal,
}

pub struct RefundTransferAction {
    wallet_repository: Arc<WalletRepository>,
    transfer_step_repository: Arc<TransferStepRepository>,
}

impl RefundTransferAction {
    pub fn new(
        wallet_repository: Arc<WalletRepository>,
        transfer_step_repository: Arc<TransferStepRepository>,
    ) -> Self {
        RefundTransferAction {
            wallet_repository,
            transfer_step_repository,
        }
    }
}

impl AsyncAction for RefundTransferAction {
    type Params = Params;
    type Result = Wallet;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Wallet, Error> {
        if self
            .transfer_step_repository
            .exists_step(params.transfer_id, TransferStepName::RefundTransfer)
            .await?
        {
            return self
                .wallet_repository
                .get_by_id(&params.from_wallet_id)
                .await;
        }
        plan.add(
            TransferStep::create(params.transfer_id, TransferStepName::RefundTransfer).build()?,
        )?;
        let source = self
            .wallet_repository
            .get_by_id(&params.from_wallet_id)
            .await?;
        let refunded = source.refund_transfer(params.transfer_id, params.amount)?;
        plan.update(refunded)
    }
}
