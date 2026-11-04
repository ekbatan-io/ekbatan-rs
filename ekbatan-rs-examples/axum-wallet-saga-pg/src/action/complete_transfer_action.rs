//! Saga step 2, run by `TransferInitiatedEventHandler`: credit the destination, or, when it is
//! missing, closed or in another currency, mark the transfer failed on the source (which starts
//! the compensation). Java's `CompleteTransferAction`.

use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::aio::{AsyncAction, AsyncRepository};
use ekbatan::{Error, Model, ShardedId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::{TransferStep, TransferStepName, Wallet, WalletState};
use crate::repository::{TransferStepRepository, WalletRepository};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub transfer_id: Uuid,
    pub from_wallet_id: ShardedId<Wallet>,
    pub to_wallet_id: ShardedId<Wallet>,
    pub amount: Decimal,
}

pub struct CompleteTransferAction {
    wallet_repository: Arc<WalletRepository>,
    transfer_step_repository: Arc<TransferStepRepository>,
}

impl CompleteTransferAction {
    pub fn new(
        wallet_repository: Arc<WalletRepository>,
        transfer_step_repository: Arc<TransferStepRepository>,
    ) -> Self {
        CompleteTransferAction {
            wallet_repository,
            transfer_step_repository,
        }
    }

    async fn fail_on_source(
        &self,
        plan: &mut ActionPlan,
        params: &Params,
        reason: &str,
    ) -> Result<Wallet, Error> {
        let source = self
            .wallet_repository
            .get_by_id(&params.from_wallet_id)
            .await?;
        let marked = source.mark_transfer_failed(
            params.transfer_id,
            params.to_wallet_id.uuid(),
            params.amount,
            reason,
        )?;
        plan.update(marked)
    }
}

impl AsyncAction for CompleteTransferAction {
    type Params = Params;
    type Result = Wallet;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Wallet, Error> {
        // A replay: this step already ran for this transfer.
        if self
            .transfer_step_repository
            .exists_step(params.transfer_id, TransferStepName::CompleteTransfer)
            .await?
        {
            return self
                .wallet_repository
                .get_by_id(&params.from_wallet_id)
                .await;
        }
        plan.add(
            TransferStep::create(params.transfer_id, TransferStepName::CompleteTransfer).build()?,
        )?;

        let Some(destination) = self
            .wallet_repository
            .find_by_id(&params.to_wallet_id)
            .await?
        else {
            return self
                .fail_on_source(plan, params, "destination wallet not found")
                .await;
        };
        if destination.state() != WalletState::Opened {
            let reason = format!("destination wallet is {}", destination.state().as_str());
            return self.fail_on_source(plan, params, &reason).await;
        }
        let source = self
            .wallet_repository
            .get_by_id(&params.from_wallet_id)
            .await?;
        if destination.currency() != source.currency() {
            let reason = format!(
                "currency mismatch: source={} destination={}",
                source.currency(),
                destination.currency()
            );
            return self.fail_on_source(plan, params, &reason).await;
        }
        let credited = destination.complete_transfer_in(
            params.transfer_id,
            params.from_wallet_id,
            params.amount,
        )?;
        plan.update(credited)
    }
}
