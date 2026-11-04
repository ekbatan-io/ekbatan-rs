//! Saga step 1, run by `POST /wallets/transfers`: debit the source. Java's
//! `InitiateTransferAction`.

use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::aio::{AsyncAction, AsyncRepository};
use ekbatan::{Error, ShardedId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::Wallet;
use crate::repository::WalletRepository;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub from_wallet_id: ShardedId<Wallet>,
    pub to_wallet_id: ShardedId<Wallet>,
    pub amount: Decimal,
}

pub struct Initiated {
    pub transfer_id: Uuid,
    pub source_wallet: Wallet,
}

pub struct InitiateTransferAction {
    wallet_repository: Arc<WalletRepository>,
}

impl InitiateTransferAction {
    pub fn new(wallet_repository: Arc<WalletRepository>) -> Self {
        InitiateTransferAction { wallet_repository }
    }
}

impl AsyncAction for InitiateTransferAction {
    type Params = Params;
    type Result = Initiated;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Initiated, Error> {
        let transfer_id = Uuid::new_v4();
        let source = self
            .wallet_repository
            .get_by_id(&params.from_wallet_id)
            .await?;
        let debited =
            source.initiate_transfer_out(transfer_id, params.to_wallet_id, params.amount)?;
        let source_wallet = plan.update(debited)?;
        Ok(Initiated {
            transfer_id,
            source_wallet,
        })
    }
}
