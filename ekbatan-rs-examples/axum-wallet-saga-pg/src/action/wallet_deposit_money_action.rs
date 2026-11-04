use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::aio::AsyncAction;
use ekbatan::aio::AsyncRepository;
use ekbatan::{Error, ShardedId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::model::Wallet;
use crate::repository::WalletRepository;

/// Java's `WalletDepositMoneyAction.Params`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub wallet_id: ShardedId<Wallet>,
    pub amount: Decimal,
    pub recipient: Option<String>,
}

pub struct WalletDepositMoneyAction {
    wallet_repository: Arc<WalletRepository>,
}

impl WalletDepositMoneyAction {
    pub fn new(wallet_repository: Arc<WalletRepository>) -> Self {
        WalletDepositMoneyAction { wallet_repository }
    }
}

impl AsyncAction for WalletDepositMoneyAction {
    type Params = Params;
    type Result = Wallet;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Wallet, Error> {
        let wallet = self.wallet_repository.get_by_id(&params.wallet_id).await?;
        let updated = wallet.deposit(params.amount)?;
        plan.update(updated)
    }
}
