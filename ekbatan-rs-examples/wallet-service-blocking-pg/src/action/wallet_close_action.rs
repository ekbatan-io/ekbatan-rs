use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::blocking::Action;
use ekbatan::blocking::Repository;
use ekbatan::{Error, ShardedId};
use serde::{Deserialize, Serialize};

use crate::model::Wallet;
use crate::repository::WalletRepository;

/// Java's `WalletCloseAction.Params`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub wallet_id: ShardedId<Wallet>,
}

pub struct WalletCloseAction {
    wallet_repository: Arc<WalletRepository>,
}

impl WalletCloseAction {
    pub fn new(wallet_repository: Arc<WalletRepository>) -> Self {
        WalletCloseAction { wallet_repository }
    }
}

impl Action for WalletCloseAction {
    type Params = Params;
    type Result = Wallet;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Wallet, Error> {
        let wallet = self.wallet_repository.get_by_id(&params.wallet_id)?;
        let closed = wallet.close()?;
        plan.update(closed)
    }
}
