use std::sync::Arc;

use ekbatan::action::ActionPlan;
use ekbatan::aio::AsyncAction;
use ekbatan::{Clock, Error, ShardIdentifier};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::model::Wallet;

pub const GLOBAL_SHARD: ShardIdentifier = ShardIdentifier::new(0, 0);
pub const MEXICO_SHARD: ShardIdentifier = ShardIdentifier::new(1, 0);

/// Java's `WalletCreateAction.Params`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub country_code: Option<String>,
    pub owner_id: Uuid,
    pub currency: String,
    pub initial_balance: Decimal,
}

/// Opens a wallet: on the Mexico shard for country `MX`, on the global shard otherwise.
pub struct WalletCreateAction {
    clock: Arc<dyn Clock>,
}

impl WalletCreateAction {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        WalletCreateAction { clock }
    }

    fn resolve_shard(country_code: Option<&str>) -> ShardIdentifier {
        match country_code {
            Some(code) if code.eq_ignore_ascii_case("MX") => MEXICO_SHARD,
            _ => GLOBAL_SHARD,
        }
    }
}

impl AsyncAction for WalletCreateAction {
    type Params = Params;
    type Result = Wallet;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        _principal: Option<&str>,
        params: &Params,
    ) -> Result<Wallet, Error> {
        let wallet = Wallet::create_wallet(
            Self::resolve_shard(params.country_code.as_deref()),
            params.owner_id,
            &params.currency,
            params.initial_balance,
            self.clock.instant(),
        )
        .build()?;
        plan.add(wallet)
    }
}
