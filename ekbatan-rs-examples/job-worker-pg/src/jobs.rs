//! The worker's two jobs, blocking (`ekbatan::blocking::DistributedJob`). Like every distributed
//! job, each runs at most once per slot across all instances: three workers running this app
//! share one `scheduled_tasks` row per job, so each slot runs once, not three times.

use std::sync::Arc;
use std::time::Duration;

use ekbatan::blocking::{ActionExecutor, DistributedJob, KeyedLockProvider, Repository};
use ekbatan::distributed_jobs::{ExecutionContext, Schedule};
use ekbatan::keyed_lock::SqlKeyedLockProvider;
use ekbatan::{Error, Persistable};
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::action::{WalletDepositMoneyAction, wallet_deposit_money_action};
use crate::repository::WalletRepository;

/// A read-only report: counts the wallets and sums their balances, and logs it. Java's
/// `WalletReportJob`: the "scheduled query" shape (dashboards, metrics, freshness probes).
pub struct WalletReportJob {
    wallets: Arc<WalletRepository>,
}

impl WalletReportJob {
    pub fn new(wallets: Arc<WalletRepository>) -> Self {
        WalletReportJob { wallets }
    }
}

impl DistributedJob for WalletReportJob {
    fn name(&self) -> String {
        "wallet-report".to_owned()
    }

    fn schedule(&self) -> Schedule {
        Schedule::fixed_delay(Duration::from_secs(5))
    }

    fn execute(&self, _ctx: &ExecutionContext) -> Result<(), Error> {
        let wallets = self.wallets.find_all()?;
        let total: Decimal = wallets.iter().map(|w| *w.balance()).sum();
        info!(
            "WalletReportJob: {} wallets, total balance = {total}",
            wallets.len()
        );
        Ok(())
    }
}

/// Wallets below this balance get a top-up on each run.
pub const THRESHOLD: Decimal = Decimal::from_parts(10000, 0, 0, false, 2);
/// What each top-up deposits.
pub const STIPEND: Decimal = Decimal::from_parts(1000, 0, 0, false, 2);

/// Tops up every open wallet under `THRESHOLD`: Java's `WalletStipendJob`, the "job runs an
/// action" shape. Each deposit is the usual action, in its own transaction, under the wallet's
/// keyed lock, and its event reaches the handler as a request's would. One wallet failing is
/// logged and skipped, so one bad row doesn't stop the run.
pub struct WalletStipendJob {
    executor: Arc<ActionExecutor>,
    locks: Arc<SqlKeyedLockProvider>,
    wallets: Arc<WalletRepository>,
}

impl WalletStipendJob {
    pub fn new(
        executor: Arc<ActionExecutor>,
        locks: Arc<SqlKeyedLockProvider>,
        wallets: Arc<WalletRepository>,
    ) -> Self {
        WalletStipendJob {
            executor,
            locks,
            wallets,
        }
    }
}

impl DistributedJob for WalletStipendJob {
    /// Unique across the cluster: the `scheduled_tasks` row's key.
    fn name(&self) -> String {
        "wallet-stipend".to_owned()
    }

    fn schedule(&self) -> Schedule {
        Schedule::fixed_delay(Duration::from_secs(2))
    }

    fn execute(&self, _ctx: &ExecutionContext) -> Result<(), Error> {
        let under_funded = self.wallets.find_open_with_balance_below(THRESHOLD)?;
        if under_funded.is_empty() {
            return Ok(());
        }
        info!(
            "WalletStipendJob: depositing {STIPEND} to {} wallet(s)",
            under_funded.len()
        );
        for wallet in under_funded {
            let id = *wallet.id();
            let deposited = self
                .locks
                .acquire(&format!("wallet:{}", id.uuid()), Duration::from_secs(10))
                .and_then(|_lease| {
                    // The lease is held until this closure returns, then released.
                    self.executor.execute::<WalletDepositMoneyAction>(
                        Some("wallet-stipend-job"),
                        wallet_deposit_money_action::Params {
                            wallet_id: id,
                            amount: STIPEND,
                            recipient: Some("stipend@system.local".to_owned()),
                        },
                    )
                });
            if let Err(e) = deposited {
                warn!(
                    "WalletStipendJob: deposit failed for wallet {} - skipping: {e}",
                    id.uuid()
                );
            }
        }
        Ok(())
    }
}
