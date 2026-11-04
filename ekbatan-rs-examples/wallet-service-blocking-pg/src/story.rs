//! The walk-through: each step is one call to the blocking API, and says what happened.

use std::sync::Arc;

use ekbatan::action::{ActionRegistry, EventEntityRepository};
use ekbatan::blocking::{ActionExecutor, Repository, block_on};
use ekbatan::migration::Migrator;
use ekbatan::repository::RepositoryRegistry;
use ekbatan::{
    DataSourceConfig, DatabaseRegistry, Error, Model, Persistable, ShardingConfig, SystemClock,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::action::{
    WalletCloseAction, WalletCreateAction, WalletDepositMoneyAction, wallet_close_action,
    wallet_create_action, wallet_deposit_money_action,
};
use crate::repository::{NotificationRepository, WalletRepository};

/// What the story ended with, for the test to check.
pub struct Ending {
    pub balance: Decimal,
    pub version: i64,
    pub state: String,
    pub event_types: Vec<String>,
    pub refused: String,
}

/// Runs the story against `database`, printing each step through `say`.
pub fn run(database: DataSourceConfig, mut say: impl FnMut(String)) -> Result<Ending, Error> {
    // 1. The schema: the migrations in `migrations/`, applied once. Migrations have no blocking
    //    twin, so `block_on` runs the async call to its end.
    let sharding = ShardingConfig::single(database);
    let applied = block_on(
        Migrator::builder()
            .with_sharding_config(&sharding)
            .locations([concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")])
            .migrate(),
    )?;
    let executed: usize = applied.iter().map(|r| r.migrations_executed).sum();
    say(format!("1. migrated: {executed} migration file(s) applied"));

    // 2. The pieces: one database, two repositories, three actions, one executor.
    let registry = Arc::new(DatabaseRegistry::from_sharding_config(&sharding)?);
    let wallets = Arc::new(WalletRepository::new(registry.clone()));
    let executor = ActionExecutor::builder()
        .namespace("example.wallet")
        .database_registry(registry.clone())
        .action_registry(
            ActionRegistry::builder()
                .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                .with_action(WalletDepositMoneyAction::new(wallets.clone()))
                .with_action(WalletCloseAction::new(wallets.clone()))
                .build(),
        )
        .repository_registry(
            RepositoryRegistry::builder()
                .with_repository(wallets.clone())
                .with_repository(Arc::new(NotificationRepository::new(registry.clone())))
                .build(),
        )
        .build()?;

    // 3. Open a wallet: the action stages it, the executor writes it with its event.
    let wallet = executor.execute::<WalletCreateAction>(
        Some("alice"),
        wallet_create_action::Params {
            country_code: None,
            owner_id: Uuid::new_v4(),
            currency: "EUR".to_owned(),
            initial_balance: Decimal::new(1000, 2),
        },
    )?;
    let id = *wallet.id();
    say(format!(
        "2. opened wallet {} with {} {} (version {})",
        id.uuid(),
        wallet.balance(),
        wallet.currency(),
        wallet.version()
    ));

    // 4. Deposit: a new version of the wallet, and an event, in one transaction.
    let deposited = executor.execute::<WalletDepositMoneyAction>(
        Some("alice"),
        wallet_deposit_money_action::Params {
            wallet_id: id,
            amount: Decimal::new(2550, 2),
            recipient: None,
        },
    )?;
    say(format!(
        "3. deposited 25.50: balance {} (version {})",
        deposited.balance(),
        deposited.version()
    ));

    // 5. Read it back, as stored.
    let stored = wallets.get_by_id(&id)?;
    say(format!(
        "4. read back: balance {}, state {}, version {}",
        stored.balance(),
        stored.state().as_str(),
        stored.version()
    ));

    // 6. The event log: what each action recorded, in the same transactions as the wallet rows.
    let events = block_on(async {
        let mut db = registry.as_async().primary(registry.default_shard())?;
        EventEntityRepository::find_all(&mut db).await
    })?;
    let event_types: Vec<String> = events
        .iter()
        .filter(|e| e.model_id.as_deref() == Some(id.uuid().to_string().as_str()))
        .map(|e| e.event_type.clone().unwrap_or_default())
        .collect();
    say(format!(
        "5. the wallet's events: {}",
        event_types.join(", ")
    ));

    // 7. Close it.
    let closed = executor.execute::<WalletCloseAction>(
        Some("alice"),
        wallet_close_action::Params { wallet_id: id },
    )?;
    say(format!("6. closed: state {}", closed.state().as_str()));

    // 8. A deposit the model refuses: nothing is written.
    let refused = executor
        .execute::<WalletDepositMoneyAction>(
            Some("alice"),
            wallet_deposit_money_action::Params {
                wallet_id: id,
                amount: Decimal::new(-5, 0),
                recipient: None,
            },
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    say(format!("7. a deposit of -5 is refused: {refused}"));

    registry.close();
    Ok(Ending {
        balance: *closed.balance(),
        version: closed.version(),
        state: closed.state().as_str().to_owned(),
        event_types,
        refused,
    })
}
