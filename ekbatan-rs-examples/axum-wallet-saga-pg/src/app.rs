//! Everything the service needs, built from `ekbatan.json` (Java's `ekbatan:` section): the
//! wiring Spring does in Java's example, written out.
//!
//! 1. Migrations run first, to every shard's primary (Java's `EkbatanShardFlywayMigrator`).
//! 2. A `DatabaseRegistry` gets one pool per shard's `primaryConfig`.
//! 3. The executor, with the actions and repositories.
//! 4. A keyed lock provider on each shard's `lockConfig` pool, and a job registry on its
//!    `jobsConfig` pool: kept apart, so locks held and jobs polling never starve requests.
//! 5. The local event handler's jobs, when `ekbatan.local-event-handler.handling.enabled`.

use std::sync::Arc;

use axum::Router;
use ekbatan::aio::AsyncActionExecutor;
use ekbatan::distributed_jobs::{JobRegistry, JobsConfig};
use ekbatan::keyed_lock::SqlKeyedLockProvider;
use ekbatan::local_event_handler::{
    EventFanoutJob, EventHandlerRegistry, EventHandlingJob, LocalEventHandlerConfig,
};
use ekbatan::migration::Migrator;
use ekbatan::repository::RepositoryRegistry;
use ekbatan::{
    ConnectionProvider, DatabaseRegistry, EkbatanConfig, Error, SystemClock, action::ActionRegistry,
};

use crate::action::{
    CompleteTransferAction, InitiateTransferAction, RefundTransferAction, WalletCloseAction,
    WalletCreateAction, WalletDepositMoneyAction,
};
use crate::handler::{TransferFailedEventHandler, TransferInitiatedEventHandler};
use crate::repository::{TransferStepRepository, WalletRepository};
use crate::web::{AppState, router};

/// The running service: its routes, and what must be stopped when it ends.
pub struct App {
    pub router: Router,
    pub registry: Arc<DatabaseRegistry>,
    pub wallets: Arc<WalletRepository>,
    pub transfer_steps: Arc<TransferStepRepository>,
    /// The executor the endpoints and handlers use.
    pub executor: Arc<AsyncActionExecutor>,
    pub jobs: JobRegistry,
}

impl App {
    pub async fn start(config: &EkbatanConfig) -> Result<App, Error> {
        let sharding = config
            .sharding
            .as_ref()
            .ok_or_else(|| Error::Config("ekbatan.sharding is required".to_owned()))?;
        let migrations = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
        Migrator::builder()
            .with_sharding_config(sharding)
            .locations([migrations])
            .migrate()
            .await?;

        let registry = Arc::new(DatabaseRegistry::from_sharding_config(sharding)?);
        let wallets = Arc::new(WalletRepository::new(registry.clone()));
        let transfer_steps = Arc::new(TransferStepRepository::new(registry.clone()));
        let executor = Arc::new(
            AsyncActionExecutor::builder()
                .namespace(config.namespace.clone().unwrap_or_default())
                .database_registry(registry.clone())
                .action_registry(
                    ActionRegistry::builder()
                        .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                        .with_action(WalletDepositMoneyAction::new(wallets.clone()))
                        .with_action(WalletCloseAction::new(wallets.clone()))
                        .with_action(InitiateTransferAction::new(wallets.clone()))
                        .with_action(CompleteTransferAction::new(
                            wallets.clone(),
                            transfer_steps.clone(),
                        ))
                        .with_action(RefundTransferAction::new(
                            wallets.clone(),
                            transfer_steps.clone(),
                        ))
                        .build(),
                )
                .repository_registry(
                    RepositoryRegistry::builder()
                        .with_repository(wallets.clone())
                        .with_repository(transfer_steps.clone())
                        .build(),
                )
                .build()?,
        );

        // Locks and jobs live on the default shard's database, each on its own pool.
        let home = sharding
            .member(sharding.default_shard)
            .ok_or_else(|| Error::Config("the default shard has no member".to_owned()))?;
        let pool_for = |name: &str| {
            let config = home.config_for(name).unwrap_or(home.primary_config());
            ConnectionProvider::from_config(config)
        };
        let locks = Arc::new(SqlKeyedLockProvider::new(pool_for("lockConfig")?));

        let jobs_config = match &config.jobs {
            Some(section) => JobsConfig::from_value(section)?,
            None => JobsConfig::default(),
        };
        let mut jobs = JobRegistry::builder()
            .connection_provider(pool_for("jobsConfig")?)
            .jobs_config(&jobs_config);
        let handler_config = match &config.local_event_handler {
            Some(section) => LocalEventHandlerConfig::from_value(section)?,
            None => LocalEventHandlerConfig::default(),
        };
        if handler_config.handling_enabled {
            let handlers = || {
                EventHandlerRegistry::builder()
                    .with_handler(TransferInitiatedEventHandler::new(executor.clone()))
                    .with_handler(TransferFailedEventHandler::new(executor.clone()))
                    .build()
            };
            jobs = jobs
                .with_job(
                    handler_config.fanout_job(EventFanoutJob::new(registry.clone(), handlers()?)),
                )
                .with_job(
                    handler_config
                        .handling_job(EventHandlingJob::new(registry.clone(), handlers()?)),
                );
        }
        let jobs = jobs.build()?;
        jobs.start().await?;

        let router = router(AppState {
            executor: executor.clone(),
            locks,
            wallets: wallets.clone(),
        });
        Ok(App {
            router,
            registry,
            wallets,
            transfer_steps,
            executor,
            jobs,
        })
    }

    /// Stops the jobs (letting running ones finish), then closes every pool.
    pub async fn stop(&self) {
        self.jobs.stop().await;
        self.registry.as_async().close().await;
    }
}
