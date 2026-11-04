//! The worker, built from `ekbatan.json`, all blocking: migrations, pools, the executor, the keyed
//! lock provider, and a job registry running the two jobs and the event handler's jobs.

use std::sync::Arc;

use ekbatan::action::ActionRegistry;
use ekbatan::blocking::{ActionExecutor, block_on};
use ekbatan::distributed_jobs::{JobRegistry, JobsConfig};
use ekbatan::keyed_lock::SqlKeyedLockProvider;
use ekbatan::local_event_handler::{
    EventFanoutJob, EventHandlerRegistry, EventHandlingJob, LocalEventHandlerConfig,
};
use ekbatan::migration::Migrator;
use ekbatan::repository::RepositoryRegistry;
use ekbatan::{ConnectionProvider, DatabaseRegistry, EkbatanConfig, Error, SystemClock};

use crate::action::{CreateNotificationAction, WalletCreateAction, WalletDepositMoneyAction};
use crate::handler::WalletMoneyDepositedEventHandler;
use crate::jobs::{WalletReportJob, WalletStipendJob};
use crate::repository::{NotificationRepository, WalletRepository};

/// The running worker.
pub struct Worker {
    pub registry: Arc<DatabaseRegistry>,
    pub executor: Arc<ActionExecutor>,
    pub wallets: Arc<WalletRepository>,
    pub notifications: Arc<NotificationRepository>,
    pub jobs: JobRegistry,
}

impl Worker {
    pub fn start(config: &EkbatanConfig) -> Result<Worker, Error> {
        let sharding = config
            .sharding
            .as_ref()
            .ok_or_else(|| Error::Config("ekbatan.sharding is required".to_owned()))?;
        // Migrations have no blocking twin: block_on runs the async call to its end.
        block_on(
            Migrator::builder()
                .with_sharding_config(sharding)
                .locations([concat!(env!("CARGO_MANIFEST_DIR"), "/migrations")])
                .migrate(),
        )?;

        let registry = Arc::new(DatabaseRegistry::from_sharding_config(sharding)?);
        let wallets = Arc::new(WalletRepository::new(registry.clone()));
        let notifications = Arc::new(NotificationRepository::new(registry.clone()));
        let executor = Arc::new(
            ActionExecutor::builder()
                .namespace(config.namespace.clone().unwrap_or_default())
                .database_registry(registry.clone())
                .action_registry(
                    ActionRegistry::builder()
                        .with_action(WalletCreateAction::new(Arc::new(SystemClock)))
                        .with_action(WalletDepositMoneyAction::new(wallets.clone()))
                        .with_action(CreateNotificationAction)
                        .build(),
                )
                .repository_registry(
                    RepositoryRegistry::builder()
                        .with_repository(wallets.clone())
                        .with_repository(notifications.clone())
                        .build(),
                )
                .build()?,
        );

        let home = sharding
            .member(sharding.default_shard)
            .ok_or_else(|| Error::Config("the default shard has no member".to_owned()))?;
        let pool_for = |name: &str| {
            ConnectionProvider::from_config(home.config_for(name).unwrap_or(home.primary_config()))
        };
        let locks = Arc::new(SqlKeyedLockProvider::new(pool_for("lockConfig")?));

        let jobs_config = match &config.jobs {
            Some(section) => JobsConfig::from_value(section)?,
            None => JobsConfig::default(),
        };
        let handler_config = match &config.local_event_handler {
            Some(section) => LocalEventHandlerConfig::from_value(section)?,
            None => LocalEventHandlerConfig::default(),
        };
        let mut jobs = JobRegistry::builder()
            .connection_provider(pool_for("jobsConfig")?)
            .jobs_config(&jobs_config)
            .with_job(WalletReportJob::new(wallets.clone()))
            .with_job(WalletStipendJob::new(
                executor.clone(),
                locks,
                wallets.clone(),
            ));
        if handler_config.handling_enabled {
            let handlers = || {
                EventHandlerRegistry::builder()
                    .with_handler(WalletMoneyDepositedEventHandler::new(executor.clone()))
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
        block_on(jobs.start())?;
        Ok(Worker {
            registry,
            executor,
            wallets,
            notifications,
            jobs,
        })
    }

    /// Stops taking new slots, lets running jobs finish, then closes every pool.
    pub fn stop(&self) {
        block_on(self.jobs.stop());
        self.registry.close();
    }
}
