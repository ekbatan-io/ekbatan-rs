//! `JobRegistry`: polls `scheduled_tasks`, claims due slots, runs them, beats their heartbeats,
//! revives dead ones. Java's `JobRegistry` on db-scheduler.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use uuid::Uuid;

use super::context::HeartbeatState;
use super::store::{Claimed, Store};
use super::{
    DEFAULT_MISSED_HEARTBEATS_LIMIT, DEFAULT_POLLING_INTERVAL, DEFAULT_SHUTDOWN_MAX_WAIT,
    EKBATAN_HEARTBEAT_INTERVAL, ExecutionContext, JobsConfig, Schedule,
};
use crate::aio::AsyncDistributedJob;
use crate::{Clock, ConnectionProvider, Error, Instant, SystemClock};

/// A job, whatever its type.
trait AnyJob: Send + Sync {
    fn execute<'a>(&'a self, ctx: &'a ExecutionContext) -> BoxFuture<'a, Result<(), Error>>;
}

struct Erased<J>(J);

impl<J: AsyncDistributedJob> AnyJob for Erased<J> {
    fn execute<'a>(&'a self, ctx: &'a ExecutionContext) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(self.0.execute(ctx))
    }
}

struct Job {
    name: String,
    schedule: Schedule,
    job: Arc<dyn AnyJob>,
}

/// One running execution on this instance.
struct Running {
    claimed: Claimed,
    heartbeat: Arc<HeartbeatState>,
    handle: Option<JoinHandle<()>>,
}

struct Shared {
    stopping: watch::Receiver<bool>,
    store: Store,
    jobs: Vec<Job>,
    clock: Arc<dyn Clock>,
    instance: String,
    poll_interval: Duration,
    heartbeat_interval: Duration,
    missed_heartbeats_limit: u32,
    running: Mutex<HashMap<String, Running>>,
}

/// The cluster's jobs on this instance: Java's `JobRegistry`. `start()` registers each job's
/// row and starts polling; `stop()` stops polling and waits for running executions.
pub struct JobRegistry {
    shared: Arc<Shared>,
    shutdown_max_wait: Duration,
    stop: watch::Sender<bool>,
    loops: Mutex<Vec<JoinHandle<()>>>,
}

impl JobRegistry {
    pub fn builder() -> JobRegistryBuilder {
        JobRegistryBuilder {
            connection_provider: None,
            jobs: Vec::new(),
            poll_interval: DEFAULT_POLLING_INTERVAL,
            heartbeat_interval: EKBATAN_HEARTBEAT_INTERVAL,
            missed_heartbeats_limit: DEFAULT_MISSED_HEARTBEATS_LIMIT,
            shutdown_max_wait: DEFAULT_SHUTDOWN_MAX_WAIT,
            clock: None,
        }
    }

    pub fn job_names(&self) -> Vec<&str> {
        self.shared.jobs.iter().map(|j| j.name.as_str()).collect()
    }

    /// Writes each job's row if it has none, checks that every enabled job has one (a missing
    /// table or a pool without write access would otherwise leave jobs that never run), then
    /// starts polling and heartbeats.
    pub async fn start(&self) -> Result<(), Error> {
        let names = self.job_names();
        info!(
            "Starting JobRegistry with {} job(s): {names:?}",
            names.len()
        );
        let now = self.shared.clock.instant();
        let mut expected = Vec::new();
        for job in &self.shared.jobs {
            if let Some(first) = job.schedule.initial_execution(now) {
                self.shared
                    .store
                    .register(&job.name, first)
                    .await
                    .map_err(|e| {
                        Error::Config(format!(
                            "JobRegistry.start() could not register job '{}': {e}. Is the \
                         scheduled_tasks table there, and can the jobs pool write to it?",
                            job.name
                        ))
                    })?;
                expected.push(job.name.clone());
            }
        }
        let registered = self.shared.store.registered(&expected).await?;
        let missing: Vec<&String> = expected
            .iter()
            .filter(|n| !registered.contains(n))
            .collect();
        if !missing.is_empty() {
            return Err(Error::Config(format!(
                "JobRegistry.start() completed but {} of {} job(s) were not registered: \
                 {missing:?}. Those jobs would never run.",
                missing.len(),
                expected.len()
            )));
        }
        let mut loops = self.loops.lock().unwrap_or_else(|p| p.into_inner());
        loops.push(tokio::spawn(poll_loop(
            self.shared.clone(),
            self.stop.subscribe(),
        )));
        loops.push(tokio::spawn(heartbeat_loop(
            self.shared.clone(),
            self.stop.subscribe(),
        )));
        Ok(())
    }

    /// Stops polling, then waits up to `shutdown_max_wait` for running executions. One still
    /// running after that is abandoned: its row stays picked with a frozen heartbeat and is
    /// revived later, as after a crash.
    pub async fn stop(&self) {
        info!("Stopping JobRegistry");
        let _ = self.stop.send(true);
        let loops: Vec<JoinHandle<()>> =
            std::mem::take(&mut *self.loops.lock().unwrap_or_else(|p| p.into_inner()));
        for handle in loops {
            let _ = handle.await;
        }
        let handles: Vec<(String, JoinHandle<()>)> = {
            let mut running = self
                .shared
                .running
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            running
                .iter_mut()
                .filter_map(|(name, r)| r.handle.take().map(|h| (name.clone(), h)))
                .collect()
        };
        let deadline = tokio::time::Instant::now() + self.shutdown_max_wait;
        for (name, mut handle) in handles {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                warn!(
                    "Job '{name}' still running at shutdown; abandoned (its row is revived later)"
                );
                handle.abort();
            }
        }
        info!("JobRegistry stopped");
    }
}

/// Java's `JobRegistry.Builder`.
pub struct JobRegistryBuilder {
    connection_provider: Option<ConnectionProvider>,
    jobs: Vec<Job>,
    poll_interval: Duration,
    heartbeat_interval: Duration,
    missed_heartbeats_limit: u32,
    shutdown_max_wait: Duration,
    clock: Option<Arc<dyn Clock>>,
}

impl JobRegistryBuilder {
    /// The pool the scheduler polls through. Required; best a dedicated one (Java's
    /// `jobsConfig`), since it polls all the time.
    pub fn connection_provider(mut self, connection_provider: ConnectionProvider) -> Self {
        self.connection_provider = Some(connection_provider);
        self
    }

    /// Adds a job. Its `name()` and `schedule()` are asked once, here.
    pub fn with_job<J: AsyncDistributedJob>(mut self, job: J) -> Self {
        self.jobs.push(Job {
            name: job.name(),
            schedule: job.schedule(),
            job: Arc::new(Erased(job)),
        });
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// How often a running execution stamps its heartbeat (default 10s). An execution is dead
    /// after `heartbeat_interval x missed_heartbeats_limit`.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// db-scheduler's `missedHeartbeatsLimit` (default 6, at least 4).
    pub fn missed_heartbeats_limit(mut self, limit: u32) -> Self {
        self.missed_heartbeats_limit = limit;
        self
    }

    /// How long `stop()` waits for running executions (default 30 minutes).
    pub fn shutdown_max_wait(mut self, wait: Duration) -> Self {
        self.shutdown_max_wait = wait;
        self
    }

    /// Applies `ekbatan.jobs` settings that are set.
    pub fn jobs_config(mut self, config: &JobsConfig) -> Self {
        if let Some(interval) = config.polling_interval {
            self.poll_interval = interval;
        }
        if let Some(interval) = config.heartbeat_interval {
            self.heartbeat_interval = interval;
        }
        if let Some(wait) = config.shutdown_max_wait {
            self.shutdown_max_wait = wait;
        }
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn build(self) -> Result<JobRegistry, Error> {
        let connection_provider = self
            .connection_provider
            .ok_or_else(|| Error::Config("connectionProvider is required".to_owned()))?;
        if self.jobs.is_empty() {
            return Err(Error::Config(
                "at least one AsyncDistributedJob must be registered".to_owned(),
            ));
        }
        let mut names: Vec<&str> = Vec::new();
        for job in &self.jobs {
            if job.name.trim().is_empty() {
                return Err(Error::Config(
                    "a AsyncDistributedJob returned a blank name(); the name is its row's key in \
                     scheduled_tasks"
                        .to_owned(),
                ));
            }
            names.push(&job.name);
        }
        let mut sorted = names.clone();
        sorted.sort();
        if sorted.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Config(format!(
                "AsyncDistributedJob names must be unique within a JobRegistry; got: {names:?}"
            )));
        }
        if self.poll_interval.is_zero() {
            return Err(Error::Config(
                "pollInterval must be positive: zero polls in a tight loop".to_owned(),
            ));
        }
        if self.heartbeat_interval.is_zero() {
            return Err(Error::Config(
                "heartbeatInterval must be positive".to_owned(),
            ));
        }
        if self.missed_heartbeats_limit < 4 {
            return Err(Error::Config(
                "missedHeartbeatsLimit must be at least 4".to_owned(),
            ));
        }
        let (stop, stopping) = watch::channel(false);
        Ok(JobRegistry {
            shared: Arc::new(Shared {
                stopping,
                store: Store::new(connection_provider),
                jobs: self.jobs,
                clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
                instance: format!("ekbatan-{}", &Uuid::new_v4().simple().to_string()[..12]),
                poll_interval: self.poll_interval,
                heartbeat_interval: self.heartbeat_interval,
                missed_heartbeats_limit: self.missed_heartbeats_limit,
                running: Mutex::new(HashMap::new()),
            }),
            shutdown_max_wait: self.shutdown_max_wait,
            stop,
            loops: Mutex::new(Vec::new()),
        })
    }
}

impl Shared {
    fn dead_after(&self) -> Duration {
        self.heartbeat_interval * self.missed_heartbeats_limit
    }

    fn job(&self, name: &str) -> Option<&Job> {
        self.jobs.iter().find(|j| j.name == name)
    }

    /// One poll: revive dead executions, then claim and start what is due.
    async fn poll(self: &Arc<Self>) -> Result<(), Error> {
        let now = self.clock.instant();
        let names: Vec<String> = self.jobs.iter().map(|j| j.name.clone()).collect();
        let dead_before = now - chrono::Duration::from_std(self.dead_after()).unwrap_or_default();
        for dead in self.store.dead(&names, dead_before).await? {
            if self.is_running(&dead.task_name) {
                continue;
            }
            warn!(
                "Reviving dead execution of job '{}' (picked by {}, no heartbeat since {:?})",
                dead.task_name,
                dead.picked_by.as_deref().unwrap_or("?"),
                dead.last_heartbeat
            );
            self.store.revive(&dead, now).await?;
        }
        for due in self.store.due(&names, now).await? {
            if self.is_running(&due.task_name) {
                continue;
            }
            let Some(claimed) = self.store.claim(&due, &self.instance, now).await? else {
                continue;
            };
            self.start_execution(claimed, now);
        }
        Ok(())
    }

    fn is_running(&self, name: &str) -> bool {
        self.running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(name)
    }

    fn start_execution(self: &Arc<Self>, claimed: Claimed, now: Instant) {
        let heartbeat = Arc::new(HeartbeatState {
            clock: self.clock.clone(),
            dead_after: self.dead_after(),
            last_heartbeat: Mutex::new(now),
        });
        let name = claimed.task_name.clone();
        let mut running = self.running.lock().unwrap_or_else(|p| p.into_inner());
        running.insert(
            name.clone(),
            Running {
                claimed: claimed.clone(),
                heartbeat: heartbeat.clone(),
                handle: None,
            },
        );
        let shared = self.clone();
        let handle = tokio::spawn(async move {
            shared.execute(claimed, heartbeat).await;
        });
        if let Some(entry) = running.get_mut(&name) {
            entry.handle = Some(handle);
        }
    }

    async fn execute(self: Arc<Self>, claimed: Claimed, heartbeat: Arc<HeartbeatState>) {
        let name = claimed.task_name.clone();
        let Some(job) = self.job(&name) else {
            return;
        };
        let ctx = ExecutionContext {
            task_name: name.clone(),
            execution_time: claimed.execution_time,
            heartbeat,
            stopping: self.stopping.clone(),
        };
        info!("Job '{name}' execution started");
        let outcome = job.job.execute(&ctx).await;
        let done = self.clock.instant();
        match &outcome {
            Ok(()) => info!("Job '{name}' execution finished"),
            Err(e) => error!("Job '{name}' execution failed: {e}"),
        }
        let next = job.schedule.next_execution(done);
        // Taken out of `running` first, so the heartbeat loop stops stamping a row that is about
        // to be released.
        let current = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&name)
            .map(|r| r.claimed)
            .unwrap_or(claimed);
        match self
            .store
            .complete(&current, outcome.is_ok(), done, next)
            .await
        {
            Ok(true) => {}
            Ok(false) => warn!(
                "Job '{name}' finished, but its row was taken over meanwhile (no heartbeat for \
                 too long); this result is not recorded"
            ),
            Err(e) => error!("Job '{name}': could not record the execution: {e}"),
        }
    }

    async fn heartbeats(&self) {
        let now = self.clock.instant();
        let running: Vec<(String, Claimed, Arc<HeartbeatState>)> = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(n, r)| (n.clone(), r.claimed.clone(), r.heartbeat.clone()))
            .collect();
        for (name, claimed, heartbeat) in running {
            match self.store.heartbeat(&claimed, now).await {
                Ok(true) => heartbeat.beat(now),
                Ok(false) => warn!(
                    "Job '{name}': heartbeat found the row no longer ours (taken over); the \
                     running execution's result won't be recorded"
                ),
                Err(e) => warn!("Job '{name}': heartbeat failed: {e}"),
            }
        }
    }
}

async fn poll_loop(shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    loop {
        if let Err(e) = shared.poll().await {
            error!("Polling scheduled_tasks failed: {e}");
        }
        tokio::select! {
            _ = tokio::time::sleep(shared.poll_interval) => {}
            _ = stop.changed() => return,
        }
    }
}

async fn heartbeat_loop(shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(shared.heartbeat_interval) => {}
            _ = stop.changed() => return,
        }
        shared.heartbeats().await;
    }
}
