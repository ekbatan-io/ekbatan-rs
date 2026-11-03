//! Java's `EventFanoutJob`, `EventHandlingJob` and `Backoff`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::join_all;
use tracing::{error, warn};
use uuid::Uuid;

use super::handler::EventHandlerRegistry;
use super::notification::{EventNotification, EventNotificationRepository, EventNotificationState};
use crate::action::EventEntityRepository;
use crate::distributed_jobs::{AsyncDistributedJob, ExecutionContext, Schedule};
use crate::{Clock, DatabaseRegistry, Error, Instant, ShardIdentifier, SystemClock};

const BASE: Duration = Duration::from_secs(30);

/// Java's `Backoff.delay`: 30s doubled per attempt (attempts >= 1), capped at `cap`.
pub fn backoff_delay(attempts: u32, cap: Duration) -> Duration {
    let exponent = attempts.saturating_sub(1).min(30);
    BASE.saturating_mul(1u32 << exponent).min(cap)
}

fn plus(at: Instant, d: Duration) -> Instant {
    at + chrono::Duration::from_std(d).unwrap_or(chrono::Duration::MAX)
}

/// Copies new events into one notification per subscribed handler: Java's `EventFanoutJob`.
pub struct EventFanoutJob {
    name: String,
    poll_delay: Duration,
    batch_size: u64,
    database_registry: Arc<DatabaseRegistry>,
    event_handler_registry: EventHandlerRegistry,
    clock: Arc<dyn Clock>,
}

impl EventFanoutJob {
    pub const DEFAULT_NAME: &'static str = "ekbatan-event-fanout";

    pub fn new(
        database_registry: Arc<DatabaseRegistry>,
        event_handler_registry: EventHandlerRegistry,
    ) -> Self {
        EventFanoutJob {
            name: Self::DEFAULT_NAME.to_owned(),
            poll_delay: Duration::from_secs(1),
            batch_size: 200,
            database_registry,
            event_handler_registry,
            clock: Arc::new(SystemClock),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_owned();
        self
    }

    pub fn poll_delay(mut self, delay: Duration) -> Self {
        self.poll_delay = delay.max(Duration::from_millis(1));
        self
    }

    pub fn batch_size(mut self, size: u64) -> Self {
        self.batch_size = size.max(1);
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// One pass over every shard, at the same time. True if any event was fanned out.
    pub async fn drain_one_round(&self) -> Result<bool, Error> {
        let shards: Vec<ShardIdentifier> = self.database_registry.shards();
        let counts = join_all(shards.into_iter().map(|s| self.drain_batch(s))).await;
        let mut any = false;
        for count in counts {
            any |= count? > 0;
        }
        Ok(any)
    }

    async fn drain_batch(&self, shard: ShardIdentifier) -> Result<usize, Error> {
        let handled = self.event_handler_registry.handled_event_types();
        if handled.is_empty() {
            return Ok(0);
        }
        let manager = self.database_registry.transaction_manager(shard)?;
        let mut tx = manager.as_async().begin().await?;
        let result = async {
            let events =
                EventEntityRepository::find_undelivered(&mut tx.db(), &handled, self.batch_size)
                    .await?;
            if events.is_empty() {
                return Ok(0);
            }
            let now = self.clock.instant();
            let mut notifications = Vec::new();
            for event in &events {
                let event_type = event.event_type.clone().unwrap_or_default();
                for handler_name in self.event_handler_registry.subscribers_for(&event_type) {
                    notifications.push(EventNotification {
                        id: Uuid::new_v4(),
                        event_id: event.id,
                        handler_name: handler_name.clone(),
                        namespace: event.namespace.clone(),
                        action_id: event.action_id,
                        action_name: event.action_name.clone(),
                        action_params: event.action_params.clone(),
                        started_date: event.started_date,
                        completion_date: event.completion_date,
                        model_id: event.model_id.clone(),
                        model_type: event.model_type.clone(),
                        event_type: event_type.clone(),
                        payload: event.payload.clone(),
                        event_date: event.event_date,
                        state: EventNotificationState::Pending,
                        attempts: 0,
                        next_retry_at: event.event_date,
                        created_date: now,
                        updated_date: now,
                    });
                }
            }
            EventNotificationRepository::add_all_no_result(&mut tx.db(), &notifications).await?;
            let ids: Vec<Uuid> = events.iter().map(|e| e.id).collect();
            EventEntityRepository::mark_delivered(&mut tx.db(), &ids).await?;
            Ok::<usize, Error>(events.len())
        }
        .await;
        match result {
            Ok(count) => {
                tx.commit().await?;
                Ok(count)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }
}

impl AsyncDistributedJob for EventFanoutJob {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn schedule(&self) -> Schedule {
        Schedule::fixed_delay(self.poll_delay)
    }

    /// Loops until the registry stops, as Java's: drains, and waits only when there was nothing.
    async fn execute(&self, ctx: &ExecutionContext) -> Result<(), Error> {
        while !ctx.is_shutting_down() {
            match self.drain_one_round().await {
                Ok(true) => {}
                Ok(false) => ctx.sleep(self.poll_delay).await,
                Err(e) => {
                    error!("Event fan-out failed: {e}");
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

/// Runs the handlers on due notifications: Java's `EventHandlingJob`.
pub struct EventHandlingJob {
    name: String,
    poll_delay: Duration,
    batch_size: u64,
    max_backoff_cap: Duration,
    retention_window: Duration,
    database_registry: Arc<DatabaseRegistry>,
    event_handler_registry: EventHandlerRegistry,
    clock: Arc<dyn Clock>,
}

impl EventHandlingJob {
    pub const DEFAULT_NAME: &'static str = "ekbatan-event-handling";

    pub fn new(
        database_registry: Arc<DatabaseRegistry>,
        event_handler_registry: EventHandlerRegistry,
    ) -> Self {
        EventHandlingJob {
            name: Self::DEFAULT_NAME.to_owned(),
            poll_delay: Duration::from_secs(1),
            batch_size: 100,
            max_backoff_cap: Duration::from_secs(5 * 60),
            retention_window: Duration::from_secs(7 * 24 * 3600),
            database_registry,
            event_handler_registry,
            clock: Arc::new(SystemClock),
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_owned();
        self
    }

    pub fn poll_delay(mut self, delay: Duration) -> Self {
        self.poll_delay = delay.max(Duration::from_millis(1));
        self
    }

    pub fn batch_size(mut self, size: u64) -> Self {
        self.batch_size = size.max(1);
        self
    }

    pub fn max_backoff_cap(mut self, cap: Duration) -> Self {
        self.max_backoff_cap = cap.max(Duration::from_millis(1));
        self
    }

    pub fn retention_window(mut self, window: Duration) -> Self {
        self.retention_window = window.max(Duration::from_millis(1));
        self
    }

    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// One pass over every shard. True if any notification was taken.
    pub async fn drain_one_round(&self) -> Result<bool, Error> {
        let shards: Vec<ShardIdentifier> = self.database_registry.shards();
        let counts = join_all(shards.into_iter().map(|s| self.drain_batch(s))).await;
        let mut any = false;
        for count in counts {
            any |= count? > 0;
        }
        Ok(any)
    }

    async fn drain_batch(&self, shard: ShardIdentifier) -> Result<usize, Error> {
        let now = self.clock.instant();
        let notifications = {
            let mut db = self.database_registry.as_async().primary(shard)?;
            EventNotificationRepository::find_due(&mut db, self.batch_size, now).await?
        };
        if notifications.is_empty() {
            return Ok(0);
        }
        let (preflight_expired, to_invoke): (Vec<_>, Vec<_>) = notifications
            .iter()
            .partition(|n| now > plus(n.event_date, self.retention_window));
        let outcomes = join_all(to_invoke.iter().map(|n| self.invoke(n))).await;
        let mut succeeded = Vec::new();
        let mut failed = Vec::new();
        for (n, ok) in to_invoke.iter().zip(outcomes) {
            if ok {
                succeeded.push(*n);
            } else {
                failed.push(*n);
            }
        }
        let after = self.clock.instant();
        let mut failed_expired = Vec::new();
        let mut retry_by_attempts: BTreeMap<i32, Vec<&EventNotification>> = BTreeMap::new();
        for n in failed {
            let deadline = plus(n.event_date, self.retention_window);
            let next = plus(
                after,
                backoff_delay((n.attempts + 1) as u32, self.max_backoff_cap),
            );
            if next > deadline {
                failed_expired.push(n);
            } else {
                retry_by_attempts.entry(n.attempts).or_default().push(n);
            }
        }
        let ids = |rows: &[&EventNotification]| rows.iter().map(|n| n.id).collect::<Vec<_>>();
        let mut db = self.database_registry.as_async().primary(shard)?;
        EventNotificationRepository::mark_expired_all_preflight(
            &mut db,
            &ids(&preflight_expired),
            after,
        )
        .await?;
        EventNotificationRepository::mark_succeeded_all(&mut db, &ids(&succeeded), after).await?;
        EventNotificationRepository::mark_expired_all_post_failure(
            &mut db,
            &ids(&failed_expired),
            after,
        )
        .await?;
        for (attempts, rows) in &retry_by_attempts {
            let next = plus(
                after,
                backoff_delay((*attempts + 1) as u32, self.max_backoff_cap),
            );
            EventNotificationRepository::mark_failed_bucket(&mut db, &ids(rows), next, after)
                .await?;
        }
        self.log_discarded(
            &failed_expired,
            "exhausted the retention window after repeated handler failures",
        );
        self.log_discarded(
            &preflight_expired,
            "was already past the retention window when it was claimed",
        );
        Ok(notifications.len())
    }

    /// Runs one notification's handler. True on success.
    async fn invoke(&self, n: &EventNotification) -> bool {
        let Some(handler) = self.event_handler_registry.handler_for(&n.handler_name) else {
            warn!(
                "No handler registered with name: {} (event {})",
                n.handler_name, n.event_id
            );
            return false;
        };
        match handler.handle(n).await {
            Ok(()) => true,
            Err(e) => {
                warn!(
                    "Handler '{}' failed while processing event {} (attempts={}): {e}",
                    n.handler_name, n.event_id, n.attempts
                );
                false
            }
        }
    }

    fn log_discarded(&self, rows: &[&EventNotification], reason: &str) {
        let mut by_handler: BTreeMap<&str, Vec<&&EventNotification>> = BTreeMap::new();
        for n in rows {
            by_handler.entry(&n.handler_name).or_default().push(n);
        }
        for (handler, discarded) in by_handler {
            error!(
                "DISCARDED {} event notification(s) for handler '{handler}' - each {reason} \
                 ({:?}). These events will never be delivered. Example event: {} (attempts={}).",
                discarded.len(),
                self.retention_window,
                discarded[0].event_id,
                discarded[0].attempts
            );
        }
    }
}

impl AsyncDistributedJob for EventHandlingJob {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn schedule(&self) -> Schedule {
        Schedule::fixed_delay(self.poll_delay)
    }

    async fn execute(&self, ctx: &ExecutionContext) -> Result<(), Error> {
        while !ctx.is_shutting_down() {
            match self.drain_one_round().await {
                Ok(true) => {}
                Ok(false) => ctx.sleep(self.poll_delay).await,
                Err(e) => {
                    error!("Event handling failed: {e}");
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_30s_up_to_the_cap() {
        let cap = Duration::from_secs(300);
        let delays: Vec<u64> = (1..=6).map(|a| backoff_delay(a, cap).as_secs()).collect();
        assert_eq!(delays, [30, 60, 120, 240, 300, 300]);
        assert_eq!(backoff_delay(100, cap), cap);
    }
}
