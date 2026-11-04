//! `ExecutionContext`: what a running job is told (its slot, whether the registry is stopping,
//! how alive its heartbeat is). Java's `ExecutionContext` from db-scheduler.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;

use crate::{Clock, Instant};
/// What a running execution knows about itself: Java's db-scheduler `ExecutionContext`.
#[derive(Clone)]
pub struct ExecutionContext {
    pub(super) task_name: String,
    pub(super) execution_time: Instant,
    pub(super) heartbeat: Arc<HeartbeatState>,
    pub(super) stopping: watch::Receiver<bool>,
}

impl ExecutionContext {
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    /// The slot this execution was due at.
    pub fn execution_time(&self) -> Instant {
        self.execution_time
    }

    /// Whether the registry is stopping: Java's `getSchedulerState().isShuttingDown()`. A job
    /// that loops should end its loop.
    pub fn is_shutting_down(&self) -> bool {
        *self.stopping.borrow()
    }

    /// Sleeps for `duration`, or until the registry starts stopping, whichever is first.
    pub async fn sleep(&self, duration: Duration) {
        let mut stopping = self.stopping.clone();
        if *stopping.borrow() {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(duration) => {}
            _ = stopping.changed() => {}
        }
    }

    /// This execution's heartbeat standing: Java's `getHeartbeatState()`.
    pub fn heartbeat_state(&self) -> &HeartbeatState {
        &self.heartbeat
    }
}

/// How close an execution is to being declared dead: Java's db-scheduler `HeartbeatState`.
pub struct HeartbeatState {
    pub(super) clock: Arc<dyn Clock>,
    pub(super) dead_after: Duration,
    pub(super) last_heartbeat: Mutex<Instant>,
}

impl HeartbeatState {
    /// 0.0 just after a successful heartbeat, 1.0 when another instance may take the row over. A
    /// long job can check it and stop early: returning counts as a normal completion.
    pub fn fraction_dead(&self) -> f64 {
        let last = *self
            .last_heartbeat
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let since = (self.clock.instant() - last).to_std().unwrap_or_default();
        since.as_secs_f64() / self.dead_after.as_secs_f64()
    }

    pub(super) fn beat(&self, at: Instant) {
        *self
            .last_heartbeat
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = at;
    }
}
