//! Work that must run once across a cluster, not once per instance: Java's
//! `io.ekbatan.distributedjobs`. Java hands the coordination to db-scheduler; Rust has no
//! equivalent that runs on PostgreSQL, MySQL and MariaDB, so this is db-scheduler's way of doing
//! it, on its table layout (`scheduled_tasks`):
//!
//! - each job is one row; every instance polls for rows that are due;
//! - an instance claims a row with one conditional `UPDATE ... WHERE version = ?`: only one
//!   instance's update matches, and only that instance runs the slot;
//! - while it runs, the instance stamps `last_heartbeat`; a row whose heartbeat is older than
//!   `heartbeat_interval x missed_heartbeats_limit` (60s by default) is dead and is rescheduled
//!   for now, so another instance takes it;
//! - when the job returns, the row gets its next time from the job's `Schedule` - but only if the
//!   row is still the one this instance claimed.
//!
//! **A slot can still run twice**: an instance that stops writing heartbeats (it stalled, or
//! briefly lost the database) is taken over while it may still be running. The claim protects the
//! row, not the job's side effects. Write jobs so a repeat is harmless.

pub(crate) mod aio;
pub(crate) mod blocking;
mod config;
mod context;
mod registry;
mod schedule;
mod store;

use std::time::Duration;

pub use config::{JobsConfig, parse_iso_duration};
pub use context::{ExecutionContext, HeartbeatState};
pub use registry::{JobRegistry, JobRegistryBuilder};
pub use schedule::Schedule;

/// The framework's heartbeat interval: Java's `EKBATAN_HEARTBEAT_INTERVAL`, not db-scheduler's
/// 5 minutes. With the missed-heartbeats limit of 6, a dead execution is revived after 60s.
pub const EKBATAN_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// db-scheduler's polling interval.
pub const DEFAULT_POLLING_INTERVAL: Duration = Duration::from_secs(10);
/// db-scheduler's `missedHeartbeatsLimit`.
pub const DEFAULT_MISSED_HEARTBEATS_LIMIT: u32 = 6;
/// db-scheduler's `shutdownMaxWait`.
pub const DEFAULT_SHUTDOWN_MAX_WAIT: Duration = Duration::from_secs(30 * 60);
