//! Reacting to our own events, in this service: Java's `ekbatan-events/local-event-handler`.
//!
//! Two distributed jobs move events from the event log to the handlers:
//!
//! - `EventFanoutJob`: for each undelivered `eventlog.events` row whose type has a handler,
//!   writes one `eventlog.event_notifications` row per handler and marks the event delivered, in
//!   one transaction per shard;
//! - `EventHandlingJob`: runs each due notification's handler; a failure retries later with a
//!   backoff (30s doubling, capped at 5 minutes), and a notification older than the retention
//!   window (7 days) is expired instead of run.
//!
//! Each handler gets each event at least once: a handler must be idempotent.

mod config;
mod handler;
mod jobs;
mod notification;

pub use config::LocalEventHandlerConfig;
pub use handler::{EventEnvelope, EventHandlerRegistry, EventHandlerRegistryBuilder};
pub use jobs::{EventFanoutJob, EventHandlingJob, backoff_delay};
pub use notification::{EventNotification, EventNotificationRepository, EventNotificationState};
