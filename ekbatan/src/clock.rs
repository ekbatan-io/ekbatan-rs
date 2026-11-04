//! Time, injected: every timestamp the framework writes comes from a `Clock` it was handed, never
//! from the wall clock read at the point of use, so tests can freeze it (`VirtualClock` in
//! `ekbatan-test-support`). Java's `java.time.Clock`.
//!
//! What a clock gives is what is stored: the framework never shifts a time's zone or value on
//! its way to or from the database. `Instant` is UTC; zone-less `TIMESTAMP` / `DATETIME(6)`
//! columns hold its digits.

use chrono::{DateTime, SubsecRound, Utc};

/// A moment in UTC: Java's `Instant`.
pub type Instant = DateTime<Utc>;

/// Where the framework gets the current moment: Java's `Clock`, of which it uses only
/// `instant()`.
pub trait Clock: Send + Sync {
    fn instant(&self) -> Instant;
}

/// The wall clock, in UTC, in whole microseconds: Java's `Clock.systemUTC()`. The default
/// everywhere.
///
/// Microseconds because the databases keep no finer detail: a time with nanoseconds (Linux
/// clocks have them, macOS's don't) would read back different from the one written, and
/// differently per database, as PostgreSQL rounds the extra digits and the others drop them.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn instant(&self) -> Instant {
        truncate_to_micros(Utc::now())
    }
}

/// `instant` cut to whole microseconds, as Java's `Model` constructor does with
/// `truncatedTo(MICROS)`: PostgreSQL, MySQL and MariaDB keep no finer detail, so a value cut
/// here reads back equal.
pub fn truncate_to_micros(instant: Instant) -> Instant {
    instant.trunc_subsecs(6)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_system_clock_reads_now_in_utc() {
        let before = truncate_to_micros(Utc::now());
        let read = SystemClock.instant();
        assert!(before <= read && read <= Utc::now());
        assert_eq!(read.timestamp_subsec_nanos() % 1_000, 0, "whole microseconds");
    }

    #[test]
    fn truncating_keeps_microseconds_only() {
        let instant: Instant = "2025-06-15T12:00:00.123456789Z".parse().unwrap();
        let truncated = truncate_to_micros(instant);
        assert_eq!(truncated.to_rfc3339(), "2025-06-15T12:00:00.123456+00:00");
        assert_eq!(truncate_to_micros(truncated), truncated);
    }
}
