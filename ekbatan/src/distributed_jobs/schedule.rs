//! When a job runs: db-scheduler's `Schedules.fixedDelay` and `Schedules.daily`, and a
//! disabled schedule.

use std::time::Duration;

use chrono::{NaiveTime, TimeZone, Utc};

use crate::Instant;

/// When a job runs: db-scheduler's `Schedule`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Schedule {
    /// First at start, then `delay` after each execution ends.
    FixedDelay(Duration),
    /// At each of these times of day, in UTC.
    Daily(Vec<NaiveTime>),
    /// Never: the job is registered but writes no row.
    Disabled,
}

impl Schedule {
    pub fn fixed_delay(delay: Duration) -> Schedule {
        Schedule::FixedDelay(delay)
    }

    /// At these times of day, UTC. At least one.
    pub fn daily(times: impl IntoIterator<Item = NaiveTime>) -> Schedule {
        let mut times: Vec<NaiveTime> = times.into_iter().collect();
        times.sort();
        times.dedup();
        assert!(
            !times.is_empty(),
            "a daily schedule needs at least one time"
        );
        Schedule::Daily(times)
    }

    pub fn disabled() -> Schedule {
        Schedule::Disabled
    }

    pub fn is_disabled(&self) -> bool {
        matches!(self, Schedule::Disabled)
    }

    /// The first slot, when the job's row is written. `None` when disabled.
    pub fn initial_execution(&self, now: Instant) -> Option<Instant> {
        match self {
            Schedule::FixedDelay(_) => Some(now),
            Schedule::Daily(times) => Some(next_daily(times, now)),
            Schedule::Disabled => None,
        }
    }

    /// The slot after an execution that ended at `completed`.
    pub fn next_execution(&self, completed: Instant) -> Instant {
        match self {
            Schedule::FixedDelay(delay) => {
                completed + chrono::Duration::from_std(*delay).unwrap_or(chrono::Duration::MAX)
            }
            Schedule::Daily(times) => next_daily(times, completed),
            // Never reached: a disabled job has no row to complete.
            Schedule::Disabled => completed + chrono::Duration::days(365 * 100),
        }
    }
}

/// The first of `times` (UTC) strictly after `after`.
fn next_daily(times: &[NaiveTime], after: Instant) -> Instant {
    let today = after.date_naive();
    for day in [today, today.succ_opt().expect("a next day")] {
        for time in times {
            let at = Utc.from_utc_datetime(&day.and_time(*time));
            if at > after {
                return at;
            }
        }
    }
    unreachable!("a daily time is always within the next day")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> Instant {
        text.parse().unwrap()
    }

    #[test]
    fn fixed_delay_starts_now_and_counts_from_the_end() {
        let s = Schedule::fixed_delay(Duration::from_secs(5));
        assert_eq!(
            s.initial_execution(at("2026-01-01T00:00:00Z")),
            Some(at("2026-01-01T00:00:00Z"))
        );
        assert_eq!(
            s.next_execution(at("2026-01-01T00:00:03Z")),
            at("2026-01-01T00:00:08Z")
        );
    }

    #[test]
    fn daily_picks_the_next_time_today_or_tomorrow() {
        let s = Schedule::daily([
            NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
            NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
        ]);
        assert_eq!(
            s.next_execution(at("2026-01-01T05:00:00Z")),
            at("2026-01-01T06:00:00Z")
        );
        assert_eq!(
            s.next_execution(at("2026-01-01T06:00:00Z")),
            at("2026-01-01T18:00:00Z")
        );
        assert_eq!(
            s.next_execution(at("2026-01-01T19:00:00Z")),
            at("2026-01-02T06:00:00Z")
        );
    }

    #[test]
    fn disabled_writes_no_row() {
        assert_eq!(
            Schedule::disabled().initial_execution(at("2026-01-01T00:00:00Z")),
            None
        );
    }
}
