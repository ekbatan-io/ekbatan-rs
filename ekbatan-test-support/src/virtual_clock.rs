use std::sync::{Arc, Mutex};

use chrono::TimeDelta;
use ekbatan::{Clock, Error, Instant, SystemClock};

/// A clock a test can stop, move, or offset: Java's `VirtualClock`. Three modes:
///
/// - **real time** (the start): reads pass through to the real clock;
/// - **frozen**, after `pause` or `pause_at`: every read returns the same instant until
///   `advance` moves it;
/// - **skewed**, after `resume_from`: time runs at the real clock's rate from another instant.
///
/// Each change is one step under a lock, as Java's `AtomicReference` makes it, so eight threads
/// advancing at once lose nothing.
pub struct VirtualClock {
    real_clock: Arc<dyn Clock>,
    state: Mutex<State>,
}

#[derive(Clone, Copy)]
enum State {
    RealTime,
    Frozen(Instant),
    Skewed {
        virtual_anchor: Instant,
        real_anchor: Instant,
    },
}

impl VirtualClock {
    /// In real time, on the system clock.
    pub fn new() -> Self {
        VirtualClock::with_real_clock(Arc::new(SystemClock))
    }

    /// In real time, on `real_clock`.
    pub fn with_real_clock(real_clock: Arc<dyn Clock>) -> Self {
        VirtualClock {
            real_clock,
            state: Mutex::new(State::RealTime),
        }
    }

    /// Freezes wherever the clock is now, whichever mode it was in.
    pub fn pause(&self) {
        let mut state = self.lock();
        *state = State::Frozen(self.read(*state));
    }

    /// Freezes at `instant`.
    pub fn pause_at(&self, instant: Instant) {
        *self.lock() = State::Frozen(instant);
    }

    /// Moves a frozen clock forward. Refused when the clock is not frozen, as Java refuses it
    /// with `IllegalStateException`.
    pub fn advance(&self, duration: TimeDelta) -> Result<(), Error> {
        let mut state = self.lock();
        match *state {
            State::Frozen(instant) => {
                *state = State::Frozen(instant + duration);
                Ok(())
            }
            _ => Err(Error::Unexpected(
                "Cannot advance: clock is not paused".to_owned(),
            )),
        }
    }

    /// Runs at the real clock's rate, starting from `instant`.
    pub fn resume_from(&self, instant: Instant) {
        *self.lock() = State::Skewed {
            virtual_anchor: instant,
            real_anchor: self.real_clock.instant(),
        };
    }

    /// Back to real time, dropping any freeze or offset.
    pub fn resume(&self) {
        *self.lock() = State::RealTime;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn read(&self, state: State) -> Instant {
        match state {
            State::RealTime => self.real_clock.instant(),
            State::Frozen(instant) => instant,
            State::Skewed {
                virtual_anchor,
                real_anchor,
            } => virtual_anchor + (self.real_clock.instant() - real_anchor),
        }
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        VirtualClock::new()
    }
}

impl Clock for VirtualClock {
    fn instant(&self) -> Instant {
        let state = *self.lock();
        self.read(state)
    }
}

/// Java's `VirtualClockTest`, case for case.
#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    use chrono::Utc;

    use super::*;

    fn at(text: &str) -> Instant {
        text.parse().unwrap()
    }

    #[test]
    fn default_clock_returns_real_time() {
        let clock = VirtualClock::new();
        let before = Utc::now();
        let result = clock.instant();
        let after = Utc::now();
        assert!(before <= result && result <= after);
    }

    #[test]
    fn pause_freezes_time() {
        let clock = VirtualClock::new();
        clock.pause();
        let first = clock.instant();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(clock.instant(), first);
    }

    #[test]
    fn pause_at_freezes_at_specific_instant() {
        let clock = VirtualClock::new();
        let target = at("2025-06-15T12:00:00Z");
        clock.pause_at(target);
        assert_eq!(clock.instant(), target);
        assert_eq!(clock.instant(), target);
    }

    #[test]
    fn advance_moves_frozen_time_forward() {
        let clock = VirtualClock::new();
        clock.pause_at(at("2025-06-15T12:00:00Z"));
        clock.advance(TimeDelta::minutes(5)).unwrap();
        assert_eq!(clock.instant(), at("2025-06-15T12:05:00Z"));
    }

    #[test]
    fn advance_throws_when_not_paused() {
        let clock = VirtualClock::new();
        let refused = clock.advance(TimeDelta::minutes(5));
        assert!(
            matches!(&refused, Err(Error::Unexpected(message)) if message.contains("not paused")),
            "{refused:?}"
        );
    }

    #[test]
    fn resume_from_ticks_forward_from_given_instant() {
        let clock = VirtualClock::new();
        let target = at("2000-01-01T00:00:00Z");
        clock.resume_from(target);
        thread::sleep(Duration::from_millis(50));
        let result = clock.instant();
        assert!(result > target);
        assert!((result - target).num_milliseconds() >= 50);
    }

    #[test]
    fn resume_returns_to_real_time() {
        let clock = VirtualClock::new();
        clock.pause_at(at("2000-01-01T00:00:00Z"));
        clock.resume();
        let before = Utc::now();
        let result = clock.instant();
        let after = Utc::now();
        assert!(before <= result && result <= after);
    }

    #[test]
    fn pause_after_resume_from_freezes_at_offset_time() {
        let clock = VirtualClock::new();
        let target = at("2000-01-01T00:00:00Z");
        clock.resume_from(target);
        thread::sleep(Duration::from_millis(50));
        clock.pause();
        let frozen = clock.instant();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(clock.instant(), frozen);
        assert!(frozen > target);
    }

    #[test]
    fn concurrent_advances_do_not_lose_updates() {
        let clock = Arc::new(VirtualClock::new());
        let start = at("2025-06-15T12:00:00Z");
        let (threads, advances_per_thread) = (8, 1_000);
        clock.pause_at(start);
        let go = Arc::new(Barrier::new(threads));
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                let (clock, go) = (Arc::clone(&clock), Arc::clone(&go));
                thread::spawn(move || {
                    go.wait();
                    for _ in 0..advances_per_thread {
                        clock.advance(TimeDelta::milliseconds(1)).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let total = TimeDelta::milliseconds((threads * advances_per_thread) as i64);
        assert_eq!(clock.instant(), start + total);
    }
}
