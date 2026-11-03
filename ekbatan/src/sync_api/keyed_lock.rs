use std::time::Duration;

use super::{block_on, blocking_runtime};
use crate::Error;

/// A keyed lock: Java's `KeyedLockProvider`. Implemented for every `AsyncKeyedLockProvider`
/// (its async twin).
pub trait KeyedLockProvider {
    /// Waits as long as it takes for `key`, then holds it until the lease is released or
    /// dropped, or `max_hold` passes.
    fn acquire(&self, key: &str, max_hold: Duration) -> Result<Lease, Error>;

    /// Waits at most `max_wait`; `None` if the key stayed taken.
    fn try_acquire(
        &self,
        key: &str,
        max_wait: Duration,
        max_hold: Duration,
    ) -> Result<Option<Lease>, Error>;
}

impl<P: crate::keyed_lock::AsyncKeyedLockProvider + ?Sized> KeyedLockProvider for P {
    fn acquire(&self, key: &str, max_hold: Duration) -> Result<Lease, Error> {
        block_on(crate::keyed_lock::AsyncKeyedLockProvider::acquire(
            self, key, max_hold,
        ))
        .map(Lease::new)
    }

    fn try_acquire(
        &self,
        key: &str,
        max_wait: Duration,
        max_hold: Duration,
    ) -> Result<Option<Lease>, Error> {
        block_on(crate::keyed_lock::AsyncKeyedLockProvider::try_acquire(
            self, key, max_wait, max_hold,
        ))
        .map(|lease| lease.map(Lease::new))
    }
}

/// A held lock: Java's `Lease`; its async twin is `AsyncLease`. Dropping it releases the key before the
/// drop returns.
pub struct Lease {
    /// `None` only once `release` took it.
    inner: Option<crate::keyed_lock::AsyncLease>,
}

impl Lease {
    fn new(lease: crate::keyed_lock::AsyncLease) -> Self {
        Lease { inner: Some(lease) }
    }

    pub fn key(&self) -> &str {
        self.inner.as_ref().expect("taken only on release").key()
    }

    /// False once released, or once `max_hold` passed and the watchdog took it back.
    pub fn is_held(&self) -> bool {
        block_on(
            self.inner
                .as_ref()
                .expect("taken only on release")
                .is_held(),
        )
    }

    pub fn release(mut self) {
        if let Some(lease) = self.inner.take() {
            block_on(lease.release());
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(lease) = self.inner.take() {
            if tokio::runtime::Handle::try_current().is_ok() {
                // Dropped on a runtime thread, where waiting could panic (say, while a
                // sync job unwinds): release it in the background instead.
                blocking_runtime().spawn(lease.release());
            } else {
                block_on(lease.release());
            }
        }
    }
}
