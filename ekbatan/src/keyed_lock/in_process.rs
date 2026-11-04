//! `InProcessKeyedLockProvider`: keyed locks within one process.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;

use super::aio::Held;
use crate::Error;
use crate::aio::{AsyncKeyedLockProvider, AsyncLease};

/// Within one process: Java's in-process `KeyedLockProvider`. FIFO per key.
#[derive(Default)]
pub struct InProcessKeyedLockProvider {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl InProcessKeyedLockProvider {
    pub fn new() -> Self {
        InProcessKeyedLockProvider::default()
    }

    fn mutex(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().unwrap_or_else(|p| p.into_inner());
        // Forget keys no one holds or waits for, so the map doesn't grow without end.
        locks.retain(|_, m| Arc::strong_count(m) > 1 || m.try_lock().is_err());
        locks.entry(key.to_owned()).or_default().clone()
    }
}

impl AsyncKeyedLockProvider for InProcessKeyedLockProvider {
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<AsyncLease, Error>> {
        Box::pin(async move {
            let guard = self.mutex(key).lock_owned().await;
            Ok(AsyncLease::new(key, Held::InProcess(guard), max_hold))
        })
    }

    fn try_acquire<'a>(
        &'a self,
        key: &'a str,
        max_wait: Duration,
        max_hold: Duration,
    ) -> BoxFuture<'a, Result<Option<AsyncLease>, Error>> {
        Box::pin(async move {
            let mutex = self.mutex(key);
            let guard = if max_wait.is_zero() {
                mutex.try_lock_owned().ok()
            } else {
                tokio::time::timeout(max_wait, mutex.lock_owned())
                    .await
                    .ok()
            };
            Ok(guard.map(|g| AsyncLease::new(key, Held::InProcess(g), max_hold)))
        })
    }
}
