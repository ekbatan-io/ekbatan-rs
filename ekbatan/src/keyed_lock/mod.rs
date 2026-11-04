//! Keyed locks: Java's `io.ekbatan.core.concurrent`. Mutual exclusion on a text key: different
//! keys are independent, the same key serializes. Derive keys per type (`"wallet:" + id`).
//!
//! - `InProcessKeyedLockProvider`: within one process.
//! - `PostgresKeyedLockProvider`: `pg_advisory_lock`, across instances.
//! - `MySqlKeyedLockProvider` / `MariaDbKeyedLockProvider`: `GET_LOCK`, across instances.
//!
//! Every lease has a `max_hold`: when it passes, the lock is released even if the holder never
//! released it - a safety net against a holder that hangs or forgets.
//!
//! Differences from Java: **not re-entrant** - Java's leases are re-entrant per thread, and an
//! async task has no thread of its own; acquiring a key the same task already holds waits for it
//! like any other caller. And a SQL acquire dropped part-way closes its connection rather than
//! returning it to the pool, so a lock granted late can't leak into the pool.
//!
//! A lock is taken through a provider in its blocking kind (`blocking::KeyedLockProvider`,
//! `blocking::Lease`) or its async kind (`aio::AsyncKeyedLockProvider`, `aio::AsyncLease`); the
//! providers serve both.

pub(crate) mod aio;
pub(crate) mod blocking;
mod in_process;
mod sql;

use sha2::{Digest, Sha256};

pub use in_process::InProcessKeyedLockProvider;
pub use sql::{
    MariaDbKeyedLockProvider, MySqlKeyedLockProvider, PostgresKeyedLockProvider,
    SqlKeyedLockProvider,
};

/// Java's `LockKeyHash`: the first 8 bytes of the key's SHA-256, as a signed 64-bit number.
pub fn lock_key_hash(key: &str) -> i64 {
    let digest = Sha256::digest(key.as_bytes());
    i64::from_be_bytes(digest[..8].try_into().expect("8 bytes"))
}

/// MySQL's lock name: Java's hex of the hash (lock names are limited to 64 characters).
pub(crate) fn mysql_name(hashed: i64) -> String {
    format!("{:x}", hashed as u64)
}

#[cfg(test)]
mod tests {
    use super::{InProcessKeyedLockProvider, lock_key_hash, mysql_name};
    use crate::aio::AsyncKeyedLockProvider;
    use sha2::{Digest, Sha256};
    use std::time::Duration;

    #[test]
    fn keys_hash_as_javas_lock_key_hash() {
        // SHA-256("wallet:1") starts 0x3f6f...; the first 8 bytes, big-endian, signed.
        let digest = Sha256::digest(b"wallet:1");
        let expected = i64::from_be_bytes(digest[..8].try_into().unwrap());
        assert_eq!(lock_key_hash("wallet:1"), expected);
        assert_eq!(mysql_name(-1), "ffffffffffffffff");
    }

    #[tokio::test]
    async fn in_process_locks_serialize_one_key_and_free_on_max_hold() {
        let locks = InProcessKeyedLockProvider::new();
        let held = locks.acquire("a", Duration::from_secs(60)).await.unwrap();
        assert!(
            locks
                .try_acquire("a", Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            locks
                .try_acquire("b", Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap()
                .is_some()
        );
        held.release().await;
        let short = locks.acquire("a", Duration::from_millis(50)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!short.is_held().await);
        assert!(
            locks
                .try_acquire("a", Duration::ZERO, Duration::from_secs(1))
                .await
                .unwrap()
                .is_some()
        );
    }
}
