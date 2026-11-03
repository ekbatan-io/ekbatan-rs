//! Java's `ExecutionConfiguration` and `RetryConfig`: retries and the cross-shard policy.

use std::fmt;
use std::time::Duration;

use crate::Error;

/// How often to retry one kind of failure, and how long to wait between tries: Java's
/// `RetryConfig`. `max_retries = 0` turns retrying off; `n` allows up to `n + 1` attempts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub delay: Duration,
}

impl RetryConfig {
    pub fn new(max_retries: u32, delay: Duration) -> Self {
        RetryConfig { max_retries, delay }
    }
}

/// Which failure a retry policy is for: Java keys it by exact exception class, checking the
/// thrown exception and each cause. Here a failure is ekbatan's `Error`, so:
///
/// - `RetryOn::stale_record()`: `Error::StaleRecord` and `Error::StaleRecords` (Java's
///   `StaleRecordException`);
/// - `RetryOn::error::<E>()`: an app error of type `E` anywhere in the error's `source()` chain,
///   such as one an action returned through `Error::action(...)`.
#[derive(Clone, Copy)]
pub struct RetryOn {
    name: &'static str,
    matches: fn(&(dyn std::error::Error + 'static)) -> bool,
}

impl RetryOn {
    pub fn stale_record() -> Self {
        RetryOn {
            name: "StaleRecord",
            matches: |error| {
                matches!(
                    error.downcast_ref::<Error>(),
                    Some(Error::StaleRecord { .. } | Error::StaleRecords { .. })
                )
            },
        }
    }

    pub fn error<E: std::error::Error + 'static>() -> Self {
        RetryOn {
            name: std::any::type_name::<E>(),
            matches: |error| error.is::<E>(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    fn matches(&self, error: &(dyn std::error::Error + 'static)) -> bool {
        (self.matches)(error)
    }
}

impl PartialEq for RetryOn {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl fmt::Debug for RetryOn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RetryOn({})", self.name)
    }
}

/// Per-call, or per-executor default, knobs for `AsyncActionExecutor`: Java's
/// `ExecutionConfiguration`. The default retries a stale record once after 100ms and refuses
/// an action that touches more than one shard.
///
/// `allow_cross_shard(true)` commits each shard's transaction on its own. That is not a
/// two-phase commit: if a later shard fails, the earlier ones stay committed.
#[derive(Clone, Debug)]
pub struct ExecutionConfiguration {
    retry_configs: Vec<(RetryOn, RetryConfig)>,
    allow_cross_shard: bool,
}

impl Default for ExecutionConfiguration {
    fn default() -> Self {
        ExecutionConfiguration {
            retry_configs: vec![(
                RetryOn::stale_record(),
                RetryConfig::new(1, Duration::from_millis(100)),
            )],
            allow_cross_shard: false,
        }
    }
}

impl ExecutionConfiguration {
    /// Java's `ExecutionConfiguration.Builder.executionConfiguration()`: start from the default.
    pub fn builder() -> ExecutionConfiguration {
        ExecutionConfiguration::default()
    }

    /// Adds, or replaces, the policy for one kind of failure.
    pub fn with_retry(mut self, on: RetryOn, config: RetryConfig) -> Self {
        self.retry_configs.retain(|(existing, _)| *existing != on);
        self.retry_configs.push((on, config));
        self
    }

    /// Replaces every policy.
    pub fn retry_configs(mut self, retry_configs: Vec<(RetryOn, RetryConfig)>) -> Self {
        self.retry_configs = retry_configs;
        self
    }

    /// No retries at all, the default stale-record one included.
    pub fn no_retry(mut self) -> Self {
        self.retry_configs.clear();
        self
    }

    pub fn allow_cross_shard(mut self, allow_cross_shard: bool) -> Self {
        self.allow_cross_shard = allow_cross_shard;
        self
    }

    pub fn build(self) -> ExecutionConfiguration {
        self
    }

    pub fn is_cross_shard_allowed(&self) -> bool {
        self.allow_cross_shard
    }

    pub fn retry_config_list(&self) -> &[(RetryOn, RetryConfig)] {
        &self.retry_configs
    }

    /// The policy for the error or, failing that, for the first error in its `source()` chain
    /// that has one: Java's cause-chain walk.
    pub(crate) fn retry_config_for(&self, error: &Error) -> Option<(RetryOn, RetryConfig)> {
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
        let mut depth = 0;
        while let Some(error) = current {
            if let Some(found) = self.retry_configs.iter().find(|(on, _)| on.matches(error)) {
                return Some(*found);
            }
            depth += 1;
            if depth > 64 {
                return None;
            }
            current = error.source();
        }
        None
    }
}
