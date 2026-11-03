//! Java's `LocalEventHandlerConfig`: the `ekbatan.local-event-handler` section.

use std::time::Duration;

use serde_json::Value;

use crate::distributed_jobs::parse_iso_duration;
use crate::{Error, canonical_key};

/// `ekbatan.local-event-handler`: Java's `LocalEventHandlerConfig`. Unset values keep the jobs'
/// defaults; `handling.enabled` is off unless set, as in Java.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocalEventHandlerConfig {
    pub fanout_poll_delay: Option<Duration>,
    pub fanout_batch_size: Option<u64>,
    pub handling_poll_delay: Option<Duration>,
    pub handling_batch_size: Option<u64>,
    pub handling_max_backoff_cap: Option<Duration>,
    pub handling_retention_window: Option<Duration>,
    pub handling_enabled: bool,
}

impl LocalEventHandlerConfig {
    /// Reads the section as `EkbatanConfig::local_event_handler` holds it.
    pub fn from_value(value: &Value) -> Result<Self, Error> {
        let root = "ekbatan.local-event-handler";
        let map = value
            .as_object()
            .ok_or_else(|| Error::Config(format!("{root} must be a map")))?;
        let mut config = LocalEventHandlerConfig::default();
        for (key, value) in map {
            let path = format!("{root}.{key}");
            let duration = || -> Result<Duration, Error> {
                value
                    .as_str()
                    .and_then(parse_iso_duration)
                    .filter(|d| !d.is_zero())
                    .ok_or_else(|| {
                        Error::Config(format!(
                            "{path} must be a positive ISO-8601 duration such as PT1S"
                        ))
                    })
            };
            let count = || -> Result<u64, Error> {
                value
                    .as_u64()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| Error::Config(format!("{path} must be a positive number")))
            };
            match canonical_key(key).as_str() {
                "fanoutPollDelay" => config.fanout_poll_delay = Some(duration()?),
                "fanoutBatchSize" => config.fanout_batch_size = Some(count()?),
                "handlingPollDelay" => config.handling_poll_delay = Some(duration()?),
                "handlingBatchSize" => config.handling_batch_size = Some(count()?),
                "handlingMaxBackoffCap" => config.handling_max_backoff_cap = Some(duration()?),
                "handlingRetentionWindow" => config.handling_retention_window = Some(duration()?),
                "handling" => {
                    let handling = value
                        .as_object()
                        .ok_or_else(|| Error::Config(format!("{path} must be a map")))?;
                    for (key, value) in handling {
                        if canonical_key(key) != "enabled" {
                            return Err(Error::Config(format!(
                                "{path}.{key} means nothing here; known keys: enabled"
                            )));
                        }
                        config.handling_enabled = value.as_bool().ok_or_else(|| {
                            Error::Config(format!("{path}.{key} must be true or false"))
                        })?;
                    }
                }
                _ => {
                    return Err(Error::Config(format!(
                        "{path} means nothing here; known keys: fanoutPollDelay, \
                         fanoutBatchSize, handlingPollDelay, handlingBatchSize, \
                         handlingMaxBackoffCap, handlingRetentionWindow, handling"
                    )));
                }
            }
        }
        Ok(config)
    }

    /// The fan-out job, with these settings.
    pub fn fanout_job(&self, job: super::EventFanoutJob) -> super::EventFanoutJob {
        let mut job = job;
        if let Some(delay) = self.fanout_poll_delay {
            job = job.poll_delay(delay);
        }
        if let Some(size) = self.fanout_batch_size {
            job = job.batch_size(size);
        }
        job
    }

    /// The handling job, with these settings.
    pub fn handling_job(&self, job: super::EventHandlingJob) -> super::EventHandlingJob {
        let mut job = job;
        if let Some(delay) = self.handling_poll_delay {
            job = job.poll_delay(delay);
        }
        if let Some(size) = self.handling_batch_size {
            job = job.batch_size(size);
        }
        if let Some(cap) = self.handling_max_backoff_cap {
            job = job.max_backoff_cap(cap);
        }
        if let Some(window) = self.handling_retention_window {
            job = job.retention_window(window);
        }
        job
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn javas_section_reads() {
        let config = LocalEventHandlerConfig::from_value(&json!({
            "handling": { "enabled": true },
            "fanout-poll-delay": "PT0.5S",
            "handlingBatchSize": 10
        }))
        .unwrap();
        assert!(config.handling_enabled);
        assert_eq!(config.fanout_poll_delay, Some(Duration::from_millis(500)));
        assert_eq!(config.handling_batch_size, Some(10));
        assert!(LocalEventHandlerConfig::from_value(&json!({"handlingBatchSize": 0})).is_err());
        assert!(LocalEventHandlerConfig::from_value(&json!({"fanoutPollDelay": 5})).is_err());
        assert!(LocalEventHandlerConfig::from_value(&json!({"handling": {"on": true}})).is_err());
    }
}
