//! Java's `JobsConfig`: the `ekbatan.jobs` section.

use std::time::Duration;

use serde_json::Value;

use crate::{Error, canonical_key};

/// `ekbatan.jobs`: Java's `JobsConfig`. Durations are ISO-8601 text, as Java's
/// `Duration.parse` reads them (`PT10S`, `PT0.5S`, `PT1M`); unset ones keep the defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobsConfig {
    pub polling_interval: Option<Duration>,
    pub heartbeat_interval: Option<Duration>,
    pub shutdown_max_wait: Option<Duration>,
}

impl JobsConfig {
    /// Reads the section as `EkbatanConfig::jobs` holds it.
    pub fn from_value(value: &Value) -> Result<JobsConfig, Error> {
        let map = value
            .as_object()
            .ok_or_else(|| Error::Config("ekbatan.jobs must be a map".to_owned()))?;
        let mut config = JobsConfig::default();
        for (key, value) in map {
            let name = canonical_key(key);
            let path = format!("ekbatan.jobs.{key}");
            let duration = match value.as_str() {
                Some(text) => parse_iso_duration(text).ok_or_else(|| {
                    Error::Config(format!(
                        "{path} must be an ISO-8601 duration such as PT10S or PT0.5S"
                    ))
                })?,
                None => {
                    return Err(Error::Config(format!(
                        "{path} must be an ISO-8601 duration such as PT10S (a bare number is \
                         refused: is it seconds or millis?)"
                    )));
                }
            };
            match name.as_str() {
                "pollingInterval" => config.polling_interval = Some(duration),
                "heartbeatInterval" => config.heartbeat_interval = Some(duration),
                "shutdownMaxWait" => config.shutdown_max_wait = Some(duration),
                _ => {
                    return Err(Error::Config(format!(
                        "{path} means nothing here; known keys: pollingInterval, \
                         heartbeatInterval, shutdownMaxWait"
                    )));
                }
            }
        }
        for (name, value) in [
            ("polling-interval", config.polling_interval),
            ("heartbeat-interval", config.heartbeat_interval),
        ] {
            if value.is_some_and(|d| d.is_zero()) {
                return Err(Error::Config(format!(
                    "ekbatan.jobs.{name} must be positive: zero polls in a tight loop"
                )));
            }
        }
        Ok(config)
    }
}

/// Java's `Duration.parse` for what configs use: `P[nD][T[nH][nM][n[.n]S]]`.
pub fn parse_iso_duration(text: &str) -> Option<Duration> {
    let upper = text.trim().to_ascii_uppercase();
    let rest = upper.strip_prefix('P')?;
    let (days, time) = match rest.split_once('T') {
        Some((days, time)) => (days, Some(time)),
        None => (rest, None),
    };
    let mut total = 0f64;
    let mut any = false;
    if !days.is_empty() {
        total += days.strip_suffix('D')?.parse::<f64>().ok()? * 86_400.0;
        any = true;
    }
    if let Some(mut time) = time {
        if time.is_empty() {
            return None;
        }
        for (unit, seconds) in [('H', 3600.0), ('M', 60.0), ('S', 1.0)] {
            if let Some(at) = time.find(unit) {
                let number: f64 = time[..at].parse().ok()?;
                total += number * seconds;
                time = &time[at + 1..];
                any = true;
            }
        }
        if !time.is_empty() {
            return None;
        }
    }
    if !any || total < 0.0 || !total.is_finite() {
        return None;
    }
    Some(Duration::from_secs_f64(total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn iso_durations_as_java_reads_them() {
        assert_eq!(parse_iso_duration("PT10S"), Some(Duration::from_secs(10)));
        assert_eq!(
            parse_iso_duration("PT0.5S"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_iso_duration("PT1M30S"), Some(Duration::from_secs(90)));
        assert_eq!(
            parse_iso_duration("P1DT1H"),
            Some(Duration::from_secs(90_000))
        );
        for bad in ["10s", "10", "PT", "P", "PTS", "PT10X"] {
            assert_eq!(parse_iso_duration(bad), None, "{bad}");
        }
    }

    #[test]
    fn the_section_reads_with_kebab_keys_and_refuses_bare_numbers() {
        let config =
            JobsConfig::from_value(&json!({"polling-interval": "PT1S", "shutdownMaxWait": "PT0S"}))
                .unwrap();
        assert_eq!(config.polling_interval, Some(Duration::from_secs(1)));
        assert_eq!(config.shutdown_max_wait, Some(Duration::ZERO));
        assert!(JobsConfig::from_value(&json!({"pollingInterval": 10})).is_err());
        assert!(JobsConfig::from_value(&json!({"pollingInterval": "PT0S"})).is_err());
        assert!(JobsConfig::from_value(&json!({"threads": "PT1S"})).is_err());
    }
}
