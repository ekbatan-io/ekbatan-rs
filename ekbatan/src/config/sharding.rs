//! Java's `ShardingConfig`, `ShardGroupConfig` and `ShardMemberConfig`, and the `ekbatan:`
//! section of an app's config, read the way Java reads it: the same keys, kebab-case or
//! camelCase (Java's `PropertyKeyNormalizer`), `${VAR}` / `${VAR:default}` from the environment
//! (Spring's syntax; `$${` is a literal `${`). So one config file serves both.
//!
//! The section is read from a parsed tree (`serde_json::Value`): a JSON file with
//! `EkbatanConfig::load_json`, or any format parsed into that tree.
//!
//! No error repeats a value - each names the key's path, or the variable.

use std::path::Path;
use std::time::Duration;

use serde_json::{Map, Value};

use crate::{DataSourceConfig, Error, ShardIdentifier, UnknownShardPolicy};

/// One shard's databases, by name: `primaryConfig` (required), `secondaryConfig` (a replica),
/// and any others the app names (Java's examples: `jobsConfig`, `lockConfig`). Java's
/// `ShardMemberConfig`.
#[derive(Clone, Debug)]
pub struct ShardMemberConfig {
    pub member: u8,
    pub name: Option<String>,
    pub configs: Vec<(String, DataSourceConfig)>,
}

impl ShardMemberConfig {
    pub const PRIMARY_CONFIG: &'static str = "primaryConfig";
    pub const SECONDARY_CONFIG: &'static str = "secondaryConfig";

    pub fn primary_config(&self) -> &DataSourceConfig {
        self.config_for(Self::PRIMARY_CONFIG)
            .expect("checked when built: configs.primaryConfig is required")
    }

    pub fn secondary_config(&self) -> Option<&DataSourceConfig> {
        self.config_for(Self::SECONDARY_CONFIG)
    }

    pub fn config_for(&self, name: &str) -> Option<&DataSourceConfig> {
        self.configs
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, config)| config)
    }

    /// Java's checks: `configs.primaryConfig` is required.
    pub fn validate(&self) -> Result<(), Error> {
        if self.config_for(Self::PRIMARY_CONFIG).is_none() {
            return Err(Error::Config(
                "configs.primaryConfig is required".to_owned(),
            ));
        }
        for (_, config) in &self.configs {
            config.validate()?;
        }
        Ok(())
    }
}

/// A group of shards: Java's `ShardGroupConfig`.
#[derive(Clone, Debug)]
pub struct ShardGroupConfig {
    pub group: u8,
    pub name: String,
    pub members: Vec<ShardMemberConfig>,
}

impl ShardGroupConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if self.name.trim().is_empty() {
            return Err(Error::Config("name is required".to_owned()));
        }
        if self.members.is_empty() {
            return Err(Error::Config("at least one member is required".to_owned()));
        }
        for (index, member) in self.members.iter().enumerate() {
            if self.members[..index]
                .iter()
                .any(|m| m.member == member.member)
            {
                return Err(Error::Config(format!(
                    "member {} appears twice in group {}",
                    member.member, self.group
                )));
            }
            member.validate()?;
        }
        Ok(())
    }
}

/// Every shard and its databases: Java's `ShardingConfig`.
#[derive(Clone, Debug)]
pub struct ShardingConfig {
    pub default_shard: ShardIdentifier,
    pub groups: Vec<ShardGroupConfig>,
    pub unknown_shard_policy: UnknownShardPolicy,
}

impl ShardingConfig {
    /// One database as the only shard, the default one.
    pub fn single(primary: DataSourceConfig) -> ShardingConfig {
        ShardingConfig {
            default_shard: ShardIdentifier::DEFAULT,
            groups: vec![ShardGroupConfig {
                group: 0,
                name: "default".to_owned(),
                members: vec![ShardMemberConfig {
                    member: 0,
                    name: None,
                    configs: vec![(ShardMemberConfig::PRIMARY_CONFIG.to_owned(), primary)],
                }],
            }],
            unknown_shard_policy: UnknownShardPolicy::UseDefaultShard,
        }
    }

    /// Java's checks, plus: the default shard must be one of the configured ones, and no group
    /// or member twice.
    pub fn validate(&self) -> Result<(), Error> {
        if self.groups.is_empty() {
            return Err(Error::Config("at least one group is required".to_owned()));
        }
        for (index, group) in self.groups.iter().enumerate() {
            if self.groups[..index].iter().any(|g| g.group == group.group) {
                return Err(Error::Config(format!(
                    "group {} appears twice",
                    group.group
                )));
            }
            group.validate()?;
        }
        if self.member(self.default_shard).is_none() {
            return Err(Error::Config(format!(
                "defaultShard {} is not one of the configured shards",
                self.default_shard
            )));
        }
        Ok(())
    }

    /// Every shard with its member config, in the order configured.
    pub fn shards(
        &self,
    ) -> impl Iterator<Item = (ShardIdentifier, &ShardGroupConfig, &ShardMemberConfig)> {
        self.groups.iter().flat_map(|group| {
            group.members.iter().map(move |member| {
                (
                    ShardIdentifier::new(group.group, member.member),
                    group,
                    member,
                )
            })
        })
    }

    pub fn member(&self, shard: ShardIdentifier) -> Option<&ShardMemberConfig> {
        self.shards()
            .find(|(s, _, _)| *s == shard)
            .map(|(_, _, member)| member)
    }
}

/// The `ekbatan:` section of an app's config: Java's `EkbatanProperties`. `jobs` and
/// `local-event-handler` are kept as read until their parts of ekbatan read them.
#[derive(Clone, Debug)]
pub struct EkbatanConfig {
    pub namespace: Option<String>,
    pub sharding: Option<ShardingConfig>,
    pub jobs: Option<Value>,
    pub local_event_handler: Option<Value>,
}

impl EkbatanConfig {
    /// Reads `ekbatan` from a JSON file.
    pub fn load_json(path: impl AsRef<Path>) -> Result<EkbatanConfig, Error> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("can't read {}: {e}", path.display())))?;
        // serde_json's message gives a line and column, never the text there.
        let root: Value = serde_json::from_str(&text).map_err(|e| {
            Error::Config(format!(
                "{} is not valid JSON (line {}, column {})",
                path.display(),
                e.line(),
                e.column()
            ))
        })?;
        EkbatanConfig::from_tree(&root)
    }

    /// Reads `ekbatan` from a parsed config tree, filling in `${VAR}` from the environment.
    pub fn from_tree(root: &Value) -> Result<EkbatanConfig, Error> {
        EkbatanConfig::from_tree_with_env(root, &|name| std::env::var(name).ok())
    }

    /// As `from_tree`, with the environment given (for tests).
    pub fn from_tree_with_env(
        root: &Value,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<EkbatanConfig, Error> {
        let root = object(root, "")?;
        let Some(section) = get(root, "ekbatan") else {
            return Err(Error::Config(
                "the config has no ekbatan section".to_owned(),
            ));
        };
        let reader = Reader { env };
        let section = object(section, "ekbatan")?;
        reader.refuse_unknown(
            section,
            "ekbatan",
            &["namespace", "sharding", "jobs", "localEventHandler"],
        )?;
        Ok(EkbatanConfig {
            namespace: get(section, "namespace")
                .map(|v| reader.text(v, "ekbatan.namespace"))
                .transpose()?,
            sharding: get(section, "sharding")
                .map(|v| reader.sharding(v, "ekbatan.sharding"))
                .transpose()?,
            jobs: get(section, "jobs").cloned(),
            local_event_handler: get(section, "localEventHandler").cloned(),
        })
    }
}

/// Java's `PropertyKeyNormalizer.kebabToCamel`, and snake_case too: `default-shard`,
/// `default_shard` and `defaultShard` are one key.
pub fn canonical_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    let mut upper = false;
    for (index, c) in key.chars().enumerate() {
        if (c == '-' || c == '_') && index > 0 {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn get<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(k, _)| canonical_key(k) == key)
        .map(|(_, v)| v)
}

fn object<'a>(value: &'a Value, path: &str) -> Result<&'a Map<String, Value>, Error> {
    value.as_object().ok_or_else(|| {
        Error::Config(format!(
            "{} must be a map",
            if path.is_empty() { "the config" } else { path }
        ))
    })
}

struct Reader<'e> {
    env: &'e dyn Fn(&str) -> Option<String>,
}

impl Reader<'_> {
    fn refuse_unknown(
        &self,
        map: &Map<String, Value>,
        path: &str,
        known: &[&str],
    ) -> Result<(), Error> {
        for key in map.keys() {
            if !known.contains(&canonical_key(key).as_str()) {
                return Err(Error::Config(format!(
                    "{path}.{key} means nothing here; known keys: {}",
                    known.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// Text, with `${VAR}` / `${VAR:default}` filled in. Numbers and booleans are written as
    /// their text, as a driver reads its settings.
    fn text(&self, value: &Value, path: &str) -> Result<String, Error> {
        match value {
            Value::String(text) => self.fill(text, path),
            Value::Number(number) => Ok(number.to_string()),
            Value::Bool(flag) => Ok(flag.to_string()),
            _ => Err(Error::Config(format!("{path} must be a text value"))),
        }
    }

    fn whole_number(&self, value: &Value, path: &str) -> Result<u64, Error> {
        let text = match value {
            Value::Number(number) => number.to_string(),
            Value::String(_) => self.text(value, path)?,
            _ => String::new(),
        };
        text.trim()
            .parse()
            .map_err(|_| Error::Config(format!("{path} must be a whole number")))
    }

    fn small(&self, value: &Value, path: &str, max: u64) -> Result<u8, Error> {
        let number = self.whole_number(value, path)?;
        if number > max {
            return Err(Error::Config(format!("{path} must be between 0 and {max}")));
        }
        Ok(number as u8)
    }

    fn millis(&self, value: &Value, path: &str) -> Result<Duration, Error> {
        Ok(Duration::from_millis(self.whole_number(value, path)?))
    }

    /// Spring's placeholders. An unset variable without a default stops the load, naming the
    /// variable; a filled-in value is used as it is and never read again.
    fn fill(&self, text: &str, path: &str) -> Result<String, Error> {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(at) = rest.find('$') {
            out.push_str(&rest[..at]);
            let after = &rest[at..];
            if let Some(literal) = after.strip_prefix("$${") {
                out.push_str("${");
                rest = literal;
            } else if let Some(open) = after.strip_prefix("${") {
                let Some(close) = open.find('}') else {
                    return Err(Error::Config(format!(
                        "{path} has a ${{ with no closing }}"
                    )));
                };
                let inner = &open[..close];
                let (name, default) = match inner.split_once(':') {
                    Some((name, default)) => (name, Some(default)),
                    None => (inner, None),
                };
                match ((self.env)(name), default) {
                    (Some(value), _) => out.push_str(&value),
                    (None, Some(default)) => out.push_str(default),
                    (None, None) => {
                        return Err(Error::Config(format!(
                            "{path} uses ${{{name}}}, which is not set and has no default"
                        )));
                    }
                }
                rest = &open[close + 1..];
            } else {
                out.push('$');
                rest = &after[1..];
            }
        }
        out.push_str(rest);
        Ok(out)
    }

    fn sharding(&self, value: &Value, path: &str) -> Result<ShardingConfig, Error> {
        let map = object(value, path)?;
        self.refuse_unknown(map, path, &["defaultShard", "groups", "unknownShardPolicy"])?;
        let default_shard = match get(map, "defaultShard") {
            Some(shard) => self.shard(shard, &format!("{path}.defaultShard"))?,
            None => return Err(Error::Config(format!("{path}.defaultShard is required"))),
        };
        let unknown_shard_policy = match get(map, "unknownShardPolicy") {
            None => UnknownShardPolicy::UseDefaultShard,
            Some(policy) => {
                let policy_path = format!("{path}.unknownShardPolicy");
                match self.text(policy, &policy_path)?.as_str() {
                    "USE_DEFAULT_SHARD" => UnknownShardPolicy::UseDefaultShard,
                    "FAIL" => UnknownShardPolicy::Fail,
                    _ => {
                        return Err(Error::Config(format!(
                            "{policy_path} must be USE_DEFAULT_SHARD or FAIL"
                        )));
                    }
                }
            }
        };
        let groups_path = format!("{path}.groups");
        let groups = match get(map, "groups") {
            Some(Value::Array(groups)) => groups
                .iter()
                .enumerate()
                .map(|(i, g)| self.group(g, &format!("{groups_path}[{i}]")))
                .collect::<Result<Vec<_>, _>>()?,
            Some(_) => return Err(Error::Config(format!("{groups_path} must be a list"))),
            None => Vec::new(),
        };
        let config = ShardingConfig {
            default_shard,
            groups,
            unknown_shard_policy,
        };
        config
            .validate()
            .map_err(|e| Error::Config(format!("{path}: {}", message(e))))?;
        Ok(config)
    }

    fn shard(&self, value: &Value, path: &str) -> Result<ShardIdentifier, Error> {
        let map = object(value, path)?;
        self.refuse_unknown(map, path, &["group", "member"])?;
        let group =
            get(map, "group").ok_or_else(|| Error::Config(format!("{path}.group is required")))?;
        let member = get(map, "member")
            .ok_or_else(|| Error::Config(format!("{path}.member is required")))?;
        Ok(ShardIdentifier::new(
            self.small(group, &format!("{path}.group"), 255)?,
            self.small(member, &format!("{path}.member"), 63)?,
        ))
    }

    fn group(&self, value: &Value, path: &str) -> Result<ShardGroupConfig, Error> {
        let map = object(value, path)?;
        self.refuse_unknown(map, path, &["group", "name", "members"])?;
        let group = match get(map, "group") {
            Some(g) => self.small(g, &format!("{path}.group"), 255)?,
            None => 0,
        };
        let name = match get(map, "name") {
            Some(n) => self.text(n, &format!("{path}.name"))?,
            None => return Err(Error::Config(format!("{path}.name is required"))),
        };
        let members_path = format!("{path}.members");
        let members = match get(map, "members") {
            Some(Value::Array(members)) => members
                .iter()
                .enumerate()
                .map(|(i, m)| self.member(m, &format!("{members_path}[{i}]")))
                .collect::<Result<Vec<_>, _>>()?,
            Some(_) => return Err(Error::Config(format!("{members_path} must be a list"))),
            None => Vec::new(),
        };
        let config = ShardGroupConfig {
            group,
            name,
            members,
        };
        config
            .validate()
            .map_err(|e| Error::Config(format!("{path}: {}", message(e))))?;
        Ok(config)
    }

    fn member(&self, value: &Value, path: &str) -> Result<ShardMemberConfig, Error> {
        let map = object(value, path)?;
        self.refuse_unknown(map, path, &["member", "name", "configs"])?;
        let member = match get(map, "member") {
            Some(m) => self.small(m, &format!("{path}.member"), 63)?,
            None => 0,
        };
        let name = get(map, "name")
            .map(|n| self.text(n, &format!("{path}.name")))
            .transpose()?;
        let configs_path = format!("{path}.configs");
        let mut configs = Vec::new();
        if let Some(value) = get(map, "configs") {
            for (key, config) in object(value, &configs_path)? {
                let name = canonical_key(key);
                let config_path = format!("{configs_path}.{name}");
                configs.push((name, self.data_source(config, &config_path)?));
            }
        }
        let config = ShardMemberConfig {
            member,
            name,
            configs,
        };
        config
            .validate()
            .map_err(|e| Error::Config(format!("{path}: {}", message(e))))?;
        Ok(config)
    }

    /// A datasource, by Java's keys. `driverClassName` is accepted and unused: sqlx picks its
    /// driver from the URL.
    fn data_source(&self, value: &Value, path: &str) -> Result<DataSourceConfig, Error> {
        let map = object(value, path)?;
        self.refuse_unknown(
            map,
            path,
            &[
                "jdbcUrl",
                "username",
                "password",
                "dataSourceProperties",
                "driverClassName",
                "maximumPoolSize",
                "minimumIdle",
                "idleTimeout",
                "connectionTimeout",
                "leakDetectionThreshold",
            ],
        )?;
        let url = match get(map, "jdbcUrl") {
            Some(url) => self.text(url, &format!("{path}.jdbcUrl"))?,
            None => return Err(Error::Config(format!("{path}.jdbcUrl is required"))),
        };
        let maximum_pool_size = match get(map, "maximumPoolSize") {
            Some(size) => self.whole_number(size, &format!("{path}.maximumPoolSize"))? as u32,
            None => 10,
        };
        let at = |e: Error| Error::Config(format!("{path}.jdbcUrl: {}", message(e)));
        let mut config = DataSourceConfig::from_jdbc_url(&url, maximum_pool_size).map_err(at)?;
        if let Some(username) = get(map, "username") {
            config.username = Some(self.text(username, &format!("{path}.username"))?);
        }
        if let Some(password) = get(map, "password") {
            config.password = Some(self.text(password, &format!("{path}.password"))?);
        }
        if let Some(minimum) = get(map, "minimumIdle") {
            config.minimum_idle =
                Some(self.whole_number(minimum, &format!("{path}.minimumIdle"))? as u32);
        }
        if let Some(idle) = get(map, "idleTimeout") {
            config.idle_timeout = Some(self.millis(idle, &format!("{path}.idleTimeout"))?);
        }
        if let Some(timeout) = get(map, "connectionTimeout") {
            config.connection_timeout =
                Some(self.millis(timeout, &format!("{path}.connectionTimeout"))?);
        }
        if let Some(threshold) = get(map, "leakDetectionThreshold") {
            config.leak_detection_threshold =
                Some(self.millis(threshold, &format!("{path}.leakDetectionThreshold"))?);
        }
        if let Some(properties) = get(map, "dataSourceProperties") {
            let properties_path = format!("{path}.dataSourceProperties");
            // A driver's own names, kept exactly as written.
            for (name, value) in object(properties, &properties_path)? {
                let value_path = format!("{properties_path}.{name}");
                if value.is_object() || value.is_array() {
                    return Err(Error::Config(format!(
                        "{value_path} must be a single value: no driver setting holds a map or a \
                         list (a dotted name split by mistake?)"
                    )));
                }
                config
                    .data_source_properties
                    .push((name.clone(), self.text(value, &value_path)?));
            }
        }
        config
            .validate()
            .map_err(|e| Error::Config(format!("{path}: {}", message(e))))?;
        Ok(config)
    }
}

fn message(error: Error) -> String {
    match error {
        Error::Config(message) => message,
        other => other.to_string(),
    }
}
