//! Configuration: Java's `io.ekbatan.core.config`.
//!
//! - `DataSourceConfig`: how to reach one database, its pool sizes, driver settings.
//! - `ShardingConfig` / `EkbatanConfig`: Java's `ekbatan:` section, read from a JSON tree or file.

mod data_source;
mod sharding;

pub use data_source::{DataSourceConfig, MYSQL_PROPERTIES, POSTGRES_PROPERTIES};
pub use sharding::{
    EkbatanConfig, ShardGroupConfig, ShardMemberConfig, ShardingConfig, canonical_key,
};
