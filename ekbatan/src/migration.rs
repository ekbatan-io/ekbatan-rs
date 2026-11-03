//! Java's `FlywayMigrator`: brings each shard's database up to date when the app starts, from
//! SQL files named as Flyway names them (`V0001__eventlog.sql`). sqlx's migrator does the
//! work: the history table (`_sqlx_migrations`), a checksum per file, and a database lock so
//! two instances starting together don't both migrate.
//!
//! Each file runs in its own transaction, except one that can't (PostgreSQL's
//! `CREATE INDEX CONCURRENTLY`, ...): mark it with a first line `-- no-transaction` (sqlx's
//! marker), or with Flyway's `V0005__name.sql.conf` beside it holding
//! `executeInTransaction=false`. (MySQL and MariaDB commit each DDL statement on their own
//! anyway, transaction or not.)
//!
//! ```ignore
//! Migrator::builder()
//!     .with_target(Migrator::target(ShardIdentifier::new(0, 0), config))
//!     .locations(["migrations"])
//!     .migrate()
//!     .await?;
//! ```

use std::borrow::Cow;
use std::fmt;
use std::path::{Path, PathBuf};

use sqlx::migrate::{Migrate, Migration, MigrationType, Migrator as SqlxMigrator};
use tracing::{error, info};

use crate::connection::Pool;
use crate::{ConnectionProvider, DataSourceConfig, Error, ShardIdentifier};

/// Where `migrate` reads files when no location is given: Java's `classpath:db/migration`.
pub const DEFAULT_LOCATION: &str = "migrations";

/// One database to migrate: Java's `FlywayMigrator.Target`.
#[derive(Clone, Debug)]
pub struct MigrationTarget {
    pub shard: ShardIdentifier,
    pub name: String,
    pub data_source_config: DataSourceConfig,
}

impl MigrationTarget {
    fn label(&self) -> String {
        format!("{} ({}:{})", self.name, self.shard.group, self.shard.member)
    }
}

/// What one target's migration did: Java's `FlywayMigrator.Result`.
#[derive(Clone, Debug)]
pub struct MigrationResult {
    pub target: MigrationTarget,
    /// The newest version applied before, if any.
    pub initial_version: Option<i64>,
    /// The newest version applied now, if any.
    pub target_version: Option<i64>,
    pub migrations_executed: usize,
}

/// Where migration files come from: a folder read at run time, or files built into the binary
/// with `embed_migrations!`.
#[derive(Clone, Debug)]
pub enum MigrationLocation {
    Dir(PathBuf),
    Embedded(&'static [(&'static str, &'static str)]),
}

impl<P: AsRef<Path>> From<P> for MigrationLocation {
    fn from(path: P) -> Self {
        MigrationLocation::Dir(path.as_ref().to_path_buf())
    }
}

/// Java's `FlywayMigrator`.
pub struct Migrator;

impl Migrator {
    pub fn builder() -> MigratorBuilder {
        MigratorBuilder {
            targets: Vec::new(),
            locations: vec![MigrationLocation::from(DEFAULT_LOCATION)],
        }
    }

    /// Migrates one database, as the default shard.
    pub async fn migrate(
        data_source_config: &DataSourceConfig,
        locations: impl IntoIterator<Item = impl Into<MigrationLocation>>,
    ) -> Result<MigrationResult, Error> {
        let mut results = Migrator::builder()
            .with_data_source(data_source_config.clone())
            .locations(locations)
            .migrate()
            .await?;
        Ok(results.remove(0))
    }

    /// The default shard's database, named `default`.
    pub fn default_target(data_source_config: DataSourceConfig) -> MigrationTarget {
        Migrator::named_target(ShardIdentifier::DEFAULT, "default", data_source_config)
    }

    /// One shard's database, named `shard-<group>-<member>`.
    pub fn target(shard: ShardIdentifier, data_source_config: DataSourceConfig) -> MigrationTarget {
        let name = format!("shard-{}-{}", shard.group, shard.member);
        Migrator::named_target(shard, &name, data_source_config)
    }

    pub fn named_target(
        shard: ShardIdentifier,
        name: &str,
        data_source_config: DataSourceConfig,
    ) -> MigrationTarget {
        MigrationTarget {
            shard,
            name: name.to_owned(),
            data_source_config,
        }
    }
}

/// Java's `FlywayMigrator.Builder`.
pub struct MigratorBuilder {
    targets: Vec<MigrationTarget>,
    locations: Vec<MigrationLocation>,
}

impl MigratorBuilder {
    pub fn with_data_source(self, data_source_config: DataSourceConfig) -> Self {
        self.with_target(Migrator::default_target(data_source_config))
    }

    pub fn with_target(mut self, target: MigrationTarget) -> Self {
        self.targets.push(target);
        self
    }

    pub fn with_targets(mut self, targets: impl IntoIterator<Item = MigrationTarget>) -> Self {
        self.targets.extend(targets);
        self
    }

    /// Replaces the locations (default: the `migrations` folder). Files from all of them are
    /// applied together, in version order.
    pub fn locations(
        mut self,
        locations: impl IntoIterator<Item = impl Into<MigrationLocation>>,
    ) -> Self {
        self.locations = locations.into_iter().map(Into::into).collect();
        self
    }

    /// Migrates each target in turn and stops at the first failure, saying which targets were
    /// already migrated: a cluster can be left half-migrated, and the error alone wouldn't say
    /// which half.
    pub async fn migrate(self) -> Result<Vec<MigrationResult>, Error> {
        if self.targets.is_empty() {
            return Err(Error::Config(
                "at least one migration target is required".to_owned(),
            ));
        }
        if self.locations.is_empty() {
            return Err(Error::Config(
                "at least one migration location is required".to_owned(),
            ));
        }
        let migrator = SqlxMigrator::with_migrations(load(&self.locations)?);
        let mut results: Vec<MigrationResult> = Vec::new();
        for target in self.targets {
            let label = target.label();
            info!("Migrating {label}");
            match migrate_one(&migrator, &target).await {
                Ok((initial_version, target_version, migrations_executed)) => {
                    let version = target_version
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "<< empty >>".to_owned());
                    info!("{label} is at version {version} ({migrations_executed} applied)");
                    results.push(MigrationResult {
                        target,
                        initial_version,
                        target_version,
                        migrations_executed,
                    });
                }
                Err(failure) => {
                    error!("Migrating {label} failed");
                    let done: Vec<&str> = results.iter().map(|r| &*r.target.name).collect();
                    error!("Stopped at {label}; already migrated: {done:?}");
                    return Err(failure);
                }
            }
        }
        Ok(results)
    }
}

async fn migrate_one(
    migrator: &SqlxMigrator,
    target: &MigrationTarget,
) -> Result<(Option<i64>, Option<i64>, usize), Error> {
    let config = DataSourceConfig {
        maximum_pool_size: 1,
        minimum_idle: None,
        ..target.data_source_config.clone()
    };
    let provider = ConnectionProvider::from_config(&config)?;
    let fail = |source: sqlx::migrate::MigrateError| Error::Migration {
        target: target.label(),
        source,
    };
    let outcome = match provider.pool() {
        Pool::Postgres(pool) => {
            let mut connection = pool.acquire().await?;
            let before = applied(&mut *connection, migrator).await.map_err(fail)?;
            migrator
                .run_direct(None, &mut *connection, false)
                .await
                .map_err(fail)?;
            let after = applied(&mut *connection, migrator).await.map_err(fail)?;
            (before, after)
        }
        Pool::MySql(pool) => {
            let mut connection = pool.acquire().await?;
            let before = applied(&mut *connection, migrator).await.map_err(fail)?;
            migrator
                .run_direct(None, &mut *connection, false)
                .await
                .map_err(fail)?;
            let after = applied(&mut *connection, migrator).await.map_err(fail)?;
            (before, after)
        }
    };
    provider.close().await;
    let (before, after) = outcome;
    Ok((
        before.iter().copied().max(),
        after.iter().copied().max(),
        after.len().saturating_sub(before.len()),
    ))
}

/// The versions the database has applied.
async fn applied<C: Migrate>(
    connection: &mut C,
    migrator: &SqlxMigrator,
) -> Result<Vec<i64>, sqlx::migrate::MigrateError> {
    connection
        .ensure_migrations_table(&migrator.table_name)
        .await?;
    Ok(connection
        .list_applied_migrations(&migrator.table_name)
        .await?
        .into_iter()
        .map(|m| m.version)
        .collect())
}

/// Every migration in the locations, checked: Flyway names, one file per version.
fn load(locations: &[MigrationLocation]) -> Result<Vec<Migration>, Error> {
    // (file name, SQL, where it came from, Flyway's executeInTransaction=false)
    let mut files: Vec<(String, Cow<'static, str>, String, bool)> = Vec::new();
    for location in locations {
        match location {
            MigrationLocation::Dir(dir) => {
                let entries = std::fs::read_dir(dir).map_err(|e| {
                    Error::Config(format!(
                        "can't read the migration folder {}: {e}",
                        dir.display()
                    ))
                })?;
                for entry in entries {
                    let path = entry
                        .map_err(|e| Error::Config(format!("{}: {e}", dir.display())))?
                        .path();
                    if path.extension().and_then(|e| e.to_str()) != Some("sql") {
                        continue;
                    }
                    let name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_else(|| {
                            Error::Config(format!("{} is not a UTF-8 file name", path.display()))
                        })?
                        .to_owned();
                    let sql = std::fs::read_to_string(&path)
                        .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
                    let conf = dir.join(format!("{name}.conf"));
                    let outside_transaction = match std::fs::read_to_string(&conf) {
                        Ok(text) => conf_says_no_transaction(&text),
                        Err(_) => false,
                    };
                    files.push((
                        name,
                        Cow::Owned(sql),
                        path.display().to_string(),
                        outside_transaction,
                    ));
                }
            }
            MigrationLocation::Embedded(embedded) => {
                for (name, sql) in embedded.iter() {
                    if name.ends_with(".sql.conf") {
                        continue;
                    }
                    let outside_transaction = embedded
                        .iter()
                        .find(|(other, _)| *other == format!("{name}.conf"))
                        .is_some_and(|(_, conf)| conf_says_no_transaction(conf));
                    files.push((
                        (*name).to_owned(),
                        Cow::Borrowed(sql),
                        (*name).to_owned(),
                        outside_transaction,
                    ));
                }
            }
        }
    }
    let mut migrations: Vec<(Migration, String)> = Vec::new();
    for (name, sql, origin, outside_transaction) in files {
        let no_tx = outside_transaction || sql.starts_with("-- no-transaction");
        let (version, description) = parse_name(&name)?;
        if let Some((_, other)) = migrations.iter().find(|(m, _)| m.version == version) {
            return Err(Error::Config(format!(
                "Found more than one migration with version {version}: {other} and {origin}"
            )));
        }
        let sql = match sql {
            Cow::Borrowed(text) => sqlx::SqlStr::from_static(text),
            Cow::Owned(text) => sqlx::SqlSafeStr::into_sql_str(sqlx::AssertSqlSafe(text)),
        };
        migrations.push((
            Migration::new(
                version,
                Cow::Owned(description),
                MigrationType::Simple,
                sql,
                no_tx,
            ),
            origin,
        ));
    }
    Ok(migrations.into_iter().map(|(m, _)| m).collect())
}

/// Flyway's per-script config: `executeInTransaction=false` runs the script outside a
/// transaction.
fn conf_says_no_transaction(conf: &str) -> bool {
    conf.lines().any(|line| {
        line.split_once('=').is_some_and(|(key, value)| {
            key.trim() == "executeInTransaction" && value.trim().eq_ignore_ascii_case("false")
        })
    })
}

/// `V0001__create_wallets.sql` -> (1, "create wallets"), as Flyway reads it. Only whole-number
/// versions: sqlx keeps a version as one integer.
fn parse_name(name: &str) -> Result<(i64, String), Error> {
    let invalid = |why: &str| {
        Error::Config(format!(
            "migration file {name:?} {why}: name it V<number>__<description>.sql, as \
             V0001__create_wallets.sql"
        ))
    };
    let stem = name
        .strip_suffix(".sql")
        .ok_or_else(|| invalid("is not a .sql file"))?;
    let rest = stem.strip_prefix('V').ok_or_else(|| {
        invalid("doesn't start with V (repeatable and undo migrations aren't supported)")
    })?;
    let (version, description) = rest
        .split_once("__")
        .ok_or_else(|| invalid("has no __ between version and description"))?;
    if version.is_empty() || !version.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid("has a version that isn't a whole number"));
    }
    let version: i64 = version
        .parse()
        .map_err(|_| invalid("has a version too large"))?;
    Ok((version, description.replace('_', " ")))
}

impl fmt::Display for MigrationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} applied, now at {:?}",
            self.target.label(),
            self.migrations_executed,
            self.target_version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::parse_name;

    #[test]
    fn flyway_names_are_read_as_flyway_reads_them() {
        assert_eq!(
            parse_name("V0001__eventlog.sql").unwrap(),
            (1, "eventlog".to_owned())
        );
        assert_eq!(
            parse_name("V12__create_wallet_table.sql").unwrap(),
            (12, "create wallet table".to_owned())
        );
        for bad in [
            "V1_1__x.sql",
            "V1.1__x.sql",
            "R__view.sql",
            "U1__undo.sql",
            "V1_x.sql",
            "V__x.sql",
            "0001_x.sql",
        ] {
            assert!(parse_name(bad).is_err(), "{bad}");
        }
    }
}
