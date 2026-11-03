//! The migrator on real databases: Java's migration files, per shard, run by the app.

mod common;

use std::path::{Path, PathBuf};

use ekbatan::migration::{MigrationLocation, Migrator};
use ekbatan::{Dialect, Error, embed_migrations};

use common::{Cluster, MEXICO, start};

#[tokio::test(flavor = "multi_thread")]
async fn on_postgres() {
    run(Dialect::Postgres).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mysql() {
    run(Dialect::MySql).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn on_mariadb() {
    run(Dialect::MariaDb).await;
}

fn targets(cluster: &Cluster) -> Vec<ekbatan::migration::MigrationTarget> {
    cluster
        .shards
        .iter()
        .map(|s| Migrator::target(s.shard, s.server.config(&s.database)))
        .collect()
}

/// A fresh folder under the test's target dir, with these files.
fn folder(name: &str, dialect: Dialect, files: &[(&str, &str)]) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("migrations-{dialect:?}-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (file, text) in files {
        std::fs::write(dir.join(file), text).unwrap();
    }
    dir
}

fn embedded(dialect: Dialect) -> MigrationLocation {
    match dialect {
        Dialect::Postgres => embed_migrations!("migrations/postgres"),
        Dialect::MySql => embed_migrations!("migrations/mysql"),
        Dialect::MariaDb => embed_migrations!("migrations/mariadb"),
    }
}

async fn run(dialect: Dialect) {
    let cluster = start(dialect).await;
    let say = |what: &str| println!("[{dialect:?}] {what}");
    let files = if dialect == Dialect::Postgres { 4 } else { 5 };

    // First run: every file, on every shard, in order.
    let results = Migrator::builder()
        .with_targets(targets(&cluster))
        .locations([cluster.migrations()])
        .migrate()
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    for result in &results {
        assert_eq!(result.migrations_executed, files);
        assert_eq!(result.initial_version, None);
        assert_eq!(result.target_version, Some(4));
    }
    assert_eq!(results[1].target.name, "shard-1-0");
    say("first run: Java's files applied on both shards");

    // Again: nothing to do. And the same files built into the binary are the same migrations
    // (a changed checksum would be refused).
    for location in [
        MigrationLocation::from(cluster.migrations()),
        embedded(dialect),
    ] {
        let again = Migrator::builder()
            .with_targets(targets(&cluster))
            .locations([location])
            .migrate()
            .await
            .unwrap();
        assert!(again.iter().all(|r| r.migrations_executed == 0
            && r.initial_version == Some(4)
            && r.target_version == Some(4)));
    }
    say("second run, from the folder and from the binary: nothing applied");

    // A file changed after it was applied is refused.
    let original = std::fs::read_dir(cluster.migrations()).unwrap();
    let mut copied: Vec<(String, String)> = original
        .map(|e| e.unwrap().path())
        .map(|p| {
            let name = p.file_name().unwrap().to_str().unwrap().to_owned();
            (name, std::fs::read_to_string(&p).unwrap())
        })
        .collect();
    for (name, text) in &mut copied {
        if name.starts_with("V0003") {
            text.push_str("\n-- changed\n");
        }
    }
    let borrowed: Vec<(&str, &str)> = copied.iter().map(|(n, t)| (&**n, &**t)).collect();
    let changed = Migrator::builder()
        .with_targets(targets(&cluster))
        .locations([folder("changed", dialect, &borrowed)])
        .migrate()
        .await;
    assert!(
        matches!(
            &changed,
            Err(Error::Migration {
                source: sqlx::migrate::MigrateError::VersionMismatch(3),
                ..
            })
        ),
        "{changed:?}"
    );
    say("a file changed after it was applied: refused");

    // Outside a transaction. On PostgreSQL, CREATE INDEX CONCURRENTLY fails inside one, so
    // first prove that, then run it marked both ways.
    let (concurrent_a, concurrent_b) = match dialect {
        Dialect::Postgres => (
            "CREATE INDEX CONCURRENTLY idx_wallets_currency ON wallets (currency);",
            "CREATE INDEX CONCURRENTLY idx_wallets_state ON wallets (state);",
        ),
        Dialect::MySql | Dialect::MariaDb => (
            "CREATE INDEX idx_wallets_currency ON wallets (currency);",
            "CREATE INDEX idx_wallets_state ON wallets (state);",
        ),
    };
    if dialect == Dialect::Postgres {
        let in_transaction = Migrator::builder()
            .with_target(Migrator::target(
                common::GLOBAL,
                cluster.config(common::GLOBAL),
            ))
            .locations([
                MigrationLocation::from(cluster.migrations()),
                folder("in-tx", dialect, &[("V0005__concurrent.sql", concurrent_a)]).into(),
            ])
            .migrate()
            .await;
        let message = format!("{:?}", in_transaction.unwrap_err());
        assert!(
            message.contains("cannot run inside a transaction block"),
            "{message}"
        );
        // sqlx records the failed attempt as dirty; clear it so the real run can go ahead.
        cluster
            .run_sql(
                common::GLOBAL,
                "DELETE FROM _sqlx_migrations WHERE version = 5",
            )
            .await;
    }
    let marked = format!("-- no-transaction\n{concurrent_a}");
    let outside = Migrator::builder()
        .with_targets(targets(&cluster))
        .locations([
            MigrationLocation::from(cluster.migrations()),
            folder(
                "no-tx",
                dialect,
                &[
                    ("V0005__concurrent_index.sql", &marked),
                    ("V0006__concurrent_index_flyway_conf.sql", concurrent_b),
                    (
                        "V0006__concurrent_index_flyway_conf.sql.conf",
                        "executeInTransaction=false\n",
                    ),
                ],
            )
            .into(),
        ])
        .migrate()
        .await
        .unwrap();
    assert!(
        outside
            .iter()
            .all(|r| r.migrations_executed == 2 && r.target_version == Some(6))
    );
    say("-- no-transaction and Flyway's executeInTransaction=false: run outside a transaction");

    // A failure on the second shard: the first stays migrated, the error names the shard.
    cluster
        .run_sql(MEXICO, "CREATE TABLE in_the_way (x INT)")
        .await;
    let failed = Migrator::builder()
        .with_targets(targets(&cluster))
        .locations([
            MigrationLocation::from(cluster.migrations()),
            folder("no-tx", dialect, &[]).into(),
            folder(
                "fails",
                dialect,
                &[
                    ("V0005__concurrent_index.sql", &marked),
                    ("V0006__concurrent_index_flyway_conf.sql", concurrent_b),
                    (
                        "V0006__concurrent_index_flyway_conf.sql.conf",
                        "executeInTransaction=false\n",
                    ),
                    ("V0007__in_the_way.sql", "CREATE TABLE in_the_way (x INT);"),
                ],
            )
            .into(),
        ])
        .migrate()
        .await;
    assert!(
        matches!(&failed, Err(Error::Migration { target, .. }) if target == "shard-1-0 (1:0)"),
        "{failed:?}"
    );
    say("a failure on the second shard: stops there and names it");

    // Names Flyway would read differently, and one version twice, are refused.
    for bad in ["V1_1__x.sql", "R__view.sql", "V2_x.sql"] {
        let refused = Migrator::builder()
            .with_targets(targets(&cluster))
            .locations([folder("bad", dialect, &[(bad, "SELECT 1;")])])
            .migrate()
            .await;
        assert!(
            matches!(refused, Err(Error::Config(_))),
            "{bad}: {refused:?}"
        );
    }
    let twice = Migrator::builder()
        .with_targets(targets(&cluster))
        .locations([
            folder("one", dialect, &[("V9__a.sql", "SELECT 1;")]),
            folder("two", dialect, &[("V9__b.sql", "SELECT 1;")]),
        ])
        .migrate()
        .await;
    assert!(
        matches!(&twice, Err(Error::Config(m)) if m.contains("more than one migration with version 9")),
        "{twice:?}"
    );
    say("bad names and a version twice: refused before touching a database");
}
