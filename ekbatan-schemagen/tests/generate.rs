//! The generator on real databases, from the wallet example's migrations.

use ekbatan::Dialect;
use ekbatan_schemagen::Schemagen;

fn migrations(folder: &str) -> String {
    format!(
        "{}/../wallet-example/migrations/{folder}",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// The module without its first line, which names the database.
fn body(code: &str) -> &str {
    code.split_once('\n').map_or(code, |(_, rest)| rest)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_three_databases_give_the_same_module() {
    let postgres = Schemagen::new(Dialect::Postgres).migrations(migrations("postgres"));
    let mysql = Schemagen::new(Dialect::MySql).migrations(migrations("mysql"));
    let mariadb = Schemagen::new(Dialect::MariaDb).migrations(migrations("mariadb"));
    // One after another: started together on one runtime, MySQL's container timed out (twice)
    // while alone it is ready in seconds.
    let postgres = postgres.generate_async().await.unwrap();
    let mysql = mysql.generate_async().await.unwrap();
    let mariadb = mariadb.generate_async().await.unwrap();
    println!("{postgres}");
    assert_eq!(body(&postgres), body(&mysql), "MySQL differs");
    assert_eq!(body(&postgres), body(&mariadb), "MariaDB differs");
    assert!(postgres.contains("pub struct WalletsRecord"));
    assert!(postgres.contains("pub owner_id: ::ekbatan::DbUuid,"));
    assert!(!postgres.contains("scheduled_tasks"));
}
