//! The `ekbatan:` section, read as Java reads it: Java's sharded example config (as JSON), key
//! spellings, environment variables, and errors that never show a value.

use std::collections::HashMap;

use ekbatan::migration::Migrator;
use ekbatan::{DatabaseRegistry, Dialect, EkbatanConfig, ShardIdentifier, UnknownShardPolicy};
use serde_json::{Value, json};

const PASSWORD: &str = "Wallet-Secret-77";

/// Java's `spring-boot-wallet-rest-gradle-sharded-pg` application.yml, as JSON, with the
/// password from the environment and one pgjdbc-only property (`ApplicationName`) renamed to
/// sqlx's (`application_name`).
fn java_sharded_example() -> Value {
    let member = |name: &str, port: u16, db: &str| {
        json!({
            "member": 0,
            "name": name,
            "configs": {
                "primaryConfig": {
                    "dataSourceProperties": { "application_name": "wallet-service" },
                    "jdbcUrl": format!("jdbc:postgresql://localhost:{port}/{db}"),
                    "username": "wallet",
                    "password": "${DB_PASSWORD}",
                    "driverClassName": "org.postgresql.Driver",
                    "maximumPoolSize": 20
                },
                "jobsConfig": {
                    "jdbcUrl": format!("jdbc:postgresql://localhost:{port}/{db}"),
                    "username": "wallet",
                    "password": "${DB_PASSWORD}",
                    "maximumPoolSize": 5
                },
                "lockConfig": {
                    "jdbcUrl": format!("jdbc:postgresql://localhost:{port}/{db}"),
                    "username": "wallet",
                    "password": "${DB_PASSWORD}",
                    "maximumPoolSize": 30,
                    "leakDetectionThreshold": 0
                }
            }
        })
    };
    json!({
        "spring": { "application": { "name": "ignored" } },
        "ekbatan": {
            "namespace": "example.wallet",
            "sharding": {
                "defaultShard": { "group": 0, "member": 0 },
                "groups": [
                    { "group": 0, "name": "global", "members": [member("global", 5432, "wallet_global")] },
                    { "group": 1, "name": "mexico", "members": [member("mexico", 5433, "wallet_mexico")] }
                ]
            },
            "local-event-handler": { "handling": { "enabled": true } }
        }
    })
}

fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    move |name| vars.get(name).cloned()
}

fn read(tree: &Value) -> Result<EkbatanConfig, ekbatan::Error> {
    EkbatanConfig::from_tree_with_env(tree, &env(&[("DB_PASSWORD", PASSWORD)]))
}

#[tokio::test]
async fn javas_sharded_example_reads_as_java_reads_it() {
    let config = read(&java_sharded_example()).unwrap();
    assert_eq!(config.namespace.as_deref(), Some("example.wallet"));
    assert!(config.local_event_handler.is_some());
    let sharding = config.sharding.unwrap();
    assert_eq!(sharding.default_shard, ShardIdentifier::new(0, 0));
    assert_eq!(
        sharding.unknown_shard_policy,
        UnknownShardPolicy::UseDefaultShard
    );
    let mexico = sharding.member(ShardIdentifier::new(1, 0)).unwrap();
    let primary = mexico.primary_config();
    assert_eq!(primary.dialect, Dialect::Postgres);
    assert_eq!(primary.url, "postgres://localhost:5433/wallet_mexico");
    assert_eq!(primary.password.as_deref(), Some(PASSWORD));
    assert_eq!(primary.maximum_pool_size, 20);
    assert_eq!(
        mexico.config_for("lockConfig").unwrap().maximum_pool_size,
        30
    );
    assert!(mexico.secondary_config().is_none());

    // What an app builds from it: a registry, and migration targets named as Java names them.
    let registry = DatabaseRegistry::from_sharding_config(&sharding).unwrap();
    assert_eq!(registry.default_shard(), ShardIdentifier::new(0, 0));
    let names: Vec<String> = Migrator::targets(&sharding)
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert_eq!(names, ["global/global", "mexico/mexico"]);

    // The password is nowhere in what can be printed.
    let shown = format!("{sharding:?}");
    assert!(!shown.contains(PASSWORD), "{shown}");
}

#[test]
fn kebab_snake_and_camel_keys_are_one_key() {
    let tree = json!({ "ekbatan": { "sharding": {
        "default-shard": { "group": 0, "member": 0 },
        "unknown_shard_policy": "FAIL",
        "groups": [{ "group": 0, "name": "g", "members": [{ "configs": {
            "primary-config": { "jdbc-url": "jdbc:mysql://db:3306/app", "maximum_pool_size": 3 }
        }}]}]
    }}});
    let sharding = read(&tree).unwrap().sharding.unwrap();
    assert_eq!(sharding.unknown_shard_policy, UnknownShardPolicy::Fail);
    // Java's UnknownShardPolicy.from: the value's case, '_' and '-' don't matter either.
    for (written, policy) in [
        ("fail", UnknownShardPolicy::Fail),
        ("useDefaultShard", UnknownShardPolicy::UseDefaultShard),
        ("use-default-shard", UnknownShardPolicy::UseDefaultShard),
    ] {
        let mut tree = tree.clone();
        tree["ekbatan"]["sharding"]["unknown_shard_policy"] = json!(written);
        let read_policy = read(&tree).unwrap().sharding.unwrap().unknown_shard_policy;
        assert_eq!(read_policy, policy, "{written}");
    }
    let mut wrong = tree.clone();
    wrong["ekbatan"]["sharding"]["unknown_shard_policy"] = json!("panic");
    assert!(read(&wrong).is_err());
    let primary = sharding
        .member(ShardIdentifier::DEFAULT)
        .unwrap()
        .primary_config();
    assert_eq!(
        (primary.dialect, primary.maximum_pool_size),
        (Dialect::MySql, 3)
    );
}

#[test]
fn environment_variables_fill_in_with_defaults_and_escapes() {
    let tree = json!({ "ekbatan": { "namespace": "${NS:fallback}", "sharding": {
        "defaultShard": { "group": 0, "member": 0 },
        "groups": [{ "name": "g", "members": [{ "configs": { "primaryConfig": {
            "jdbcUrl": "jdbc:postgresql://${HOST}:${PORT:5432}/db",
            "password": "$${not a variable}"
        }}}]}]
    }}});
    let config =
        EkbatanConfig::from_tree_with_env(&tree, &env(&[("HOST", "db.internal")])).unwrap();
    assert_eq!(config.namespace.as_deref(), Some("fallback"));
    let primary = config
        .sharding
        .unwrap()
        .member(ShardIdentifier::DEFAULT)
        .unwrap()
        .primary_config()
        .clone();
    assert_eq!(primary.url, "postgres://db.internal:5432/db");
    assert_eq!(primary.password.as_deref(), Some("${not a variable}"));

    let missing = EkbatanConfig::from_tree_with_env(&tree, &env(&[]))
        .unwrap_err()
        .to_string();
    assert!(
        missing.contains("${HOST}") && missing.contains("jdbcUrl"),
        "{missing}"
    );
}

/// Each mistake is refused with its path, and no message shows the password.
#[test]
fn mistakes_are_named_by_path_never_by_value() {
    let with = |change: &dyn Fn(&mut Value)| {
        let mut tree = java_sharded_example();
        change(&mut tree);
        let message = read(&tree).unwrap_err().to_string();
        assert!(!message.contains(PASSWORD), "{message}");
        message
    };
    let primary = "/ekbatan/sharding/groups/1/members/0/configs/primaryConfig";
    let m = with(&|t| {
        *t.pointer_mut(&format!("{primary}/maximumPoolSize"))
            .unwrap() = json!(0)
    });
    assert!(
        m.contains("groups[1].members[0].configs.primaryConfig") && m.contains("maximumPoolSize"),
        "{m}"
    );
    let m = with(&|t| {
        t.pointer_mut(primary).unwrap()["jdbcUrl"] =
            json!(format!("jdbc:postgresql://wallet:{PASSWORD}@h/db"))
    });
    assert!(m.contains("carries a login"), "{m}");
    let m = with(&|t| {
        t.pointer_mut(primary).unwrap()["dataSourceProperties"] = json!({"PASSWORD": PASSWORD})
    });
    assert!(m.contains("set password instead"), "{m}");
    let m = with(&|t| {
        t.pointer_mut(primary).unwrap()["dataSourceProperties"] = json!({"ApplicationName": "x"})
    });
    assert!(m.contains("application_name"), "{m}");
    let m = with(&|t| t.pointer_mut(primary).unwrap()["poolSize"] = json!(3));
    assert!(
        m.contains("primaryConfig.poolSize means nothing here"),
        "{m}"
    );
    let m = with(&|t| {
        t.pointer_mut("/ekbatan/sharding/groups/1/members/0/configs")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("primaryConfig");
    });
    assert!(m.contains("primaryConfig is required"), "{m}");
    let m = with(&|t| t["ekbatan"]["sharding"]["defaultShard"]["group"] = json!(7));
    assert!(
        m.contains("defaultShard 7/0 is not one of the configured shards"),
        "{m}"
    );
    let m = with(&|t| t["ekbatan"]["sharding"]["groups"][1]["group"] = json!(0));
    assert!(m.contains("group 0 appears twice"), "{m}");
    let m = with(&|t| t["ekbatan"]["shardng"] = json!({}));
    assert!(m.contains("ekbatan.shardng means nothing here"), "{m}");
}

#[test]
fn a_json_file_is_read() {
    let path = std::env::temp_dir().join(format!("ekbatan-config-{}.json", std::process::id()));
    std::fs::write(&path, java_sharded_example().to_string()).unwrap();
    // SAFETY: this test binary sets no other variable, and no test reads DB_PASSWORD meanwhile
    // other than through this file.
    unsafe { std::env::set_var("DB_PASSWORD", PASSWORD) };
    let config = EkbatanConfig::load_json(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(config.namespace.as_deref(), Some("example.wallet"));
    std::fs::write(
        &path,
        format!("{{\"ekbatan\": {{\"namespace\": \"{PASSWORD}\",,}}}}"),
    )
    .unwrap();
    let broken = EkbatanConfig::load_json(&path).unwrap_err().to_string();
    std::fs::remove_file(&path).unwrap();
    assert!(
        !broken.contains(PASSWORD) && broken.contains("line 1"),
        "{broken}"
    );
}
