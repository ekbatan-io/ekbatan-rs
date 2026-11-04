//! The worker end to end on a real PostgreSQL, from plain blocking code. Java's
//! `WalletJobsIntegrationTest`: the stipend job tops up under-funded wallets (and leaves the
//! rest), and each top-up's event becomes a notification through the handler.

use std::time::{Duration, Instant};

use ekbatan::blocking::{Repository, block_on};
use ekbatan::{EkbatanConfig, Persistable};
use job_worker_pg::action::{WalletCreateAction, wallet_create_action};
use job_worker_pg::app::Worker;
use job_worker_pg::jobs::{STIPEND, THRESHOLD};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

#[test]
fn the_stipend_tops_up_poor_wallets_and_each_top_up_notifies() {
    let container = block_on(Postgres::default().with_tag("17-alpine").start()).unwrap();
    let port = block_on(container.get_host_port_ipv4(5432)).unwrap();
    let mut tree: Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/ekbatan.json")).unwrap(),
    )
    .unwrap();
    tree["ekbatan"]["jobs"] = json!({ "pollingInterval": "PT0.2S" });
    tree["ekbatan"]["local-event-handler"]["fanoutPollDelay"] = json!("PT0.2S");
    tree["ekbatan"]["local-event-handler"]["handlingPollDelay"] = json!("PT0.2S");
    let url = format!("jdbc:postgresql://127.0.0.1:{port}/postgres");
    let env = move |name: &str| match name {
        "DATABASE_URL" => Some(url.clone()),
        "DATABASE_USER" | "DATABASE_PASSWORD" => Some("postgres".to_owned()),
        _ => None,
    };
    let worker = Worker::start(&EkbatanConfig::from_tree_with_env(&tree, &env).unwrap()).unwrap();

    let open = |balance: i64| {
        worker
            .executor
            .execute::<WalletCreateAction>(
                Some("test"),
                wallet_create_action::Params {
                    country_code: None,
                    owner_id: Uuid::new_v4(),
                    currency: "USD".to_owned(),
                    initial_balance: Decimal::from(balance),
                },
            )
            .unwrap()
    };
    let poor = open(0);
    let rich = open(500);

    let deadline = Instant::now() + Duration::from_secs(20);
    let notifications = loop {
        let found = worker.notifications.find_all().unwrap();
        if !found.is_empty() || Instant::now() > deadline {
            break found;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let topped_up = worker.wallets.get_by_id(poor.id()).unwrap();
    assert!(*topped_up.balance() >= STIPEND, "{}", topped_up.balance());
    assert!(*topped_up.balance() < THRESHOLD + STIPEND);
    assert_eq!(
        *worker.wallets.get_by_id(rich.id()).unwrap().balance(),
        Decimal::from(500)
    );
    println!(
        "the stipend topped the poor wallet up to {} and left the rich one at 500",
        topped_up.balance()
    );
    assert!(!notifications.is_empty());
    assert!(notifications.iter().all(|n| {
        n.recipient() == "stipend@system.local" && *n.wallet_id() == poor.id().uuid()
    }));
    println!(
        "each top-up's event became a notification ({} so far)",
        notifications.len()
    );

    let stopping = Instant::now();
    worker.stop();
    assert!(stopping.elapsed() < Duration::from_secs(5));
    println!(
        "stop(): running jobs finished, pools closed, in {:?}",
        stopping.elapsed()
    );
    let _inside = ekbatan::blocking::runtime().enter();
    drop(container);
}
