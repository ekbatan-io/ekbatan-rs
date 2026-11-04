//! The sharded service end to end on two real PostgreSQL servers, one per shard. The app is
//! built from its own `ekbatan.json` (each server's address filled in through the file's
//! placeholders). Java's sharded `WalletControllerIntegrationTest`: a wallet per shard, each row
//! only in its own database, deposits routed by the id, the handler's notifications.

use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum_wallet_rest_sharded_pg::app::App;
use axum_wallet_rest_sharded_pg::web::WalletResponse;
use ekbatan::aio::AsyncRepository;
use ekbatan::{EkbatanConfig, ShardIdentifier};
use http_body_util::BodyExt;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

const GLOBAL: ShardIdentifier = ShardIdentifier::new(0, 0);
const MEXICO: ShardIdentifier = ShardIdentifier::new(1, 0);

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(match body {
            Some(body) => Body::from(body.to_string()),
            None => Body::empty(),
        })
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

fn wallet(value: Value) -> WalletResponse {
    serde_json::from_value(value).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn wallets_live_on_the_shard_their_id_names() {
    let server = || {
        Postgres::default()
            .with_user("wallet")
            .with_password("wallet")
            .with_db_name("wallet")
            .with_tag("17-alpine")
            .start()
    };
    let (global, mexico) = tokio::join!(server(), server());
    let (global, mexico) = (global.unwrap(), mexico.unwrap());
    let url = |port: u16| format!("jdbc:postgresql://127.0.0.1:{port}/wallet");
    let global_url = url(global.get_host_port_ipv4(5432).await.unwrap());
    let mexico_url = url(mexico.get_host_port_ipv4(5432).await.unwrap());

    let mut tree: Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/ekbatan.json")).unwrap(),
    )
    .unwrap();
    tree["ekbatan"]["jobs"] = json!({ "pollingInterval": "PT0.2S" });
    tree["ekbatan"]["local-event-handler"]["fanoutPollDelay"] = json!("PT0.2S");
    tree["ekbatan"]["local-event-handler"]["handlingPollDelay"] = json!("PT0.2S");
    let env = move |name: &str| match name {
        "GLOBAL_DATABASE_URL" => Some(global_url.clone()),
        "MEXICO_DATABASE_URL" => Some(mexico_url.clone()),
        _ => None,
    };
    let config = EkbatanConfig::from_tree_with_env(&tree, &env).unwrap();
    let app = App::start(&config).await.unwrap();
    let router = &app.router;

    // One wallet per shard: "MX" goes to Mexico, anything else to the global shard.
    let open = |country: Option<&str>| {
        json!({
            "countryCode": country,
            "ownerId": Uuid::new_v4(),
            "currency": "USD",
            "initialBalance": 10
        })
    };
    let (status, body) = call(router, "POST", "/wallets", Some(open(None))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let on_global = wallet(body);
    let (status, body) = call(router, "POST", "/wallets", Some(open(Some("MX")))).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let on_mexico = wallet(body);
    assert_eq!(
        (on_global.shard_group, on_global.shard_member),
        (GLOBAL.group, GLOBAL.member)
    );
    assert_eq!(
        (on_mexico.shard_group, on_mexico.shard_member),
        (MEXICO.group, MEXICO.member)
    );
    for (id, home, other) in [
        (on_global.id, GLOBAL, MEXICO),
        (on_mexico.id, MEXICO, GLOBAL),
    ] {
        assert!(app.wallets.exists_on_shard(home, id).await.unwrap());
        assert!(!app.wallets.exists_on_shard(other, id).await.unwrap());
    }
    println!("a wallet per shard: each row only in the database its id names");

    // Deposits and reads find each wallet's shard from its id alone.
    for (id, recipient) in [
        (on_global.id, "global@example.com"),
        (on_mexico.id, "mexico@example.com"),
    ] {
        let (status, body) = call(
            router,
            "POST",
            &format!("/wallets/{id}/deposits"),
            Some(json!({ "amount": "2.50", "recipient": recipient })),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (_, body) = call(router, "GET", &format!("/wallets/{id}"), None).await;
        assert_eq!(wallet(body).balance, Decimal::new(1250, 2));
    }
    println!("deposits and reads routed by the id: 12.50 on each shard");

    // The handler records a notification per deposit, on the default shard.
    let deadline = Instant::now() + Duration::from_secs(15);
    let notifications = loop {
        let found = AsyncRepository::find_all(&*app.notifications)
            .await
            .unwrap();
        if found.len() >= 2 || Instant::now() > deadline {
            break found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut recipients: Vec<&str> = notifications
        .iter()
        .map(|n| n.recipient().as_str())
        .collect();
    recipients.sort();
    assert_eq!(recipients, ["global@example.com", "mexico@example.com"]);
    println!("the handler recorded a notification for the deposit on each shard");

    let close = format!("/wallets/{}/close", on_mexico.id);
    let (status, body) = call(router, "POST", &close, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(wallet(body).state, "CLOSED");
    println!("closing the Mexico wallet: CLOSED");

    app.stop().await;
}
