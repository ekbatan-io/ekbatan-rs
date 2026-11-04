//! The service end to end on a real PostgreSQL: the app is built from its own `ekbatan.json`
//! (the database's address filled in through the file's `${DATABASE_URL}` placeholders), every
//! endpoint is called through the router, and the notification the event handler writes after a
//! deposit is waited for. Java's `WalletControllerIntegrationTest`.

use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum_wallet_rest_pg::app::App;
use axum_wallet_rest_pg::web::WalletResponse;
use ekbatan::EkbatanConfig;
use ekbatan::aio::AsyncRepository;
use http_body_util::BodyExt;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;
use uuid::Uuid;

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
async fn the_wallet_api_end_to_end() {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();

    // The shipped config file, with the container's address in its placeholders, and the jobs
    // polling fast so the test needn't wait.
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
    let config = EkbatanConfig::from_tree_with_env(&tree, &env).unwrap();
    let app = App::start(&config).await.unwrap();
    let router = &app.router;

    // Open.
    let owner = Uuid::new_v4();
    let (status, body) = call(
        router,
        "POST",
        "/wallets",
        Some(json!({ "ownerId": owner, "currency": "USD", "initialBalance": 10 })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let opened = wallet(body);
    assert_eq!((opened.balance, opened.version), (Decimal::TEN, 1));
    assert_eq!((opened.shard_group, opened.shard_member), (0, 0));
    let id = opened.id;
    println!("POST /wallets: 201, balance 10, version 1");

    // Two deposits at once: the keyed lock runs them one after the other, so neither fails on
    // the optimistic lock.
    let deposits = format!("/wallets/{id}/deposits");
    let (first, second) = tokio::join!(
        call(
            router,
            "POST",
            &deposits,
            Some(json!({ "amount": 5.5, "recipient": "alice@example.com" }))
        ),
        call(
            router,
            "POST",
            &deposits,
            Some(json!({ "amount": 1, "recipient": "bob@example.com" }))
        ),
    );
    assert_eq!(
        (first.0, second.0),
        (StatusCode::OK, StatusCode::OK),
        "{first:?} {second:?}"
    );
    let (status, body) = call(router, "GET", &format!("/wallets/{id}"), None).await;
    assert_eq!(status, StatusCode::OK);
    let read = wallet(body);
    assert_eq!((read.balance, read.version), (Decimal::new(165, 1), 3));
    println!("two deposits at once under the keyed lock: both 200, balance 16.5, version 3");

    // Mistakes answer as such.
    let (status, body) = call(router, "POST", &deposits, Some(json!({ "amount": -1 }))).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"], "Deposit amount must be positive");
    let (status, _) = call(router, "GET", &format!("/wallets/{}", Uuid::new_v4()), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    println!("a negative deposit: 422 with the model's message; an unknown wallet: 404");

    // The handler: each deposit becomes a notification, in its own transaction, soon after.
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
    assert_eq!(recipients, ["alice@example.com", "bob@example.com"]);
    assert!(
        notifications
            .iter()
            .all(|n| *n.wallet_id() == id && n.message().starts_with("Deposit of "))
    );
    println!("the event handler recorded a notification per deposit");

    // Close.
    let (status, body) = call(router, "POST", &format!("/wallets/{id}/close"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(wallet(body).state, "CLOSED");
    println!("POST /wallets/{{id}}/close: CLOSED");

    let stopping = Instant::now();
    app.stop().await;
    assert!(stopping.elapsed() < Duration::from_secs(5));
}
