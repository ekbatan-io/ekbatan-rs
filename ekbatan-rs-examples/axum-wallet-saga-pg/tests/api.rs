//! The transfer saga end to end on a real PostgreSQL. Java's `WalletSagaIntegrationTest`:
//!
//! 1. the happy path: 25 from A (100) to B (0) settles at A = 75, B = 25;
//! 2. a closed destination: A is debited, then refunded back to 100; B stays closed at 0;
//! 3. a missing destination: A is refunded back to 100.
//!
//! The first two also replay the follow-up step with the same transfer id, as an at-least-once
//! delivery would, and check that nothing moves twice.

use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum_wallet_saga_pg::action::{
    CompleteTransferAction, RefundTransferAction, complete_transfer_action, refund_transfer_action,
};
use axum_wallet_saga_pg::app::App;
use axum_wallet_saga_pg::web::{TransferResponse, WalletResponse};
use ekbatan::{EkbatanConfig, ShardedId};
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

async fn open(router: &Router, balance: i64) -> Uuid {
    let (status, body) = call(
        router,
        "POST",
        "/wallets",
        Some(json!({ "ownerId": Uuid::new_v4(), "currency": "USD", "initialBalance": balance })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    serde_json::from_value::<WalletResponse>(body).unwrap().id
}

async fn read(router: &Router, id: Uuid) -> WalletResponse {
    let (status, body) = call(router, "GET", &format!("/wallets/{id}"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    serde_json::from_value(body).unwrap()
}

async fn transfer(router: &Router, from: Uuid, to: Uuid, amount: i64) -> TransferResponse {
    let (status, body) = call(
        router,
        "POST",
        "/wallets/transfers",
        Some(json!({ "fromWalletId": from, "toWalletId": to, "amount": amount })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    serde_json::from_value(body).unwrap()
}

/// Waits (up to 15 seconds) until `id`'s balance is `expected`.
async fn settles_at(router: &Router, id: Uuid, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let balance = read(router, id).await.balance;
        if balance == Decimal::from(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{id}: {balance}, waited for {expected}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn transfers_settle_or_compensate() {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
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
    let app = App::start(&EkbatanConfig::from_tree_with_env(&tree, &env).unwrap())
        .await
        .unwrap();
    let router = &app.router;

    // 1. The happy path.
    let (a, b) = (open(router, 100).await, open(router, 0).await);
    let started = transfer(router, a, b, 25).await;
    assert_eq!(started.source_wallet.balance, Decimal::from(75));
    settles_at(router, b, 25).await;
    assert_eq!(read(router, a).await.balance, Decimal::from(75));
    // A replayed step 2 finds its marker and moves nothing.
    app.executor
        .execute::<CompleteTransferAction>(
            Some("replay"),
            complete_transfer_action::Params {
                transfer_id: started.transfer_id,
                from_wallet_id: ShardedId::from_uuid(a),
                to_wallet_id: ShardedId::from_uuid(b),
                amount: Decimal::from(25),
            },
        )
        .await
        .unwrap();
    assert_eq!(read(router, b).await.balance, Decimal::from(25));
    assert_eq!(read(router, a).await.balance, Decimal::from(75));
    println!(
        "happy path: 202, A debited to 75 at once, B credited to 25 after; a replay moves nothing"
    );

    // 2. A closed destination: debited, then refunded.
    let (a, b) = (open(router, 100).await, open(router, 0).await);
    let (status, body) = call(router, "POST", &format!("/wallets/{b}/close"), None).await;
    assert_eq!(
        (status, body["state"].as_str()),
        (StatusCode::OK, Some("CLOSED"))
    );
    let started = transfer(router, a, b, 25).await;
    assert_eq!(started.source_wallet.balance, Decimal::from(75));
    settles_at(router, a, 100).await;
    let closed = read(router, b).await;
    assert_eq!(
        (closed.balance, closed.state.as_str()),
        (Decimal::ZERO, "CLOSED")
    );
    app.executor
        .execute::<RefundTransferAction>(
            Some("replay"),
            refund_transfer_action::Params {
                transfer_id: started.transfer_id,
                from_wallet_id: ShardedId::from_uuid(a),
                amount: Decimal::from(25),
            },
        )
        .await
        .unwrap();
    assert_eq!(read(router, a).await.balance, Decimal::from(100));
    println!(
        "closed destination: A debited to 75, then refunded to 100; B still CLOSED at 0; a replayed refund moves nothing"
    );

    // 3. A destination that doesn't exist.
    let a = open(router, 100).await;
    transfer(router, a, Uuid::new_v4(), 25).await;
    settles_at(router, a, 100).await;
    println!("missing destination: A refunded to 100");

    // Step 1 refuses what it can't do, before anything is written.
    let (status, body) = call(
        router,
        "POST",
        "/wallets/transfers",
        Some(json!({ "fromWalletId": a, "toWalletId": Uuid::new_v4(), "amount": 1000 })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"], "Insufficient balance for transfer");
    println!("an overdraft: 422 Insufficient balance for transfer, nothing written");

    app.stop().await;
}
