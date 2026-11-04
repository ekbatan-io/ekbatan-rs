//! Java's `WalletController`: four endpoints under `/wallets`.
//!
//! - `POST /wallets` opens a wallet (201).
//! - `POST /wallets/{id}/deposits` deposits, holding the keyed lock `wallet:{id}` around the
//!   whole action, so two deposits to one wallet run one after the other instead of one failing
//!   on the optimistic lock.
//! - `POST /wallets/{id}/close` closes it.
//! - `GET /wallets/{id}` reads it (404 if there is none).
//! - `POST /wallets/transfers` starts a transfer: step 1 (the debit) runs in the request; the
//!   rest runs after, through events. So the answer is 202 Accepted, with the transfer's id and
//!   the debited source.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use ekbatan::aio::{AsyncActionExecutor, AsyncKeyedLockProvider, AsyncRepository};
use ekbatan::{Error, Model, Persistable, ShardedId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::action::{
    InitiateTransferAction, WalletCloseAction, WalletCreateAction, WalletDepositMoneyAction,
    initiate_transfer_action, wallet_close_action, wallet_create_action,
    wallet_deposit_money_action,
};
use crate::model::Wallet;
use crate::repository::WalletRepository;

/// Who the actions run as: Java's example passes `() -> "rest-user"`.
const PRINCIPAL: &str = "rest-user";

/// How long a deposit may hold its wallet's lock before the lock is taken back.
const DEPOSIT_LOCK_MAX_HOLD: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct AppState {
    pub executor: Arc<AsyncActionExecutor>,
    pub locks: Arc<dyn AsyncKeyedLockProvider>,
    pub wallets: Arc<WalletRepository>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/wallets", post(create))
        .route("/wallets/{id}", get(read))
        .route("/wallets/{id}/deposits", post(deposit))
        .route("/wallets/{id}/close", post(close))
        .route("/wallets/transfers", post(transfer))
        .with_state(state)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRequest {
    pub country_code: Option<String>,
    pub owner_id: Uuid,
    pub currency: String,
    pub initial_balance: Decimal,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositRequest {
    pub amount: Decimal,
    pub recipient: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferRequest {
    pub from_wallet_id: Uuid,
    pub to_wallet_id: Uuid,
    pub amount: Decimal,
}

/// Java's `TransferResponse`.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransferResponse {
    pub transfer_id: Uuid,
    pub source_wallet: WalletResponse,
}

/// Java's `WalletResponse`.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WalletResponse {
    pub id: Uuid,
    pub shard_group: u8,
    pub shard_member: u8,
    pub owner_id: Uuid,
    pub currency: String,
    pub balance: Decimal,
    pub state: String,
    pub version: i64,
}

impl From<&Wallet> for WalletResponse {
    fn from(wallet: &Wallet) -> Self {
        let shard = wallet.id().shard();
        WalletResponse {
            id: wallet.id().uuid(),
            shard_group: shard.group,
            shard_member: shard.member,
            owner_id: *wallet.owner_id(),
            currency: wallet.currency().clone(),
            balance: *wallet.balance(),
            state: wallet.state().as_str().to_owned(),
            version: wallet.version(),
        }
    }
}

/// An `ekbatan::Error` as an HTTP answer.
pub struct ApiError(Error);

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        ApiError(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::Invalid(_) | Error::Action(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Error::StaleRecord { .. } | Error::StaleRecords { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let message = if status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!("request failed: {}", self.0);
            "internal error".to_owned()
        } else {
            self.0.to_string()
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateRequest>,
) -> Result<(StatusCode, Json<WalletResponse>), ApiError> {
    let wallet = state
        .executor
        .execute::<WalletCreateAction>(
            Some(PRINCIPAL),
            wallet_create_action::Params {
                country_code: body.country_code,
                owner_id: body.owner_id,
                currency: body.currency,
                initial_balance: body.initial_balance,
            },
        )
        .await?;
    Ok((StatusCode::CREATED, Json(WalletResponse::from(&wallet))))
}

async fn deposit(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<DepositRequest>,
) -> Result<Json<WalletResponse>, ApiError> {
    let lease = state
        .locks
        .acquire(&format!("wallet:{id}"), DEPOSIT_LOCK_MAX_HOLD)
        .await?;
    let deposited = state
        .executor
        .execute::<WalletDepositMoneyAction>(
            Some(PRINCIPAL),
            wallet_deposit_money_action::Params {
                wallet_id: ShardedId::from_uuid(id),
                amount: body.amount,
                recipient: body.recipient,
            },
        )
        .await;
    lease.release().await;
    Ok(Json(WalletResponse::from(&deposited?)))
}

async fn close(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<WalletResponse>, ApiError> {
    let closed = state
        .executor
        .execute::<WalletCloseAction>(
            Some(PRINCIPAL),
            wallet_close_action::Params {
                wallet_id: ShardedId::from_uuid(id),
            },
        )
        .await?;
    Ok(Json(WalletResponse::from(&closed)))
}

async fn read(State(state): State<AppState>, Path(id): Path<Uuid>) -> Result<Response, ApiError> {
    Ok(
        match AsyncRepository::find_by_id(&*state.wallets, &ShardedId::from_uuid(id)).await? {
            Some(wallet) => Json(WalletResponse::from(&wallet)).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
    )
}

async fn transfer(
    State(state): State<AppState>,
    Json(body): Json<TransferRequest>,
) -> Result<(StatusCode, Json<TransferResponse>), ApiError> {
    let initiated = state
        .executor
        .execute::<InitiateTransferAction>(
            Some(PRINCIPAL),
            initiate_transfer_action::Params {
                from_wallet_id: ShardedId::from_uuid(body.from_wallet_id),
                to_wallet_id: ShardedId::from_uuid(body.to_wallet_id),
                amount: body.amount,
            },
        )
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(TransferResponse {
            transfer_id: initiated.transfer_id,
            source_wallet: WalletResponse::from(&initiated.source_wallet),
        }),
    ))
}
