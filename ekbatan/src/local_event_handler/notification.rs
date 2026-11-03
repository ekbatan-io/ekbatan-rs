//! Java's `EventNotification`, `EventNotificationState` and `EventNotificationRepository`: one
//! row per (event, handler) in `eventlog.event_notifications`.

use sea_query::{Alias, Expr, ExprTrait, IntoIden, Order, Query};
use uuid::Uuid;

use crate::AsyncDb;
use crate::action::{json_text, json_value};
use crate::{DbUuid, Error, Instant, truncate_to_micros};

/// A notification's state: Java's `EventNotificationState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventNotificationState {
    Pending,
    Succeeded,
    Failed,
    Expired,
}

impl EventNotificationState {
    pub fn as_str(self) -> &'static str {
        match self {
            EventNotificationState::Pending => "PENDING",
            EventNotificationState::Succeeded => "SUCCEEDED",
            EventNotificationState::Failed => "FAILED",
            EventNotificationState::Expired => "EXPIRED",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "PENDING" => Some(EventNotificationState::Pending),
            "SUCCEEDED" => Some(EventNotificationState::Succeeded),
            "FAILED" => Some(EventNotificationState::Failed),
            "EXPIRED" => Some(EventNotificationState::Expired),
            _ => None,
        }
    }
}

/// One delivery of one event to one handler: Java's `EventNotification`. A copy of the event's
/// row, so the handler needs nothing else; JSON columns as text.
#[derive(Clone, Debug, PartialEq)]
pub struct EventNotification {
    pub id: Uuid,
    pub event_id: Uuid,
    pub handler_name: String,
    pub namespace: String,
    pub action_id: Uuid,
    pub action_name: String,
    pub action_params: String,
    pub started_date: Instant,
    pub completion_date: Instant,
    pub model_id: Option<String>,
    pub model_type: Option<String>,
    pub event_type: String,
    pub payload: Option<String>,
    pub event_date: Instant,
    pub state: EventNotificationState,
    pub attempts: i32,
    pub next_retry_at: Instant,
    pub created_date: Instant,
    pub updated_date: Instant,
}

#[derive(sqlx::FromRow)]
struct Row {
    id: DbUuid,
    event_id: DbUuid,
    handler_name: String,
    namespace: String,
    action_id: DbUuid,
    action_name: String,
    action_params: String,
    started_date: chrono::NaiveDateTime,
    completion_date: chrono::NaiveDateTime,
    model_id: Option<String>,
    model_type: Option<String>,
    event_type: String,
    payload: Option<String>,
    event_date: chrono::NaiveDateTime,
    state: String,
    attempts: i32,
    next_retry_at: chrono::NaiveDateTime,
    created_date: chrono::NaiveDateTime,
    updated_date: chrono::NaiveDateTime,
}

impl TryFrom<Row> for EventNotification {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Error> {
        Ok(EventNotification {
            id: row.id.0,
            event_id: row.event_id.0,
            handler_name: row.handler_name,
            namespace: row.namespace,
            action_id: row.action_id.0,
            action_name: row.action_name,
            action_params: row.action_params,
            started_date: row.started_date.and_utc(),
            completion_date: row.completion_date.and_utc(),
            model_id: row.model_id,
            model_type: row.model_type,
            event_type: row.event_type,
            payload: row.payload,
            event_date: row.event_date.and_utc(),
            state: EventNotificationState::parse(&row.state).ok_or_else(|| {
                Error::Mapping(format!("unknown notification state {:?}", row.state))
            })?,
            attempts: row.attempts,
            next_retry_at: row.next_retry_at.and_utc(),
            created_date: row.created_date.and_utc(),
            updated_date: row.updated_date.and_utc(),
        })
    }
}

const COLUMNS: [&str; 19] = [
    "id",
    "event_id",
    "handler_name",
    "namespace",
    "action_id",
    "action_name",
    "action_params",
    "started_date",
    "completion_date",
    "model_id",
    "model_type",
    "event_type",
    "payload",
    "event_date",
    "state",
    "attempts",
    "next_retry_at",
    "created_date",
    "updated_date",
];

fn table() -> (Alias, Alias) {
    (Alias::new("eventlog"), Alias::new("event_notifications"))
}

fn col(name: &str) -> Expr {
    Expr::col(Alias::new(name))
}

fn naive(at: Instant) -> chrono::NaiveDateTime {
    truncate_to_micros(at).naive_utc()
}

/// `eventlog.event_notifications`: Java's `EventNotificationRepository`.
pub struct EventNotificationRepository;

impl EventNotificationRepository {
    /// One multi-row INSERT.
    pub async fn add_all_no_result(
        db: &mut AsyncDb<'_>,
        notifications: &[EventNotification],
    ) -> Result<(), Error> {
        if notifications.is_empty() {
            return Ok(());
        }
        let dialect = db.dialect();
        let mut insert = Query::insert();
        insert
            .into_table(table())
            .columns(COLUMNS.map(|c| Alias::new(c).into_iden()));
        for n in notifications {
            insert.values_panic([
                Expr::val(n.id),
                Expr::val(n.event_id),
                Expr::val(n.handler_name.clone()),
                Expr::val(n.namespace.clone()),
                Expr::val(n.action_id),
                Expr::val(n.action_name.clone()),
                json_value(dialect, Some(n.action_params.clone())),
                Expr::val(naive(n.started_date)),
                Expr::val(naive(n.completion_date)),
                Expr::val(n.model_id.clone()),
                Expr::val(n.model_type.clone()),
                Expr::val(n.event_type.clone()),
                json_value(dialect, n.payload.clone()),
                Expr::val(naive(n.event_date)),
                Expr::val(n.state.as_str()),
                Expr::val(n.attempts),
                Expr::val(naive(n.next_retry_at)),
                Expr::val(naive(n.created_date)),
                Expr::val(naive(n.updated_date)),
            ]);
        }
        db.execute(&insert).await?;
        Ok(())
    }

    fn select(db: &AsyncDb<'_>) -> sea_query::SelectStatement {
        let dialect = db.dialect();
        let mut select = Query::select();
        select.from(table());
        for column in COLUMNS {
            if column == "action_params" || column == "payload" {
                select.expr_as(json_text(dialect, column), Alias::new(column));
            } else {
                select.column(Alias::new(column));
            }
        }
        select
    }

    async fn fetch(
        db: &mut AsyncDb<'_>,
        select: &sea_query::SelectStatement,
    ) -> Result<Vec<EventNotification>, Error> {
        let rows: Vec<Row> = db.fetch_all(select).await?;
        rows.into_iter().map(EventNotification::try_from).collect()
    }

    /// Up to `limit` PENDING or FAILED notifications due by `now`, soonest first: Java's
    /// `findDue`.
    pub async fn find_due(
        db: &mut AsyncDb<'_>,
        limit: u64,
        now: Instant,
    ) -> Result<Vec<EventNotification>, Error> {
        let mut select = Self::select(db);
        select
            .and_where(col("state").is_in([
                EventNotificationState::Pending.as_str(),
                EventNotificationState::Failed.as_str(),
            ]))
            .and_where(col("next_retry_at").lte(naive(now)))
            .order_by(Alias::new("next_retry_at"), Order::Asc)
            .limit(limit);
        Self::fetch(db, &select).await
    }

    /// Every notification of one event.
    pub async fn find_by_event_id(
        db: &mut AsyncDb<'_>,
        event_id: Uuid,
    ) -> Result<Vec<EventNotification>, Error> {
        let mut select = Self::select(db);
        select.and_where(col("event_id").eq(event_id));
        Self::fetch(db, &select).await
    }

    /// Every notification.
    pub async fn find_all(db: &mut AsyncDb<'_>) -> Result<Vec<EventNotification>, Error> {
        let select = Self::select(db);
        Self::fetch(db, &select).await
    }

    async fn set(
        db: &mut AsyncDb<'_>,
        ids: &[Uuid],
        state: EventNotificationState,
        count_attempt: bool,
        next_retry_at: Option<Instant>,
        now: Instant,
    ) -> Result<u64, Error> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut update = Query::update();
        update
            .table(table())
            .value(Alias::new("state"), state.as_str())
            .value(Alias::new("updated_date"), naive(now));
        if count_attempt {
            update.value(Alias::new("attempts"), col("attempts").add(1));
        }
        if let Some(next) = next_retry_at {
            update.value(Alias::new("next_retry_at"), naive(next));
        }
        update.and_where(col("id").is_in(ids.iter().copied()));
        db.execute(&update).await
    }

    /// Java's `markSucceededAll`: SUCCEEDED, one more attempt.
    pub async fn mark_succeeded_all(
        db: &mut AsyncDb<'_>,
        ids: &[Uuid],
        now: Instant,
    ) -> Result<u64, Error> {
        Self::set(db, ids, EventNotificationState::Succeeded, true, None, now).await
    }

    /// Java's `markFailedBucket`: FAILED, one more attempt, retried at `next_retry_at`.
    pub async fn mark_failed_bucket(
        db: &mut AsyncDb<'_>,
        ids: &[Uuid],
        next_retry_at: Instant,
        now: Instant,
    ) -> Result<u64, Error> {
        Self::set(
            db,
            ids,
            EventNotificationState::Failed,
            true,
            Some(next_retry_at),
            now,
        )
        .await
    }

    /// Java's `markExpiredAllPreflight`: EXPIRED before running (no attempt counted).
    pub async fn mark_expired_all_preflight(
        db: &mut AsyncDb<'_>,
        ids: &[Uuid],
        now: Instant,
    ) -> Result<u64, Error> {
        Self::set(db, ids, EventNotificationState::Expired, false, None, now).await
    }

    /// Java's `markExpiredAllPostFailure`: EXPIRED after a failed attempt.
    pub async fn mark_expired_all_post_failure(
        db: &mut AsyncDb<'_>,
        ids: &[Uuid],
        now: Instant,
    ) -> Result<u64, Error> {
        Self::set(db, ids, EventNotificationState::Expired, true, None, now).await
    }
}
