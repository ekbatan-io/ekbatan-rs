//! Java's `EventPersister` and `SingleTableJsonEventPersister`: the action's row(s) in
//! `eventlog.events`, written in the same transaction as the action's changes.

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Mutex;

use futures_util::future::BoxFuture;
use sea_query::{Alias, Asterisk, Expr, ExprTrait, IntoIden, Query, SimpleExpr};
use uuid::Uuid;

use crate::db::Db;
use crate::{AnyModelEvent, DbUuid, Dialect, Error, Instant, ShardIdentifier, Transaction};

/// One event as the event log receives it: the event and the model it belongs to.
#[derive(Clone, Copy, Debug)]
pub struct EmittedEvent<'a> {
    /// The model's id, as text: Java's `modelId`.
    pub model_id: &'a str,
    /// The model's simple type name: Java's `modelName`, the `model_type` column.
    pub model_name: &'a str,
    pub event: &'a dyn AnyModelEvent,
}

/// Writes an action's events: Java's `EventPersister`. Called once per shard, inside that
/// shard's transaction, after the action's changes there.
pub trait EventPersister: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn persist_action_events<'a>(
        &'a self,
        tx: &'a mut Transaction,
        namespace: &'a str,
        action_name: &'a str,
        started_date: Instant,
        completion_date: Instant,
        action_params: &'a str,
        model_events: &'a [EmittedEvent<'a>],
        shard: ShardIdentifier,
        action_event_id: Uuid,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

/// The default `EventPersister`: one row per event in `eventlog.events`, with the action's
/// params and the event's payload as JSON; one row with no event when the action emitted none.
#[derive(Default)]
pub struct SingleTableJsonEventPersister {
    event_types_by_simple_name: Mutex<HashMap<&'static str, (TypeId, &'static str)>>,
}

impl SingleTableJsonEventPersister {
    pub fn new() -> Self {
        SingleTableJsonEventPersister::default()
    }

    /// The event's simple type name; two event types with one simple name are refused, as a
    /// reader routes on that name alone.
    fn event_type_name(&self, event: &dyn AnyModelEvent) -> Result<&'static str, Error> {
        let name = event.event_type();
        let id = (event.as_any().type_id(), event.type_path());
        let mut seen = self
            .event_types_by_simple_name
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = *seen.entry(name).or_insert(id);
        if previous.0 != id.0 {
            return Err(Error::Invalid(format!(
                "Event type simple name collision: {name} maps to both {} and {}",
                previous.1, id.1
            )));
        }
        Ok(name)
    }
}

impl EventPersister for SingleTableJsonEventPersister {
    fn persist_action_events<'a>(
        &'a self,
        tx: &'a mut Transaction,
        namespace: &'a str,
        action_name: &'a str,
        started_date: Instant,
        completion_date: Instant,
        action_params: &'a str,
        model_events: &'a [EmittedEvent<'a>],
        _shard: ShardIdentifier,
        action_event_id: Uuid,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let mut rows = Vec::new();
            if model_events.is_empty() {
                rows.push(EventEntity {
                    id: Uuid::new_v4(),
                    namespace: namespace.to_owned(),
                    action_id: action_event_id,
                    action_name: action_name.to_owned(),
                    action_params: action_params.to_owned(),
                    started_date,
                    completion_date,
                    model_id: None,
                    model_type: None,
                    event_type: None,
                    payload: None,
                    event_date: completion_date,
                    delivered: false,
                });
            }
            for emitted in model_events {
                rows.push(EventEntity {
                    id: Uuid::new_v4(),
                    namespace: namespace.to_owned(),
                    action_id: action_event_id,
                    action_name: action_name.to_owned(),
                    action_params: action_params.to_owned(),
                    started_date,
                    completion_date,
                    model_id: Some(emitted.model_id.to_owned()),
                    model_type: Some(emitted.model_name.to_owned()),
                    event_type: Some(self.event_type_name(emitted.event)?.to_owned()),
                    payload: Some(event_payload(emitted)?),
                    event_date: completion_date,
                    delivered: false,
                });
            }
            EventEntityRepository::add_all_no_result(&mut tx.db(), &rows).await
        })
    }
}

/// An event's payload as Java's Jackson writes it: `modelId` and `modelName` first, then the
/// event's own fields in declaration order.
pub fn event_payload(emitted: &EmittedEvent<'_>) -> Result<String, Error> {
    let fields = emitted.event.to_json()?;
    let Some(rest) = fields.strip_prefix('{') else {
        return Err(Error::Invalid(
            "event must serialize to a JSON object; check for a custom Serialize rendering it \
             as a scalar/list"
                .to_owned(),
        ));
    };
    let mut payload = String::with_capacity(fields.len() + 64);
    payload.push_str("{\"modelId\":");
    payload.push_str(&serde_json::to_string(emitted.model_id)?);
    payload.push_str(",\"modelName\":");
    payload.push_str(&serde_json::to_string(emitted.model_name)?);
    if rest != "}" {
        payload.push(',');
    }
    payload.push_str(rest);
    Ok(payload)
}

/// One row of `eventlog.events`: Java's `EventEntity`. JSON columns as text.
#[derive(Clone, Debug, PartialEq)]
pub struct EventEntity {
    pub id: Uuid,
    pub namespace: String,
    pub action_id: Uuid,
    pub action_name: String,
    pub action_params: String,
    pub started_date: Instant,
    pub completion_date: Instant,
    pub model_id: Option<String>,
    pub model_type: Option<String>,
    pub event_type: Option<String>,
    pub payload: Option<String>,
    pub event_date: Instant,
    pub delivered: bool,
}

#[derive(sqlx::FromRow)]
struct EventRow {
    id: DbUuid,
    namespace: String,
    action_id: DbUuid,
    action_name: String,
    action_params: String,
    started_date: chrono::NaiveDateTime,
    completion_date: chrono::NaiveDateTime,
    model_id: Option<String>,
    model_type: Option<String>,
    event_type: Option<String>,
    payload: Option<String>,
    event_date: chrono::NaiveDateTime,
    delivered: bool,
}

impl From<EventRow> for EventEntity {
    fn from(row: EventRow) -> Self {
        EventEntity {
            id: row.id.0,
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
            delivered: row.delivered,
        }
    }
}

/// `eventlog.events`: a schema on PostgreSQL, its own database on MySQL and MariaDB.
/// Java's `EventEntityRepository`.
pub struct EventEntityRepository;

const COLUMNS: [&str; 13] = [
    "id",
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
    "delivered",
];

fn events_table() -> (Alias, Alias) {
    (Alias::new("eventlog"), Alias::new("events"))
}

/// A JSON value to write: `jsonb` on PostgreSQL; text on MySQL and MariaDB, stored as given.
fn json_value(dialect: Dialect, text: Option<String>) -> SimpleExpr {
    let value = Expr::val(text);
    match dialect {
        Dialect::Postgres => value.cast_as(Alias::new("jsonb")),
        Dialect::MySql | Dialect::MariaDb => value,
    }
}

/// A JSON column read back as text.
fn json_text(dialect: Dialect, column: &str) -> SimpleExpr {
    let column = Expr::col(Alias::new(column));
    match dialect {
        Dialect::Postgres => column.cast_as(Alias::new("text")),
        Dialect::MySql | Dialect::MariaDb => column.cast_as(Alias::new("char")),
    }
}

impl EventEntityRepository {
    /// One multi-row INSERT.
    pub async fn add_all_no_result(db: &mut Db<'_>, entities: &[EventEntity]) -> Result<(), Error> {
        if entities.is_empty() {
            return Ok(());
        }
        let dialect = db.dialect();
        let mut insert = Query::insert();
        insert
            .into_table(events_table())
            .columns(COLUMNS.map(|column| Alias::new(column).into_iden()));
        for entity in entities {
            insert.values_panic([
                Expr::val(entity.id),
                Expr::val(entity.namespace.clone()),
                Expr::val(entity.action_id),
                Expr::val(entity.action_name.clone()),
                json_value(dialect, Some(entity.action_params.clone())),
                Expr::val(entity.started_date.naive_utc()),
                Expr::val(entity.completion_date.naive_utc()),
                Expr::val(entity.model_id.clone()),
                Expr::val(entity.model_type.clone()),
                Expr::val(entity.event_type.clone()),
                json_value(dialect, entity.payload.clone()),
                Expr::val(entity.event_date.naive_utc()),
                Expr::val(entity.delivered),
            ]);
        }
        db.execute(&insert).await?;
        Ok(())
    }

    /// Every row, in no particular order.
    pub async fn find_all(db: &mut Db<'_>) -> Result<Vec<EventEntity>, Error> {
        Self::find(db, None).await
    }

    /// The rows of one action execution.
    pub async fn find_by_action_id(
        db: &mut Db<'_>,
        action_id: Uuid,
    ) -> Result<Vec<EventEntity>, Error> {
        Self::find(db, Some(action_id)).await
    }

    pub async fn count(db: &mut Db<'_>) -> Result<i64, Error> {
        let select = Query::select()
            .expr(Expr::col(Asterisk).count())
            .from(events_table())
            .to_owned();
        db.fetch_scalar(&select).await
    }

    async fn find(db: &mut Db<'_>, action_id: Option<Uuid>) -> Result<Vec<EventEntity>, Error> {
        let dialect = db.dialect();
        let mut select = Query::select();
        select.from(events_table());
        for column in COLUMNS {
            if column == "action_params" || column == "payload" {
                select.expr_as(json_text(dialect, column), Alias::new(column));
            } else {
                select.column(Alias::new(column));
            }
        }
        if let Some(action_id) = action_id {
            select.and_where(Expr::col(Alias::new("action_id")).eq(action_id));
        }
        let rows: Vec<EventRow> = db.fetch_all(&select).await?;
        Ok(rows.into_iter().map(EventEntity::from).collect())
    }
}
