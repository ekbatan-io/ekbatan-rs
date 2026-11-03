//! The scheduler's statements on `scheduled_tasks` (db-scheduler's layout). Every change to a
//! row is conditional on its `version`, which every change raises: of two instances acting on one
//! row, exactly one's update matches.

use sea_query::{Alias, Expr, ExprTrait, Order, Query};

use crate::{ConnectionProvider, Error, Instant, truncate_to_micros};

/// Recurring jobs have one row each, under this instance name (db-scheduler's).
const RECURRING: &str = "recurring";

fn table() -> Alias {
    Alias::new("scheduled_tasks")
}

fn col(name: &str) -> Expr {
    Expr::col(Alias::new(name))
}

/// A row this instance holds: the version its claim wrote.
#[derive(Clone, Debug)]
pub(super) struct Claimed {
    pub task_name: String,
    pub task_instance: String,
    pub execution_time: Instant,
    pub version: i64,
    pub consecutive_failures: i32,
}

#[derive(Debug, sqlx::FromRow)]
pub(super) struct Row {
    pub task_name: String,
    pub task_instance: String,
    pub execution_time: Instant,
    pub picked_by: Option<String>,
    pub last_heartbeat: Option<Instant>,
    pub version: i64,
    pub consecutive_failures: Option<i32>,
}

pub(super) struct Store {
    provider: ConnectionProvider,
}

impl Store {
    pub fn new(provider: ConnectionProvider) -> Self {
        Store { provider }
    }

    /// The job's row, unless it has one already (then its schedule and state are kept).
    pub async fn register(&self, name: &str, first: Instant) -> Result<(), Error> {
        let insert = Query::insert()
            .into_table(table())
            .columns(
                [
                    "task_name",
                    "task_instance",
                    "execution_time",
                    "picked",
                    "consecutive_failures",
                    "version",
                ]
                .map(Alias::new),
            )
            .values_panic([
                Expr::val(name),
                Expr::val(RECURRING),
                Expr::val(truncate_to_micros(first)),
                Expr::val(false),
                Expr::val(0),
                Expr::val(1i64),
            ])
            .to_owned();
        self.provider
            .db()
            .insert_ignoring_any_duplicate(&insert)
            .await?;
        Ok(())
    }

    /// Which of `names` have a row.
    pub async fn registered(&self, names: &[String]) -> Result<Vec<String>, Error> {
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let select = Query::select()
            .column(Alias::new("task_name"))
            .from(table())
            .and_where(col("task_name").is_in(names.iter().cloned()))
            .to_owned();
        let rows: Vec<(String,)> = self.provider.db().fetch_all(&select).await?;
        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    fn select() -> sea_query::SelectStatement {
        Query::select()
            .columns(
                [
                    "task_name",
                    "task_instance",
                    "execution_time",
                    "picked_by",
                    "last_heartbeat",
                    "version",
                    "consecutive_failures",
                ]
                .map(Alias::new),
            )
            .from(table())
            .to_owned()
    }

    /// Unpicked rows of `names` due by `now`, oldest first.
    pub async fn due(&self, names: &[String], now: Instant) -> Result<Vec<Row>, Error> {
        let select = Self::select()
            .and_where(col("task_name").is_in(names.iter().cloned()))
            .and_where(col("picked").eq(false))
            .and_where(col("execution_time").lte(truncate_to_micros(now)))
            .order_by(Alias::new("execution_time"), Order::Asc)
            .to_owned();
        self.provider.db().fetch_all(&select).await
    }

    /// Picked rows of `names` whose last heartbeat is older than `before`.
    pub async fn dead(&self, names: &[String], before: Instant) -> Result<Vec<Row>, Error> {
        let select = Self::select()
            .and_where(col("task_name").is_in(names.iter().cloned()))
            .and_where(col("picked").eq(true))
            .and_where(col("last_heartbeat").lt(truncate_to_micros(before)))
            .to_owned();
        self.provider.db().fetch_all(&select).await
    }

    /// Takes a due row: wins only if no one changed it since it was read.
    pub async fn claim(
        &self,
        row: &Row,
        instance: &str,
        now: Instant,
    ) -> Result<Option<Claimed>, Error> {
        let now = truncate_to_micros(now);
        let update = Query::update()
            .table(table())
            .value(Alias::new("picked"), true)
            .value(Alias::new("picked_by"), instance)
            .value(Alias::new("last_heartbeat"), now)
            .value(Alias::new("version"), col("version").add(1))
            .and_where(col("task_name").eq(row.task_name.as_str()))
            .and_where(col("task_instance").eq(row.task_instance.as_str()))
            .and_where(col("picked").eq(false))
            .and_where(col("version").eq(row.version))
            .to_owned();
        if self.provider.db().execute(&update).await? != 1 {
            return Ok(None);
        }
        Ok(Some(Claimed {
            task_name: row.task_name.clone(),
            task_instance: row.task_instance.clone(),
            execution_time: row.execution_time,
            version: row.version + 1,
            consecutive_failures: row.consecutive_failures.unwrap_or(0),
        }))
    }

    /// Stamps the heartbeat of a row this instance holds. `false`: it was taken over.
    pub async fn heartbeat(&self, claimed: &Claimed, now: Instant) -> Result<bool, Error> {
        let update = Query::update()
            .table(table())
            .value(Alias::new("last_heartbeat"), truncate_to_micros(now))
            .and_where(col("task_name").eq(claimed.task_name.as_str()))
            .and_where(col("task_instance").eq(claimed.task_instance.as_str()))
            .and_where(col("picked").eq(true))
            .and_where(col("version").eq(claimed.version))
            .to_owned();
        Ok(self.provider.db().execute(&update).await? == 1)
    }

    /// Releases a row after its execution, with its next slot. `false`: it was taken over and
    /// this result is not recorded.
    pub async fn complete(
        &self,
        claimed: &Claimed,
        succeeded: bool,
        done: Instant,
        next: Instant,
    ) -> Result<bool, Error> {
        let done = truncate_to_micros(done);
        let mut update = Query::update();
        update
            .table(table())
            .value(Alias::new("picked"), false)
            .value(Alias::new("picked_by"), Option::<String>::None)
            .value(Alias::new("last_heartbeat"), Option::<Instant>::None)
            .value(Alias::new("execution_time"), truncate_to_micros(next))
            .value(Alias::new("version"), col("version").add(1));
        if succeeded {
            update
                .value(Alias::new("last_success"), done)
                .value(Alias::new("consecutive_failures"), 0);
        } else {
            update.value(Alias::new("last_failure"), done).value(
                Alias::new("consecutive_failures"),
                claimed.consecutive_failures + 1,
            );
        }
        update
            .and_where(col("task_name").eq(claimed.task_name.as_str()))
            .and_where(col("task_instance").eq(claimed.task_instance.as_str()))
            .and_where(col("version").eq(claimed.version));
        Ok(self.provider.db().execute(&update).await? == 1)
    }

    /// A dead execution, due again now: db-scheduler's revive-dead-execution handler.
    pub async fn revive(&self, row: &Row, now: Instant) -> Result<bool, Error> {
        let update = Query::update()
            .table(table())
            .value(Alias::new("picked"), false)
            .value(Alias::new("picked_by"), Option::<String>::None)
            .value(Alias::new("last_heartbeat"), Option::<Instant>::None)
            .value(Alias::new("execution_time"), truncate_to_micros(now))
            .value(Alias::new("version"), col("version").add(1))
            .and_where(col("task_name").eq(row.task_name.as_str()))
            .and_where(col("task_instance").eq(row.task_instance.as_str()))
            .and_where(col("picked").eq(true))
            .and_where(col("version").eq(row.version))
            .to_owned();
        Ok(self.provider.db().execute(&update).await? == 1)
    }
}
