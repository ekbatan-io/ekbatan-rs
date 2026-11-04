//! The SQL of the ready-made methods, as Java's jOOQ calls write it; shared by both kinds of
//! repository.

use sea_query::{
    Asterisk, Expr, ExprTrait, Func, Iden, InsertStatement, Query, SelectStatement,
    UpdateStatement, Value, Values,
};
use uuid::Uuid;

use crate::Dialect;

use super::{Table, TableRecord};

/// `state <> 'DELETED'`: Java's private `notDeleted()`. Only the ready-made methods use it;
/// custom queries write their own condition when they want one.
pub(crate) fn not_deleted<T: Table>() -> Expr {
    Expr::col(T::STATE).ne("DELETED")
}

/// `SELECT * FROM <table>`: jOOQ's `selectFrom(table)`.
pub(crate) fn select_from<T: Table>() -> SelectStatement {
    Query::select()
        .columns(T::COLUMNS.iter().copied())
        .from(T::TABLE)
        .to_owned()
}

/// `SELECT 1 FROM <table>`, for `EXISTS`: jOOQ's `selectOne().from(table)`.
pub(crate) fn select_one_from<T: Table>() -> SelectStatement {
    Query::select().expr(Expr::val(1)).from(T::TABLE).to_owned()
}

/// `SELECT COUNT(*) FROM <table>`: jOOQ's `fetchCount(table, ...)`.
pub(crate) fn count_from<T: Table>() -> SelectStatement {
    Query::select()
        .expr(Func::count(Expr::col(Asterisk)))
        .from(T::TABLE)
        .to_owned()
}

/// One multi-row INSERT of every record: jOOQ's `insertInto(table, fields).values(...)...`.
pub(crate) fn insert_of<R: TableRecord>(records: &[R]) -> InsertStatement {
    let columns: Vec<_> = records[0]
        .values()
        .into_iter()
        .map(|(column, _)| column)
        .collect();
    let mut insert = Query::insert();
    insert.into_table(R::Table::TABLE).columns(columns);
    for record in records {
        insert.values_panic(
            record
                .values()
                .into_iter()
                .map(|(_, value)| Expr::val(value)),
        );
    }
    insert
}

/// `UPDATE <table> SET <every column>, version = <current> + 1 WHERE id = ? AND version =
/// <current>`: jOOQ's `update(table).set(record).where(id).and(version)`.
pub(crate) fn update_of<R: TableRecord>(
    record: &R,
    id: Uuid,
    current_version: i64,
) -> UpdateStatement {
    let table = R::Table::TABLE;
    let mut update = Query::update();
    update.table(table);
    for (column, value) in record.values() {
        if column == R::Table::VERSION {
            update.value(column, current_version + 1);
        } else {
            update.value(column, Expr::val(value));
        }
    }
    update
        .and_where(Expr::col(R::Table::ID).eq(id))
        .and_where(Expr::col(R::Table::VERSION).eq(current_version));
    update
}

/// One UPDATE for many rows, each matched on its id and version, as Java's `updateAll` writes it:
///
/// - PostgreSQL: `UPDATE t SET version = v.version + 1, c = v.c, ... FROM (VALUES (...), ...)
///   AS v (c, ...) WHERE t.id = v.id AND t.version = v.version RETURNING t.*`
/// - MySQL and MariaDB: `UPDATE t JOIN (SELECT ? AS c, ... UNION ALL SELECT ...) AS v ON t.id =
///   v.id AND t.version = v.version SET t.version = v.version + 1, t.c = v.c, ...`
///
/// Every value is a bound parameter. sea-query can't build either shape, so the text is written
/// here for the shard's database, as jOOQ writes it for each dialect.
pub(crate) fn update_all_sql<R: TableRecord>(dialect: Dialect, records: &[R]) -> (String, Values) {
    let quote = |name: String| match dialect {
        Dialect::Postgres => format!("\"{name}\""),
        Dialect::MySql | Dialect::MariaDb => format!("`{name}`"),
    };
    let rows: Vec<Vec<(R::Table, Value)>> = records.iter().map(TableRecord::values).collect();
    let columns: Vec<R::Table> = rows[0].iter().map(|(column, _)| *column).collect();
    let name = |column: R::Table| quote(column.to_string());
    let table = name(R::Table::TABLE);
    let (id, version) = (name(R::Table::ID), name(R::Table::VERSION));
    let others: Vec<R::Table> = columns
        .iter()
        .copied()
        .filter(|column| *column != R::Table::VERSION)
        .collect();
    let mut values = Vec::new();
    let mut next_placeholder = |value: Value| {
        values.push(value);
        match dialect {
            Dialect::Postgres => format!("${}", values.len()),
            Dialect::MySql | Dialect::MariaDb => "?".to_owned(),
        }
    };
    let sql = match dialect {
        Dialect::Postgres => {
            let tuples: Vec<String> = rows
                .into_iter()
                .map(|row| {
                    let placeholders: Vec<String> = row
                        .into_iter()
                        .map(|(_, value)| next_placeholder(value))
                        .collect();
                    format!("({})", placeholders.join(", "))
                })
                .collect();
            let column_list: Vec<String> = columns.iter().map(|column| name(*column)).collect();
            let sets: Vec<String> = others
                .iter()
                .map(|column| format!("{0} = \"v\".{0}", name(*column)))
                .collect();
            format!(
                "UPDATE {table} SET {version} = \"v\".{version} + 1, {sets} FROM (VALUES {tuples}) \
                 AS \"v\" ({columns}) WHERE {table}.{id} = \"v\".{id} AND {table}.{version} = \
                 \"v\".{version} RETURNING {table}.*",
                sets = sets.join(", "),
                tuples = tuples.join(", "),
                columns = column_list.join(", "),
            )
        }
        Dialect::MySql | Dialect::MariaDb => {
            let selects: Vec<String> = rows
                .into_iter()
                .map(|row| {
                    let fields: Vec<String> = row
                        .into_iter()
                        .map(|(column, value)| {
                            format!("{} AS {}", next_placeholder(value), name(column))
                        })
                        .collect();
                    format!("SELECT {}", fields.join(", "))
                })
                .collect();
            let sets: Vec<String> = others
                .iter()
                .map(|column| format!("{table}.{0} = `v`.{0}", name(*column)))
                .collect();
            format!(
                "UPDATE {table} JOIN ({selects}) AS `v` ON {table}.{id} = `v`.{id} AND \
                 {table}.{version} = `v`.{version} SET {table}.{version} = `v`.{version} + 1, \
                 {sets}",
                selects = selects.join(" UNION ALL "),
                sets = sets.join(", "),
            )
        }
    };
    (sql, Values(values))
}
