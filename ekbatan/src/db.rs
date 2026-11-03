use sqlx::mysql::{MySql, MySqlRow};
use sqlx::postgres::{PgRow, Postgres};
use sqlx::{Decode, FromRow, Type};

/// The SQL flavour of one database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    MySql,
    MariaDb,
}

/// Rows that read the same from PostgreSQL and MySQL: what `#[derive(sqlx::FromRow)]` gives a
/// struct whose fields all decode from both.
pub trait Record:
    for<'r> FromRow<'r, PgRow> + for<'r> FromRow<'r, MySqlRow> + Send + Unpin
{
}

impl<T> Record for T where
    T: for<'r> FromRow<'r, PgRow> + for<'r> FromRow<'r, MySqlRow> + Send + Unpin
{
}

/// A single value, such as a count, that reads the same from PostgreSQL and MySQL.
pub trait Scalar:
    for<'r> Decode<'r, Postgres>
    + Type<Postgres>
    + for<'r> Decode<'r, MySql>
    + Type<MySql>
    + Send
    + Unpin
{
}

impl<T> Scalar for T where
    T: for<'r> Decode<'r, Postgres>
        + Type<Postgres>
        + for<'r> Decode<'r, MySql>
        + Type<MySql>
        + Send
        + Unpin
{
}
