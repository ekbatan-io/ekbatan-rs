use std::fmt;

use crate::ShardIdentifier;

#[derive(Debug)]
pub enum Error {
    Database(sqlx::Error),
    NotFound {
        table: String,
        id: String,
    },
    /// Someone saved a newer version first: Java's `StaleRecordException`.
    StaleRecord {
        table: String,
        id: String,
        version: i64,
    },
    /// A row on one shard, written with a transaction on another.
    WrongShard {
        transaction: ShardIdentifier,
        row: ShardIdentifier,
    },
    UnknownShard(ShardIdentifier),
    /// A row that doesn't make a valid model.
    Mapping(String),
    /// A value a model or entity can't have: Java's `Validate` failures (`IllegalArgumentException`).
    Invalid(String),
    /// Models on two shards in one call that must stay on one: Java's `CrossShardException`.
    CrossShard {
        first: ShardIdentifier,
        second: ShardIdentifier,
    },
    /// A bulk update matched fewer rows than models: some were saved by someone else first.
    StaleRecords {
        table: String,
        expected: usize,
        updated: usize,
    },
    /// One row was expected and a shard had more: jOOQ's `TooManyRowsException`.
    TooManyRows {
        table: String,
    },
    /// Something that should not happen: Java's `IllegalStateException`.
    Unexpected(String),
    /// A setting that can't work, such as a pool of zero connections.
    Config(String),
    /// A value that can't be written as JSON, or JSON that can't be read back.
    Json(serde_json::Error),
    /// An action's own failure: what a Java action throws. Carries the app's error, which
    /// `ExecutionConfiguration` can retry by type.
    Action(Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// Wraps an app's own error as an action failure: `Err(Error::action(InsufficientFunds))`.
    pub fn action(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::Action(Box::new(error))
    }

    /// The app's error inside `Error::Action`, if it is an `E`.
    pub fn action_error<E: std::error::Error + 'static>(&self) -> Option<&E> {
        match self {
            Error::Action(error) => error.downcast_ref::<E>(),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json(error)
    }
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Error::Database(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Database(error) => write!(f, "database error: {error}"),
            Error::NotFound { table, id } => write!(f, "no row {id} in {table}"),
            Error::StaleRecord { table, id, version } => {
                write!(f, "{table} {id} is no longer at version {version}")
            }
            Error::WrongShard { transaction, row } => {
                write!(
                    f,
                    "a transaction on shard {transaction} can't reach a row on shard {row}"
                )
            }
            Error::UnknownShard(shard) => write!(f, "no database for shard {shard}"),
            Error::Mapping(message) => write!(f, "{message}"),
            Error::Invalid(message) => write!(f, "{message}"),
            Error::Config(message) => write!(f, "invalid configuration: {message}"),
            Error::CrossShard { first, second } => {
                write!(
                    f,
                    "one call can't write to shard {first} and shard {second}"
                )
            }
            Error::StaleRecords {
                table,
                expected,
                updated,
            } => {
                write!(
                    f,
                    "expected to update {expected} rows of {table} but updated {updated}"
                )
            }
            Error::TooManyRows { table } => write!(f, "more than one row of {table} matched"),
            Error::Unexpected(message) => write!(f, "{message}"),
            Error::Json(error) => write!(f, "JSON error: {error}"),
            Error::Action(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Database(error) => Some(error),
            Error::Json(error) => Some(error),
            Error::Action(error) => Some(&**error),
            _ => None,
        }
    }
}
