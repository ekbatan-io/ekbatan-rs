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
    /// A setting that can't work, such as a pool of zero connections.
    Config(String),
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
            Error::Config(message) => write!(f, "invalid configuration: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Database(error) => Some(error),
            _ => None,
        }
    }
}
