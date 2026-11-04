//! The blocking kind: `Db`, `Dbs`, `Transaction`, and the blocking methods of
//! `TransactionManager` and `DatabaseRegistry`.

mod db;
mod registry;
mod transaction;

pub use db::{Db, Dbs};
pub use transaction::Transaction;
