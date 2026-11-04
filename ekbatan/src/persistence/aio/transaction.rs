//! `AsyncTransaction` and `AsyncTransactionManager`: transactions waited on with `.await`.
//! `blocking::Transaction` and `TransactionManager::begin` / `in_transaction` are the blocking twins.

use crate::{AsyncDb, Connection, Dialect, Error, ShardIdentifier, TransactionManager};

/// A shard's transactions, waited on with `.await`: the async twin of `TransactionManager`'s
/// `begin` and `in_transaction` (Python's `AsyncTransactionManager`). `manager.as_async()`.
#[derive(Clone, Copy)]
pub struct AsyncTransactionManager<'a>(pub(crate) &'a TransactionManager);

impl<'a> AsyncTransactionManager<'a> {
    pub fn shard_identifier(&self) -> ShardIdentifier {
        self.0.shard_identifier()
    }

    /// Takes a primary connection and opens a transaction on it. End it with `commit()` or
    /// `rollback()`; a transaction dropped without either has its connection closed.
    pub async fn begin(&self) -> Result<AsyncTransaction, Error> {
        let connection = self.0.primary_connection_provider().acquire().await?;
        AsyncTransaction::begin(connection, self.0.shard_identifier()).await
    }

    /// Java's `inTransaction(block)`: commits if the block returns `Ok`, rolls back if it
    /// returns `Err`, keeping the block's error as Java does.
    pub async fn in_transaction<R>(
        &self,
        block: impl AsyncFnOnce(&mut AsyncTransaction) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut tx = self.begin().await?;
        match block(&mut tx).await {
            Ok(value) => {
                tx.commit().await?;
                Ok(value)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}

/// A connection with a transaction open on it: Java's `Transaction`, a JDBC `Connection` with
/// auto-commit off.
pub struct AsyncTransaction {
    connection: Connection,
    shard_identifier: ShardIdentifier,
    open: bool,
}

impl AsyncTransaction {
    async fn begin(
        mut connection: Connection,
        shard_identifier: ShardIdentifier,
    ) -> Result<Self, Error> {
        let begin = match connection.dialect() {
            Dialect::Postgres => "BEGIN",
            Dialect::MySql | Dialect::MariaDb => "START TRANSACTION",
        };
        if let Err(error) = connection.run(begin).await {
            connection.evict();
            return Err(error);
        }
        Ok(AsyncTransaction {
            connection,
            shard_identifier,
            open: true,
        })
    }

    pub fn shard(&self) -> ShardIdentifier {
        self.shard_identifier
    }

    /// Statements inside the transaction.
    pub fn db(&mut self) -> AsyncDb<'_> {
        self.connection.db()
    }

    /// As in Java, a failed `COMMIT` is followed by a `ROLLBACK`; only if that fails too is the
    /// connection closed rather than returned.
    pub async fn commit(mut self) -> Result<(), Error> {
        match self.connection.run("COMMIT").await {
            Ok(()) => {
                self.open = false;
                Ok(())
            }
            Err(error) => {
                if self.connection.run("ROLLBACK").await.is_ok() {
                    self.open = false;
                }
                Err(error)
            }
        }
    }

    /// If the `ROLLBACK` fails, the connection is closed rather than returned.
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.connection.run("ROLLBACK").await?;
        self.open = false;
        Ok(())
    }
}

impl Drop for AsyncTransaction {
    /// Never ended, or its rollback failed: close the connection rather than return it, so no
    /// one inherits a half-done transaction. The database rolls back what a closed connection
    /// leaves open.
    fn drop(&mut self) {
        if self.open {
            self.connection.close_on_drop();
        }
    }
}
