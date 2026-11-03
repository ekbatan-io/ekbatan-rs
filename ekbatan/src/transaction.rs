use crate::{Connection, ConnectionProvider, Db, Dialect, Error, ShardIdentifier};

/// One shard's pools and transactions: Java's `TransactionManager`. Statements outside a
/// transaction borrow from the pools (through `DatabaseRegistry`); a transaction holds one
/// primary connection from `begin` until it ends.
pub struct TransactionManager {
    shard_identifier: ShardIdentifier,
    primary_connection_provider: ConnectionProvider,
    secondary_connection_provider: ConnectionProvider,
}

impl TransactionManager {
    /// `secondary` is the replica's pool. Without one, replica reads use the primary's pool,
    /// as in Java.
    pub fn new(
        shard_identifier: ShardIdentifier,
        primary: ConnectionProvider,
        secondary: Option<ConnectionProvider>,
    ) -> Self {
        let secondary = secondary.unwrap_or_else(|| primary.clone());
        TransactionManager {
            shard_identifier,
            primary_connection_provider: primary,
            secondary_connection_provider: secondary,
        }
    }

    pub fn shard_identifier(&self) -> ShardIdentifier {
        self.shard_identifier
    }

    pub fn dialect(&self) -> Dialect {
        self.primary_connection_provider.dialect()
    }

    pub fn primary_connection_provider(&self) -> &ConnectionProvider {
        &self.primary_connection_provider
    }

    pub fn secondary_connection_provider(&self) -> &ConnectionProvider {
        &self.secondary_connection_provider
    }

    /// Takes a primary connection and opens a transaction on it. End it with `commit()` or
    /// `rollback()`; a transaction dropped without either has its connection closed.
    pub async fn begin(&self) -> Result<Transaction, Error> {
        let connection = self.primary_connection_provider.acquire().await?;
        Transaction::begin(connection, self.shard_identifier).await
    }

    /// Java's `inTransaction(block)`: commits if the block returns `Ok`, rolls back if it
    /// returns `Err`, keeping the block's error as Java does.
    pub async fn in_transaction<R>(
        &self,
        block: impl AsyncFnOnce(&mut Transaction) -> Result<R, Error>,
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
pub struct Transaction {
    connection: Connection,
    shard_identifier: ShardIdentifier,
    open: bool,
}

impl Transaction {
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
        Ok(Transaction {
            connection,
            shard_identifier,
            open: true,
        })
    }

    pub fn shard(&self) -> ShardIdentifier {
        self.shard_identifier
    }

    /// Statements inside the transaction.
    pub fn db(&mut self) -> Db<'_> {
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

impl Drop for Transaction {
    /// Never ended, or its rollback failed: close the connection rather than return it, so no
    /// one inherits a half-done transaction. The database rolls back what a closed connection
    /// leaves open.
    fn drop(&mut self) {
        if self.open {
            self.connection.close_on_drop();
        }
    }
}
