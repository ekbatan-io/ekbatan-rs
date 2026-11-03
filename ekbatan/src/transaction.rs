use crate::aio::{AsyncTransaction, AsyncTransactionManager};
use crate::runtime::{block_on, blocking_runtime};
use crate::{ConnectionProvider, Db, Dialect, Error, ShardIdentifier};

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

    /// The async view: `begin().await` and `in_transaction(...).await`.
    pub fn as_async(&self) -> AsyncTransactionManager<'_> {
        AsyncTransactionManager(self)
    }
}

impl TransactionManager {
    /// Takes a primary connection and opens a transaction on it. End it with `commit()` or
    /// `rollback()`; a transaction dropped without either has its connection closed.
    pub fn begin(&self) -> Result<Transaction, Error> {
        block_on(self.as_async().begin()).map(Transaction::new)
    }

    /// Java's `inTransaction(block)`: commits if the block returns `Ok`, rolls back if it
    /// returns `Err`, keeping the block's error as Java does.
    pub fn in_transaction<R>(
        &self,
        block: impl FnOnce(&mut Transaction) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut transaction = self.begin()?;
        match block(&mut transaction) {
            Ok(value) => {
                transaction.commit()?;
                Ok(value)
            }
            Err(error) => {
                let _ = transaction.rollback();
                Err(error)
            }
        }
    }
}

/// A connection with a transaction open on it: Java's `Transaction`. Its async twin is
/// `AsyncTransaction`. End it with `commit()` or `rollback()`; one
/// dropped without either has its connection closed, and the database rolls it back.
pub struct Transaction {
    /// `None` only once `commit` or `rollback` took it.
    inner: Option<AsyncTransaction>,
}

impl Transaction {
    pub fn new(transaction: AsyncTransaction) -> Self {
        Transaction {
            inner: Some(transaction),
        }
    }

    fn inner(&mut self) -> &mut AsyncTransaction {
        self.inner
            .as_mut()
            .expect("a transaction is only taken when it ends")
    }

    pub fn shard(&self) -> ShardIdentifier {
        self.inner
            .as_ref()
            .expect("a transaction is only taken when it ends")
            .shard()
    }

    /// Statements inside the transaction.
    pub fn db(&mut self) -> Db<'_> {
        Db::new(self.inner().db())
    }

    /// The async transaction underneath, for an async call that takes one.
    pub fn as_async(&mut self) -> &mut AsyncTransaction {
        self.inner()
    }

    pub fn commit(mut self) -> Result<(), Error> {
        let transaction = self.inner.take().expect("taken only here");
        block_on(transaction.commit())
    }

    pub fn rollback(mut self) -> Result<(), Error> {
        let transaction = self.inner.take().expect("taken only here");
        block_on(transaction.rollback())
    }
}

impl Drop for Transaction {
    /// The pooled connection hands itself back (or closes) on the runtime it came from, which
    /// must be current when it is dropped.
    fn drop(&mut self) {
        if let Some(transaction) = self.inner.take() {
            let _inside = blocking_runtime().enter();
            drop(transaction);
        }
    }
}
