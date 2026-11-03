use super::{block_on, blocking_runtime};
use crate::Db;
use crate::{Error, ShardIdentifier};

/// A connection with a transaction open on it: Java's `Transaction`. Its async twin is
/// `AsyncTransaction`. End it with `commit()` or `rollback()`; one
/// dropped without either has its connection closed, and the database rolls it back.
pub struct Transaction {
    /// `None` only once `commit` or `rollback` took it.
    inner: Option<crate::AsyncTransaction>,
}

impl Transaction {
    pub fn new(transaction: crate::AsyncTransaction) -> Self {
        Transaction {
            inner: Some(transaction),
        }
    }

    fn inner(&mut self) -> &mut crate::AsyncTransaction {
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
    pub fn as_async(&mut self) -> &mut crate::AsyncTransaction {
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

impl crate::TransactionManager {
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

impl crate::DatabaseRegistry {
    /// Statements on the shard's primary pool: Java's `primary.get(shard)`.
    pub fn primary(&self, shard: ShardIdentifier) -> Result<Db<'_>, Error> {
        self.as_async().primary(shard).map(Db::new)
    }

    /// Statements on the shard's replica pool (the primary's when it has none).
    pub fn secondary(&self, shard: ShardIdentifier) -> Result<Db<'_>, Error> {
        self.as_async().secondary(shard).map(Db::new)
    }

    /// Every shard's primary pool.
    pub fn primaries(&self) -> crate::Dbs<'_> {
        crate::Dbs::new(self.as_async().primaries())
    }

    /// Every shard's replica pool.
    pub fn secondaries(&self) -> crate::Dbs<'_> {
        crate::Dbs::new(self.as_async().secondaries())
    }

    /// Closes every pool: Java's `close()`.
    pub fn close(&self) {
        block_on(self.as_async().close())
    }
}
