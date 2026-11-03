use crate::aio::AsyncTransactionManager;
use crate::{ConnectionProvider, Dialect, ShardIdentifier};

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
