//! `Id`: a plain UUID id, with no shard in it. Java's `Id`. (`ShardedId` is in `shard`.)

use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use uuid::Uuid;

use super::ModelId;
use crate::{ShardKey, ShardingStrategy};

/// A plain UUID id, with no shard in it: Java's `Id`. Under `EmbeddedBits`, a version-7 one
/// still names its shard by its bits, as Java's strategy reads it.
pub struct Id<M> {
    value: Uuid,
    model: PhantomData<fn() -> M>,
}

impl<M> Id<M> {
    pub fn of(value: Uuid) -> Self {
        Id {
            value,
            model: PhantomData,
        }
    }

    /// Java's `Id.random(...)`: a random (version 4) UUID.
    pub fn random() -> Self {
        Id::of(Uuid::new_v4())
    }

    pub fn value(&self) -> Uuid {
        self.value
    }
}

impl<M> Clone for Id<M> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<M> Copy for Id<M> {}

impl<M> PartialEq for Id<M> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<M> Eq for Id<M> {}

impl<M> Hash for Id<M> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<M> fmt::Debug for Id<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl<M> fmt::Display for Id<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.value, f)
    }
}

impl<M: 'static> ModelId for Id<M> {
    fn uuid(&self) -> Uuid {
        self.value
    }
}

impl<M> ShardKey for Id<M> {
    fn resolve(&self, strategy: ShardingStrategy) -> Option<crate::ShardIdentifier> {
        strategy.resolve_shard_identifier_by_id(self.value)
    }
}
