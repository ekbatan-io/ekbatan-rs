//! What the framework stores: Java's `Persistable`, `Model`, `Entity`, `ModelEvent`, `Id`,
//! `GenericState`. A model is immutable: a change makes a new version through its builder
//! (`copy()...build()`), and a model carries the events of its change. An entity is tracked
//! the same way but emits no events.
//!
//! Rust has no inheritance, so the fields Java's base classes hold live in one field, `base`
//! (`ModelBase` / `EntityBase`), that `#[model]` / `#[entity]` add to the struct, together with
//! the builder (Java's `@AutoBuilder`).

mod entity;
mod event;
mod id;
mod model;
mod state;

pub use entity::{Entity, EntityBase, EntityBuilderBase};
pub use event::{AnyModelEvent, ModelEvent, event_type_name};
pub use id::Id;
pub use model::{Model, ModelBase, ModelBuilderBase};
pub use state::GenericState;

use std::fmt;

use uuid::Uuid;

use crate::{Error, ShardKey, ShardedId};

/// What a repository stores: Java's `Persistable`.
pub trait Persistable: Clone + Send + Sync + 'static {
    type Id: ModelId;

    fn id(&self) -> &Self::Id;
    fn version(&self) -> i64;
    /// The same object at the next version: Java's `nextVersion()`.
    fn next_version(&self) -> Self;
    /// `true` for a model, `false` for an entity: Java's `isModel()`.
    fn is_model() -> bool;
    /// The type's simple name, as Java's `getSimpleName()`: the event log's `model_type`.
    fn model_name() -> &'static str;
    /// The events this version carries, whatever their type; empty for an entity. The action
    /// executor writes them to the event log.
    fn emitted_events(&self) -> Vec<&dyn AnyModelEvent> {
        Vec::new()
    }
}

/// An id: a UUID that may know its shard. Java's `ModelId`.
pub trait ModelId:
    ShardKey + Clone + Eq + fmt::Debug + fmt::Display + Send + Sync + 'static
{
    fn uuid(&self) -> Uuid;
}

impl<M: 'static> ModelId for ShardedId<M> {
    fn uuid(&self) -> Uuid {
        ShardedId::uuid(self)
    }
}

pub(crate) fn missing(field: &str) -> Error {
    Error::Invalid(format!("{field} cannot be null"))
}
