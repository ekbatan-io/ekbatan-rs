//! What the framework stores: Java's `Persistable`, `Model`, `Entity`, `ModelEvent`, `Id`,
//! `GenericState`. A model is immutable: a change makes a new version through its builder
//! (`copy()...build()`), and a model carries the events of its change. An entity is tracked
//! the same way but emits no events.
//!
//! Rust has no inheritance, so the fields Java's base classes hold live in one field, `base`
//! (`ModelBase` / `EntityBase`), that `#[model]` / `#[entity]` add to the struct, together with
//! the builder (Java's `@AutoBuilder`).

use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use uuid::Uuid;

use crate::{Error, Instant, ShardKey, ShardedId, ShardingStrategy, truncate_to_micros};

/// What a repository stores: Java's `Persistable`.
pub trait Persistable: Clone + Send + Sync + 'static {
    type Id: ModelId;

    fn id(&self) -> &Self::Id;
    fn version(&self) -> i64;
    /// The same object at the next version: Java's `nextVersion()`.
    fn next_version(&self) -> Self;
    /// `true` for a model, `false` for an entity: Java's `isModel()`.
    fn is_model() -> bool;
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

/// A persistent aggregate that emits events: Java's `Model`. `#[model]` implements it.
pub trait Model: Persistable {
    type State: Copy + Eq + fmt::Debug + Send + Sync + 'static;
    type Builder;

    fn base(&self) -> &ModelBase<Self>;
    /// A builder holding this model's values: the start of every change. Java's `copy()`.
    fn copy(&self) -> Self::Builder;

    fn state(&self) -> Self::State {
        self.base().state
    }

    fn created_date(&self) -> Instant {
        self.base().created_date
    }

    fn updated_date(&self) -> Instant {
        self.base().updated_date
    }

    /// The events attached to this version; written to the event log when the action commits.
    fn events(&self) -> &[Arc<dyn ModelEvent<Self>>] {
        &self.base().events
    }
}

/// Persistent state whose changes don't belong in the event stream: Java's `Entity`.
/// `#[entity]` implements it.
pub trait Entity: Persistable {
    type State: Copy + Eq + fmt::Debug + Send + Sync + 'static;
    type Builder;

    fn base(&self) -> &EntityBase<Self>;
    /// A builder holding this entity's values. Java's `copy()`.
    fn copy(&self) -> Self::Builder;

    fn state(&self) -> Self::State {
        self.base().state
    }
}

/// An event a model emits: Java's `ModelEvent`. `#[model_event(Model)]` implements it, and
/// derives serde's `Serialize` / `Deserialize` with Java's camelCase field names. The event
/// log stores the model's id and name with it, so an event doesn't repeat them.
pub trait ModelEvent<M>: fmt::Debug + Send + Sync + 'static {
    /// The event's type name, as Java uses the simple class name: `WalletCreatedEvent`.
    fn event_type(&self) -> &'static str {
        short_name(std::any::type_name::<Self>())
    }
}

/// The fields every model has: what Java's `Model` superclass holds. Built only by a builder,
/// which checks them.
pub struct ModelBase<M: Model> {
    pub(crate) id: M::Id,
    pub(crate) state: M::State,
    pub(crate) version: i64,
    pub(crate) created_date: Instant,
    pub(crate) updated_date: Instant,
    pub(crate) events: Vec<Arc<dyn ModelEvent<M>>>,
}

impl<M: Model> ModelBase<M> {
    pub fn id(&self) -> &M::Id {
        &self.id
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    /// The next version: same values and events, version + 1. Java's `nextVersion()`.
    pub fn next_version(&self) -> Self {
        ModelBase {
            version: self.version + 1,
            ..self.clone()
        }
    }
}

impl<M: Model> Clone for ModelBase<M> {
    fn clone(&self) -> Self {
        ModelBase {
            id: self.id.clone(),
            state: self.state,
            version: self.version,
            created_date: self.created_date,
            updated_date: self.updated_date,
            events: self.events.clone(),
        }
    }
}

/// Equal when id, state, version and both dates match. Events don't count, as in Java: a row
/// read back equals the version that was written with events.
impl<M: Model> PartialEq for ModelBase<M> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.state == other.state
            && self.version == other.version
            && self.created_date == other.created_date
            && self.updated_date == other.updated_date
    }
}

impl<M: Model> fmt::Debug for ModelBase<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let events: Vec<_> = self.events.iter().map(|event| event.event_type()).collect();
        f.debug_struct("ModelBase")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("version", &self.version)
            .field("created_date", &self.created_date)
            .field("updated_date", &self.updated_date)
            .field("events", &events)
            .finish()
    }
}

/// The builder's half of `ModelBase`: Java's `Model.Builder`. Generated builders hold one.
#[doc(hidden)]
pub struct ModelBuilderBase<M: Model> {
    pub id: Option<M::Id>,
    pub state: Option<M::State>,
    pub version: Option<i64>,
    pub created_date: Option<Instant>,
    pub updated_date: Option<Instant>,
    pub events: Vec<Arc<dyn ModelEvent<M>>>,
}

impl<M: Model> Default for ModelBuilderBase<M> {
    fn default() -> Self {
        ModelBuilderBase {
            id: None,
            state: None,
            version: None,
            created_date: None,
            updated_date: None,
            events: Vec::new(),
        }
    }
}

impl<M: Model> ModelBuilderBase<M> {
    /// Java's `copyBase(model)`: id, state, dates, version and events.
    pub fn copy_of(base: &ModelBase<M>) -> Self {
        ModelBuilderBase {
            id: Some(base.id.clone()),
            state: Some(base.state),
            version: Some(base.version),
            created_date: Some(base.created_date),
            updated_date: Some(base.updated_date),
            events: base.events.clone(),
        }
    }

    /// Java's `Model` constructor: every field set, version at least 1, dates cut to
    /// microseconds, `updated_date` defaulting to `created_date`.
    pub fn build(self) -> Result<ModelBase<M>, Error> {
        let id = self.id.ok_or_else(|| missing("id"))?;
        let state = self.state.ok_or_else(|| missing("state"))?;
        let created_date =
            truncate_to_micros(self.created_date.ok_or_else(|| missing("createdDate"))?);
        let version = self.version.ok_or_else(|| missing("version"))?;
        if version < 1 {
            return Err(Error::Invalid(
                "version must be greater than or equal to 1".to_owned(),
            ));
        }
        Ok(ModelBase {
            id,
            state,
            version,
            created_date,
            updated_date: self.updated_date.map_or(created_date, truncate_to_micros),
            events: self.events,
        })
    }
}

/// The fields every entity has: what Java's `Entity` superclass holds.
pub struct EntityBase<E: Entity> {
    pub(crate) id: E::Id,
    pub(crate) state: E::State,
    pub(crate) version: i64,
}

impl<E: Entity> EntityBase<E> {
    pub fn id(&self) -> &E::Id {
        &self.id
    }

    pub fn version(&self) -> i64 {
        self.version
    }

    /// The next version. Java's `nextVersion()`.
    pub fn next_version(&self) -> Self {
        EntityBase {
            version: self.version + 1,
            ..self.clone()
        }
    }
}

impl<E: Entity> Clone for EntityBase<E> {
    fn clone(&self) -> Self {
        EntityBase {
            id: self.id.clone(),
            state: self.state,
            version: self.version,
        }
    }
}

/// Equal when id, state and version match, as Java's `Entity.equals`.
impl<E: Entity> PartialEq for EntityBase<E> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.state == other.state && self.version == other.version
    }
}

impl<E: Entity> fmt::Debug for EntityBase<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityBase")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("version", &self.version)
            .finish()
    }
}

/// The builder's half of `EntityBase`: Java's `Entity.Builder`.
#[doc(hidden)]
pub struct EntityBuilderBase<E: Entity> {
    pub id: Option<E::Id>,
    pub state: Option<E::State>,
    pub version: Option<i64>,
}

impl<E: Entity> Default for EntityBuilderBase<E> {
    fn default() -> Self {
        EntityBuilderBase {
            id: None,
            state: None,
            version: None,
        }
    }
}

impl<E: Entity> EntityBuilderBase<E> {
    /// Java's `copyBase(entity)`: id, state and version.
    pub fn copy_of(base: &EntityBase<E>) -> Self {
        EntityBuilderBase {
            id: Some(base.id.clone()),
            state: Some(base.state),
            version: Some(base.version),
        }
    }

    /// Java's `Entity` constructor: every field set, version at least 1.
    pub fn build(self) -> Result<EntityBase<E>, Error> {
        let id = self.id.ok_or_else(|| missing("id"))?;
        let state = self.state.ok_or_else(|| missing("state"))?;
        let version = self.version.ok_or_else(|| missing("version"))?;
        if version < 1 {
            return Err(Error::Invalid("version must be >= 1".to_owned()));
        }
        Ok(EntityBase { id, state, version })
    }
}

fn missing(field: &str) -> Error {
    Error::Invalid(format!("{field} cannot be null"))
}

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

/// The two states most entities need: Java's `GenericState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GenericState {
    Active,
    Deleted,
}

impl GenericState {
    /// The text in the `state` column: Java's enum names.
    pub fn as_str(self) -> &'static str {
        match self {
            GenericState::Active => "ACTIVE",
            GenericState::Deleted => "DELETED",
        }
    }

    pub fn parse(text: &str) -> Option<GenericState> {
        match text {
            "ACTIVE" => Some(GenericState::Active),
            "DELETED" => Some(GenericState::Deleted),
            _ => None,
        }
    }
}

/// `wallet_example::model::WalletCreatedEvent` -> `WalletCreatedEvent`: Java's simple class name.
pub(crate) fn short_name(full: &'static str) -> &'static str {
    full.rsplit("::").next().unwrap_or(full)
}
