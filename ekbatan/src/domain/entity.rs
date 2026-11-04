//! `Entity`: tracked like a model, but with no events. Java's `Entity`; `EntityBase` and
//! `EntityBuilderBase` hold what Java's base classes hold.

use std::fmt;

use super::{Persistable, missing};
use crate::Error;

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
