//! `Model`: something with history, immutable, changed through its builder, carrying the
//! events of its change. Java's `Model`; `ModelBase` and `ModelBuilderBase` hold what Java's
//! base classes hold.

use std::fmt;
use std::sync::Arc;

use super::{ModelEvent, Persistable, missing};
use crate::{Error, Instant, truncate_to_micros};

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
