//! Java's `ActionPlan` and `PersistableChanges`: what one action execution means to write.

use std::any::{Any, TypeId};
use std::collections::HashMap;

use uuid::Uuid;

use crate::{AnyModelEvent, Error, ModelId, Persistable};

/// What one action execution means to write: Java's `ActionPlan`. Created fresh for each
/// execution (and each retry) and handed to `AsyncAction::perform`. Java binds it in a `ScopedValue`
/// and the action reaches it through `plan()`; Rust has no such thing, so it is a parameter.
#[derive(Default)]
pub struct ActionPlan {
    changes_by_type: Vec<(TypeId, Box<dyn AnyChanges>)>,
}

impl ActionPlan {
    pub fn new() -> Self {
        ActionPlan::default()
    }

    /// Stages a model or entity for insert. Returns it unchanged, for fluent use.
    pub fn add<E: Persistable>(&mut self, persistable: E) -> Result<E, Error> {
        self.changes_mut::<E>().add(persistable.clone())?;
        Ok(persistable)
    }

    /// Stages several for insert.
    pub fn add_all<E: Persistable>(&mut self, persistables: Vec<E>) -> Result<Vec<E>, Error> {
        persistables.into_iter().map(|p| self.add(p)).collect()
    }

    /// Stages one for update. Returns it at `version + 1`, as it will be once written, so the
    /// action can go on from there.
    pub fn update<E: Persistable>(&mut self, persistable: E) -> Result<E, Error> {
        let next = persistable.next_version();
        self.changes_mut::<E>().update(persistable)?;
        Ok(next)
    }

    /// Stages several for update; returns them at `version + 1`, in the same order.
    pub fn update_all<E: Persistable>(&mut self, persistables: Vec<E>) -> Result<Vec<E>, Error> {
        persistables.into_iter().map(|p| self.update(p)).collect()
    }

    /// The additions staged for one type, in the order staged.
    pub fn additions<E: Persistable>(&self) -> &[E] {
        self.typed_changes::<E>()
            .map_or(&[], |changes| &changes.additions)
    }

    /// The updates staged for one type, in the order staged (as given, not `version + 1`).
    pub fn updates<E: Persistable>(&self) -> &[E] {
        self.typed_changes::<E>()
            .map_or(&[], |changes| &changes.updates)
    }

    /// Whether anything was staged.
    pub fn has_changes(&self) -> bool {
        self.changes_by_type
            .iter()
            .any(|(_, changes)| changes.has_changes())
    }

    /// Every staged change, by type, in the order the types were first staged.
    pub fn changes(&self) -> impl Iterator<Item = &dyn AnyChanges> {
        self.changes_by_type.iter().map(|(_, changes)| &**changes)
    }

    /// Every event the staged models carry: additions first, then updates, type by type.
    pub fn events(&self) -> Vec<&dyn AnyModelEvent> {
        self.changes()
            .flat_map(|changes| changes.events())
            .collect()
    }

    pub(crate) fn into_changes(self) -> Vec<(TypeId, Box<dyn AnyChanges>)> {
        self.changes_by_type
    }

    fn typed_changes<E: Persistable>(&self) -> Option<&PersistableChanges<E>> {
        self.changes_by_type
            .iter()
            .find(|(type_id, _)| *type_id == TypeId::of::<E>())
            .and_then(|(_, changes)| changes.as_any().downcast_ref())
    }

    fn changes_mut<E: Persistable>(&mut self) -> &mut PersistableChanges<E> {
        let type_id = TypeId::of::<E>();
        let index = match self.changes_by_type.iter().position(|(t, _)| *t == type_id) {
            Some(index) => index,
            None => {
                self.changes_by_type
                    .push((type_id, Box::new(PersistableChanges::<E>::default())));
                self.changes_by_type.len() - 1
            }
        };
        self.changes_by_type[index]
            .1
            .as_any_mut()
            .downcast_mut()
            .expect("changes stored under their own type")
    }
}

/// One type's staged additions and updates: Java's `PersistableChanges`. Each row is written
/// once per action, so one id can't be staged twice.
pub struct PersistableChanges<E: Persistable> {
    pub(crate) additions: Vec<E>,
    pub(crate) updates: Vec<E>,
    staged: HashMap<Uuid, &'static str>,
}

impl<E: Persistable> Default for PersistableChanges<E> {
    fn default() -> Self {
        PersistableChanges {
            additions: Vec::new(),
            updates: Vec::new(),
            staged: HashMap::new(),
        }
    }
}

impl<E: Persistable> PersistableChanges<E> {
    pub fn additions(&self) -> &[E] {
        &self.additions
    }

    pub fn updates(&self) -> &[E] {
        &self.updates
    }

    pub(crate) fn add(&mut self, persistable: E) -> Result<(), Error> {
        self.check_not_registered(&persistable, "add")?;
        self.staged.insert(persistable.id().uuid(), "addition");
        self.additions.push(persistable);
        Ok(())
    }

    pub(crate) fn update(&mut self, persistable: E) -> Result<(), Error> {
        self.check_not_registered(&persistable, "update")?;
        self.staged.insert(persistable.id().uuid(), "update");
        self.updates.push(persistable);
        Ok(())
    }

    fn check_not_registered(&self, persistable: &E, operation: &str) -> Result<(), Error> {
        let id = persistable.id();
        match self.staged.get(&id.uuid()) {
            None => Ok(()),
            Some(existing) => Err(Error::Unexpected(format!(
                "{} {id} is already registered for {existing} operation, cannot {operation}. \
                 An action writes each row once, so two changes to one row must be composed \
                 before they are staged - wallet.deposit(10)?.withdraw(5)? - and then staged \
                 with a single plan.update(...), rather than staged one after another.",
                E::model_name()
            ))),
        }
    }
}

/// One type's staged changes, whatever the type: what the executor and the event log need.
pub trait AnyChanges: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// The staged type's simple name.
    fn model_name(&self) -> &'static str;
    fn has_changes(&self) -> bool;
    fn addition_count(&self) -> usize;
    fn update_count(&self) -> usize;
    /// The events of the staged models: additions first, then updates.
    fn events(&self) -> Vec<&dyn AnyModelEvent>;
    /// The events with the id of the model each belongs to, for the event log.
    fn events_with_model_ids(&self) -> Vec<(String, &dyn AnyModelEvent)>;
}

impl<E: Persistable> AnyChanges for PersistableChanges<E> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn model_name(&self) -> &'static str {
        E::model_name()
    }

    fn has_changes(&self) -> bool {
        !(self.additions.is_empty() && self.updates.is_empty())
    }

    fn addition_count(&self) -> usize {
        self.additions.len()
    }

    fn update_count(&self) -> usize {
        self.updates.len()
    }

    fn events(&self) -> Vec<&dyn AnyModelEvent> {
        self.additions
            .iter()
            .chain(&self.updates)
            .flat_map(Persistable::emitted_events)
            .collect()
    }

    fn events_with_model_ids(&self) -> Vec<(String, &dyn AnyModelEvent)> {
        self.additions
            .iter()
            .chain(&self.updates)
            .flat_map(|persistable| {
                let model_id = persistable.id().uuid().to_string();
                persistable
                    .emitted_events()
                    .into_iter()
                    .map(move |event| (model_id.clone(), event))
            })
            .collect()
    }
}
