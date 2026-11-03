//! Java's `ActionSpec` and `ActionAssert`: run an action's `perform` on a fresh plan, with no
//! database, and check what it staged and emitted.

use std::any::Any;

use ekbatan::action::{Action, ActionPlan};
use ekbatan::{AnyModelEvent, Error, Persistable};

/// Runs one action in a test: Java's `ActionSpec`. Nothing is written; `execute` hands back an
/// `ActionAssert` over the result and the plan.
pub struct ActionSpec<'a, A: Action> {
    action: &'a A,
    principal: Option<String>,
}

impl<'a, A: Action> ActionSpec<'a, A> {
    pub fn of(action: &'a A) -> Self {
        ActionSpec {
            action,
            principal: None,
        }
    }

    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Runs `perform`; panics with the error if it fails.
    pub async fn execute(&self, params: A::Params) -> ActionAssert<A::Result> {
        let mut plan = ActionPlan::new();
        match self
            .action
            .perform(&mut plan, self.principal.as_deref(), &params)
            .await
        {
            Ok(result) => ActionAssert { result, plan },
            Err(error) => panic!("Expected the action to succeed but it failed: {error}"),
        }
    }

    /// Runs `perform`, expects it to fail, and hands back the error. Java's
    /// `assertThrows(type, params, verifier)`: check the error's kind with `matches!` or
    /// `Error::action_error::<E>()`.
    pub async fn execute_err(&self, params: A::Params) -> Error {
        let mut plan = ActionPlan::new();
        match self
            .action
            .perform(&mut plan, self.principal.as_deref(), &params)
            .await
        {
            Ok(_) => panic!("Expected the action to fail but nothing was returned as an error"),
            Err(error) => error,
        }
    }

    /// Java's `assertThrows(type, params)` for an app error: expects `Error::Action` holding an
    /// `E`, and hands it to `verifier`.
    pub async fn assert_fails_with<E: std::error::Error + 'static>(
        &self,
        params: A::Params,
        verifier: impl FnOnce(&E),
    ) -> &Self {
        let error = self.execute_err(params).await;
        match error.action_error::<E>() {
            Some(found) => verifier(found),
            None => panic!("Expected {} but got {error:?}", std::any::type_name::<E>()),
        }
        self
    }
}

/// What an action staged, and what it returned: Java's `ActionAssert`.
pub struct ActionAssert<R> {
    result: R,
    plan: ActionPlan,
}

fn simple_name<T: ?Sized>() -> &'static str {
    let full = std::any::type_name::<T>();
    full.rsplit("::").next().unwrap_or(full)
}

impl<R> ActionAssert<R> {
    // ---- Additions

    /// Exactly one `E` was staged for insert; `verifier` checks it.
    pub fn assert_added_one<E: Persistable>(&self, verifier: impl FnOnce(&E)) -> &Self {
        let added = self.plan.additions::<E>();
        assert!(
            added.len() == 1,
            "Expected exactly 1 addition of {} but found {}",
            E::model_name(),
            added.len()
        );
        verifier(&added[0]);
        self
    }

    pub fn assert_added<E: Persistable>(&self, count: usize) -> &Self {
        let added = self.plan.additions::<E>();
        assert!(
            added.len() == count,
            "Expected {count} addition(s) of {} but found {}",
            E::model_name(),
            added.len()
        );
        self
    }

    pub fn assert_added_with<E: Persistable>(
        &self,
        count: usize,
        verifier: impl FnOnce(&[E]),
    ) -> &Self {
        self.assert_added::<E>(count);
        verifier(self.plan.additions::<E>());
        self
    }

    pub fn assert_no_additions(&self) -> &Self {
        let total: usize = self.plan.changes().map(|c| c.addition_count()).sum();
        assert!(total == 0, "Expected no additions but found {total}");
        self
    }

    pub fn assert_no_additions_of<E: Persistable>(&self) -> &Self {
        let found = self.plan.additions::<E>().len();
        assert!(
            found == 0,
            "Expected no additions of {} but found {found}",
            E::model_name()
        );
        self
    }

    pub fn assert_total_additions(&self, count: usize) -> &Self {
        let total: usize = self.plan.changes().map(|c| c.addition_count()).sum();
        assert!(
            total == count,
            "Expected {count} total addition(s) but found {total}"
        );
        self
    }

    // ---- Updates

    /// Exactly one `E` was staged for update; `verifier` checks it (as staged, before the
    /// version is raised).
    pub fn assert_updated_one<E: Persistable>(&self, verifier: impl FnOnce(&E)) -> &Self {
        let updated = self.plan.updates::<E>();
        assert!(
            updated.len() == 1,
            "Expected exactly 1 update of {} but found {}",
            E::model_name(),
            updated.len()
        );
        verifier(&updated[0]);
        self
    }

    pub fn assert_updated<E: Persistable>(&self, count: usize) -> &Self {
        let updated = self.plan.updates::<E>();
        assert!(
            updated.len() == count,
            "Expected {count} update(s) of {} but found {}",
            E::model_name(),
            updated.len()
        );
        self
    }

    pub fn assert_updated_with<E: Persistable>(
        &self,
        count: usize,
        verifier: impl FnOnce(&[E]),
    ) -> &Self {
        self.assert_updated::<E>(count);
        verifier(self.plan.updates::<E>());
        self
    }

    pub fn assert_no_updates(&self) -> &Self {
        let total: usize = self.plan.changes().map(|c| c.update_count()).sum();
        assert!(total == 0, "Expected no updates but found {total}");
        self
    }

    pub fn assert_no_updates_of<E: Persistable>(&self) -> &Self {
        let found = self.plan.updates::<E>().len();
        assert!(
            found == 0,
            "Expected no updates of {} but found {found}",
            E::model_name()
        );
        self
    }

    pub fn assert_total_updates(&self, count: usize) -> &Self {
        let total: usize = self.plan.changes().map(|c| c.update_count()).sum();
        assert!(
            total == count,
            "Expected {count} total update(s) but found {total}"
        );
        self
    }

    // ---- Events

    /// At least one event of type `V`.
    pub fn assert_emitted<V: AnyModelEvent>(&self) -> &Self {
        let found = self.events::<V>().len();
        assert!(
            found > 0,
            "Expected at least 1 event of {} but none were emitted",
            simple_name::<V>()
        );
        self
    }

    /// Exactly one event of type `V`; `verifier` checks it.
    pub fn assert_emitted_one<V: AnyModelEvent>(&self, verifier: impl FnOnce(&V)) -> &Self {
        let events = self.events::<V>();
        assert!(
            events.len() == 1,
            "Expected exactly 1 event of {} but found {}",
            simple_name::<V>(),
            events.len()
        );
        verifier(events[0]);
        self
    }

    pub fn assert_emitted_count<V: AnyModelEvent>(&self, count: usize) -> &Self {
        let found = self.events::<V>().len();
        assert!(
            found == count,
            "Expected {count} event(s) of {} but found {found}",
            simple_name::<V>()
        );
        self
    }

    pub fn assert_emitted_with<V: AnyModelEvent>(
        &self,
        count: usize,
        verifier: impl FnOnce(&[&V]),
    ) -> &Self {
        self.assert_emitted_count::<V>(count);
        verifier(&self.events::<V>());
        self
    }

    pub fn assert_not_emitted<V: AnyModelEvent>(&self) -> &Self {
        let found = self.events::<V>().len();
        assert!(
            found == 0,
            "Expected no events of {} but found {found}",
            simple_name::<V>()
        );
        self
    }

    pub fn assert_no_events(&self) -> &Self {
        let total = self.plan.events().len();
        assert!(total == 0, "Expected no events but found {total}");
        self
    }

    // ---- No changes

    pub fn assert_no_changes(&self) -> &Self {
        self.assert_no_additions();
        self.assert_no_updates();
        self.assert_no_events();
        self
    }

    pub fn assert_no_changes_of<E: Persistable>(&self) -> &Self {
        self.assert_no_additions_of::<E>();
        self.assert_no_updates_of::<E>();
        self
    }

    // ---- Result and access

    pub fn assert_result(&self, verifier: impl FnOnce(&R)) -> &Self {
        verifier(&self.result);
        self
    }

    pub fn result(&self) -> &R {
        &self.result
    }

    pub fn into_result(self) -> R {
        self.result
    }

    pub fn additions<E: Persistable>(&self) -> &[E] {
        self.plan.additions::<E>()
    }

    pub fn updates<E: Persistable>(&self) -> &[E] {
        self.plan.updates::<E>()
    }

    pub fn plan(&self) -> &ActionPlan {
        &self.plan
    }

    /// The staged events of type `V`, in order.
    pub fn events<V: AnyModelEvent>(&self) -> Vec<&V> {
        self.plan
            .events()
            .into_iter()
            .filter_map(|event| (event.as_any() as &dyn Any).downcast_ref::<V>())
            .collect()
    }
}
