//! `ActionSpec`: runs a blocking `Action`'s `perform` on the test's thread. `aio::AsyncActionSpec`
//! is the async twin.

use ekbatan::Error;
use ekbatan::action::ActionPlan;
use ekbatan::blocking::Action;

use crate::ActionAssert;

/// Runs one action in a test: Java's `ActionSpec`. Nothing is written; `execute`
/// hands back an `ActionAssert` over the result and the plan.
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
    pub fn execute(&self, params: A::Params) -> ActionAssert<A::Result> {
        let mut plan = ActionPlan::new();
        match self
            .action
            .perform(&mut plan, self.principal.as_deref(), &params)
        {
            Ok(result) => ActionAssert::new(result, plan),
            Err(error) => panic!("Expected the action to succeed but it failed: {error}"),
        }
    }

    /// Runs `perform`, expects it to fail, and hands back the error.
    pub fn execute_err(&self, params: A::Params) -> Error {
        let mut plan = ActionPlan::new();
        match self
            .action
            .perform(&mut plan, self.principal.as_deref(), &params)
        {
            Ok(_) => panic!("Expected the action to fail but nothing was returned as an error"),
            Err(error) => error,
        }
    }

    /// For an app error: expects `Error::Action` holding an `E`, and hands it to `verifier`.
    pub fn assert_fails_with<E: std::error::Error + 'static>(
        &self,
        params: A::Params,
        verifier: impl FnOnce(&E),
    ) -> &Self {
        let error = self.execute_err(params);
        match error.action_error::<E>() {
            Some(found) => verifier(found),
            None => panic!("Expected {} but got {error:?}", std::any::type_name::<E>()),
        }
        self
    }
}
