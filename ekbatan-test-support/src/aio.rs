//! `AsyncActionSpec`: the async twin of `ActionSpec`, for an `AsyncAction`; `perform` is
//! awaited.

use ekbatan::Error;
use ekbatan::action::ActionPlan;
use ekbatan::aio::AsyncAction;

use crate::ActionAssert;

/// Runs one async action in a test, as Java's `ActionSpec`. Nothing is written; `execute` hands back an
/// `ActionAssert` over the result and the plan.
pub struct AsyncActionSpec<'a, A: AsyncAction> {
    action: &'a A,
    principal: Option<String>,
}

impl<'a, A: AsyncAction> AsyncActionSpec<'a, A> {
    pub fn of(action: &'a A) -> Self {
        AsyncActionSpec {
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
            Ok(result) => ActionAssert::new(result, plan),
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
