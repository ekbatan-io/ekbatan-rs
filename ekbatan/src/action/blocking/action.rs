//! `Action`: an action whose `perform` is a plain `fn`. `aio::AsyncAction` is the async twin;
//! every `Action` is also an `AsyncAction`.

use serde::Serialize;

use crate::Error;
use crate::action::ActionPlan;
use crate::aio::AsyncAction;

/// One unit of domain work: Java's `Action<PARAM, RESULT>`. One instance serves every call, so
/// it holds only what its constructor gave it (repositories, a clock); what one call stages goes
/// on the `plan` it is handed.
///
/// `perform` returns `Err` to abort: nothing it staged is written. An app's own error goes in
/// `Error::action(...)`. Every `Action` is also an `AsyncAction` (its async twin), so it
/// registers with `ActionRegistry::builder().with_action(...)` like any other.
pub trait Action: Send + Sync + 'static {
    /// The input. Written to the event log as JSON, so it must serialize to a JSON object.
    type Params: Serialize + Send + Sync;
    type Result: Send;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        principal: Option<&str>,
        params: &Self::Params,
    ) -> Result<Self::Result, Error>;
}

impl<A: Action> AsyncAction for A {
    type Params = A::Params;
    type Result = A::Result;

    async fn perform(
        &self,
        plan: &mut ActionPlan,
        principal: Option<&str>,
        params: &A::Params,
    ) -> Result<A::Result, Error> {
        tokio::task::block_in_place(|| Action::perform(self, plan, principal, params))
    }
}
