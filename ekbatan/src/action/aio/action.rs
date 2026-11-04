//! `AsyncAction`: an action whose `perform` is awaited. `blocking::Action` is its blocking twin; every
//! `Action` is also an `AsyncAction`.

use std::future::Future;

use serde::Serialize;

use crate::Error;
use crate::action::ActionPlan;

/// One unit of domain work, with an async `perform`: the async twin of `Action` (Java's
/// `Action<PARAM, RESULT>`). One instance serves every call, so
/// it holds only what its constructor gave it (repositories, a clock); what one call stages goes
/// on the `plan` it is handed.
///
/// `perform` returns `Err` to abort: nothing it staged is written. An app's own error goes in
/// `Error::action(...)`.
pub trait AsyncAction: Send + Sync + 'static {
    /// The input. Written to the event log as JSON, so it must serialize to a JSON object.
    type Params: Serialize + Send + Sync;
    type Result: Send;

    fn perform(
        &self,
        plan: &mut ActionPlan,
        principal: Option<&str>,
        params: &Self::Params,
    ) -> impl Future<Output = Result<Self::Result, Error>> + Send;
}
