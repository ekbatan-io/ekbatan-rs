//! The blocking kind: `Action` (a plain `fn perform`) and `ActionExecutor`.

mod action;
mod executor;

pub use action::Action;
pub use executor::{ActionExecutor, ActionExecutorBuilder};
