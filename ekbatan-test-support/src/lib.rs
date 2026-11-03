//! What tests of an ekbatan app use: Java's `ekbatan-test-support`.

mod action;
mod sync_spec;
mod virtual_clock;

pub use action::ActionAssert;
pub use sync_spec::ActionSpec;
pub use virtual_clock::VirtualClock;

/// The async twins: `AsyncActionSpec` runs an `AsyncAction`'s `perform` with `.await`.
pub mod aio {
    pub use crate::action::AsyncActionSpec;
}
