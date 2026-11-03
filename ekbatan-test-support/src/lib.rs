//! What tests of an ekbatan app use: Java's `ekbatan-test-support`. `blocking::ActionSpec` runs a
//! blocking action, `aio::AsyncActionSpec` an async one.

mod action;
pub mod aio;
pub mod blocking;
mod virtual_clock;

pub use action::ActionAssert;
pub use virtual_clock::VirtualClock;
