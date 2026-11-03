//! What tests of an ekbatan app use: Java's `ekbatan-test-support`.

mod action;
mod virtual_clock;

pub use action::{ActionAssert, ActionSpec};
pub use virtual_clock::VirtualClock;
