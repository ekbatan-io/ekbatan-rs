//! `GenericState`: the two states most entities need. Java's `GenericState`.

use std::hash::Hash;

/// The two states most entities need: Java's `GenericState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GenericState {
    Active,
    Deleted,
}

impl GenericState {
    /// The text in the `state` column: Java's enum names.
    pub fn as_str(self) -> &'static str {
        match self {
            GenericState::Active => "ACTIVE",
            GenericState::Deleted => "DELETED",
        }
    }

    pub fn parse(text: &str) -> Option<GenericState> {
        match text {
            "ACTIVE" => Some(GenericState::Active),
            "DELETED" => Some(GenericState::Deleted),
            _ => None,
        }
    }
}
