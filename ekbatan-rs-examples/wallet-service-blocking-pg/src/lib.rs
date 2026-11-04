//! Start here: one wallet's life on Ekbatan's blocking API, in plain code with no async and no
//! server. `story::run` opens a wallet, deposits, reads it back, lists its events, closes it,
//! and shows a refused deposit, saying what happens at each step.

pub mod action;
pub mod model;
pub mod repository;
pub mod schema;
pub mod story;
