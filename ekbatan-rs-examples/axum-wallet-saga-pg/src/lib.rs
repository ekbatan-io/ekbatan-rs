//! A wallet transfer as a saga, on Ekbatan's async API and axum: Java's
//! `spring-boot-wallet-saga-gradle-pg`, in Rust. Three single-wallet actions chained by events
//! through the local event handler, with forward-only compensation.
//!
//! - `model`: `Wallet` (a model: versioned, emits events), with the transfer's steps and events,
//!   and `TransferStep` (an entity: the marker that makes a step run once).
//! - `action`: the wallet actions and the transfer's three; each stages its change, the
//!   executor writes it with its events.
//! - `repository`: `WalletRepository`, `TransferStepRepository`.
//! - `handler`: the two handlers that run step 2 and the compensation.
//! - `web`: the HTTP endpoints.
//! - `app`: builds it all from `ekbatan.json`: migrations, pools, executor, locks, jobs.

pub mod action;
pub mod app;
pub mod handler;
pub mod model;
pub mod repository;
pub mod schema;
pub mod web;
