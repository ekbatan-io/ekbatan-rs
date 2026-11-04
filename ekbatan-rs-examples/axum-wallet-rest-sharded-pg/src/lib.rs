//! A wallet service over HTTP, on Ekbatan's async API and axum: Java's
//! `spring-boot-wallet-rest-gradle-sharded-pg`, in Rust: two shards, one PostgreSQL server each.
//!
//! - `model`: `Wallet` (a model: versioned, emits events) and `Notification` (an entity).
//! - `action`: the four actions; each stages its change, the executor writes it with its events.
//! - `repository`: `WalletRepository`, `NotificationRepository`.
//! - `handler`: reacts to a committed deposit by recording a notification (a second action).
//! - `web`: the HTTP endpoints.
//! - `app`: builds it all from `ekbatan.json`: migrations, pools, executor, locks, jobs.

pub mod action;
pub mod app;
pub mod handler;
pub mod model;
pub mod repository;
pub mod schema;
pub mod web;
