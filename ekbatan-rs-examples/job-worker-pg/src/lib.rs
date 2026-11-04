//! A worker process with no HTTP: two distributed jobs and the event handler's jobs, on
//! Ekbatan's blocking API. Java's `spring-boot-job-worker-gradle-pg`, in Rust.
//!
//! - `jobs`: `WalletReportJob` (a scheduled query) and `WalletStipendJob` (a job that runs an
//!   action under a keyed lock).
//! - `handler`: each deposit becomes a notification.
//! - `app`: the wiring, from `ekbatan.json`.

pub mod action;
pub mod app;
pub mod handler;
pub mod jobs;
pub mod model;
pub mod repository;
pub mod schema;
