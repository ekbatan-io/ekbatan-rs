//! Runs the worker until Ctrl-C, configured by the file `EKBATAN_CONFIG` names (`ekbatan.json`).
//! A plain `fn main`: no async runtime of the app's own.

use std::sync::mpsc;

use ekbatan::EkbatanConfig;
use job_worker_pg::app::Worker;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logs to stderr: `info` unless RUST_LOG says otherwise (RUST_LOG=ekbatan=debug, ...).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,sqlx::postgres::notice=warn")
            }),
        )
        .init();
    let path = std::env::var("EKBATAN_CONFIG").unwrap_or_else(|_| "ekbatan.json".to_owned());
    let config = EkbatanConfig::load_json(&path)?;
    let worker = Worker::start(&config)?;
    println!("worker running: wallet-report every 5s, wallet-stipend every 2s; Ctrl-C stops it");

    let (stop, stopped) = mpsc::channel();
    ctrlc_on(stop);
    let _ = stopped.recv();
    println!("stopping: running jobs finish first");
    worker.stop();
    Ok(())
}

/// Sends on `stop` at Ctrl-C (SIGINT) or SIGTERM.
fn ctrlc_on(stop: mpsc::Sender<()>) {
    ekbatan::blocking::runtime().spawn(async move {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        let _ = stop.send(());
    });
}
