//! Serves the wallet API on `PORT` (8080), configured by the file `EKBATAN_CONFIG` names
//! (`ekbatan.json`). Ctrl-C stops it: requests in flight finish, jobs stop, pools close.

use axum_wallet_rest_mariadb::app::App;
use ekbatan::EkbatanConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let app = App::start(&config).await?;

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_owned());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    println!("wallet API on http://localhost:{port}/wallets");
    axum::serve(listener, app.router.clone())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    app.stop().await;
    Ok(())
}
