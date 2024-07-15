mod config;
mod error;

use self::error::SpanErr;
use clap::error::Result;
use dotenvy::dotenv;
use thiserror::Error;
use tracing::instrument;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _ = dotenv();
    tracing_subscriber::Registry::default()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
        )
        .with(ErrorLayer::default())
        .try_init()
        .expect("failed to initialize tracing subscriber");

    tracing::info!("s3-reproxy v{}", env!("CARGO_PKG_VERSION"));

    if let Err(e) = s3_reproxy().await {
        tracing::error!("{}", e.error);
        eprintln!("{}", color_spantrace::colorize(&e.span));
    }
}

#[derive(Error, Debug)]
enum S3ProxyError {}

#[instrument]
async fn s3_reproxy() -> Result<(), SpanErr<S3ProxyError>> {
    Ok(())
}
