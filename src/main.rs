use clap::Parser;
mod config;
mod error;

use self::config::S3ReproxySetup;
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
        tracing::error!(
            "s3-reproxy stopped due to following error:\n\n\x1b[31m\x1b[1m{}\x1b[m\n\n{}",
            e.error,
            color_spantrace::colorize(&e.span)
        );
    }
}

#[derive(Error, Debug)]
enum S3ProxyError {
    #[error("Failed to setup s3-reproxy: \n{0}")]
    SetupError(#[from] config::Error),
}

#[instrument]
async fn s3_reproxy() -> Result<(), SpanErr<S3ProxyError>> {
    let args = config::AppArgs::parse();

    let setup = S3ReproxySetup::new(args)
        .await
        .map_err(|e| e.map(S3ProxyError::SetupError))?;

    tracing::info!("{:?}", setup.config);

    Ok(())
}
