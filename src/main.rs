use std::net::Ipv4Addr;
use std::sync::Arc;

use crate::server::remote::spawn_remote;
use crate::server::S3Reproxy;
use clap::Parser;
use hyper_util::rt::{TokioExecutor, TokioIo};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tower::ServiceBuilder;
pub mod config;
pub mod error;
pub mod server;

use self::config::S3ReproxySetup;
use self::error::SpanErr;
use self::server::remote::RemoteMessage;
use clap::error::Result;
use dotenvy::dotenv;
use thiserror::Error;
use tracing::{info, instrument, Instrument};
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
    Setup(#[from] config::Error),

    #[error("Failed to bind to port: \n{0}")]
    Bind(std::io::Error),

    #[error("Failed to setup signal handler: \n{0}")]
    Signal(std::io::Error),

    #[error("Failed to communicate with remote task: \n{0}")]
    Remote(#[from] mpsc::error::SendError<RemoteMessage>),
}

#[instrument]
async fn s3_reproxy() -> Result<(), SpanErr<S3ProxyError>> {
    let args = config::AppArgs::parse();

    let setup = S3ReproxySetup::new(args)
        .await
        .map_err(|e| e.map(S3ProxyError::Setup))?;

    let mut remote_tasks = JoinSet::new();
    let remotes = Arc::new(
        setup
            .config
            .targets
            .into_iter()
            .map(|t| spawn_remote(t, &mut remote_tasks))
            .collect(),
    );

    let server = S3Reproxy {
        bucket: setup.args.bucket,
        remotes: Arc::clone(&remotes),
    };

    for r in remotes.iter() {
        r.tx.send(server::remote::RemoteMessage::HealthCheck {
            reply: tokio::sync::oneshot::channel().0,
        })
        .await
        .map_err(S3ProxyError::Remote)?;
    }

    let s3_service = {
        let mut builder = S3ServiceBuilder::new(server);
        builder.set_auth(SimpleAuth::from_single(
            setup.args.access_key,
            setup.args.secret_key,
        ));
        builder.build()
    };

    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, setup.args.port))
        .await
        .map_err(S3ProxyError::Bind)?;

    let hyper_s3_service = ServiceBuilder::new().service(s3_service.into_shared());

    let http_server = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut sigint = signal(SignalKind::interrupt()).map_err(S3ProxyError::Signal)?;
    let mut sigterm = signal(SignalKind::terminate()).map_err(S3ProxyError::Signal)?;

    loop {
        tokio::select! {
            _ = sigint.recv() => {
                tracing::info!("Received SIGINT, shutting down...");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM, shutting down...");
                break;
            }
            res = listener.accept() => {
                match res {
                    Ok((stream, _)) => {
                        let peer = stream.peer_addr().ok().map(|a| format!("{:?}", a));
                        let serve = graceful.watch(
                            http_server.serve_connection(TokioIo::new(stream), hyper_s3_service.clone()).into_owned()
                        );
                        tokio::spawn(async move {
                            let _ = serve.await;
                        }.instrument(
                            tracing::info_span!("connection", remote = peer)
                        ));

                    }
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {}", e);
                    }
                }
            }

        }
    }

    for r in remotes.iter() {
        r.tx.send(server::remote::RemoteMessage::Shutdown)
            .await
            .map_err(S3ProxyError::Remote)?;
    }

    graceful.shutdown().await;
    while (remote_tasks.join_next().await).is_some() {}

    info!("Server shutdown complete");

    Ok(())
}
