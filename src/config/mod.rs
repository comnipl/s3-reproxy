use clap::Parser;
use derivative::Derivative;
use std::path::PathBuf;
use thiserror::Error;
use tokio::fs;
use tracing::instrument;

use crate::error::SpanErr;

use self::s3_target::Config;

pub mod s3_target;

#[derive(Parser, Derivative)]
#[derivative(Debug)]
#[clap(
    name = "s3-reproxy",
    version = env!("CARGO_PKG_VERSION"),
    author = "AsPulse (pus' uite)",
    about = "A transparent proxy for S3 replication"
)]
pub(crate) struct AppArgs {
    #[clap(long)]
    pub config_file: PathBuf,

    #[clap(long, default_value = "9000", env = "PORT")]
    pub port: u16,

    #[clap(long, env = "ACCESS_KEY")]
    pub access_key: String,

    #[clap(long, env = "SECRET_KEY", hide_env_values = true)]
    #[derivative(Debug = "ignore")]
    pub secret_key: String,

    #[clap(long, env = "BUCKET")]
    pub bucket: String,

    #[clap(long, env = "MONGO_URI", hide_env_values = true)]
    pub mongo_uri: String,

    #[clap(long, env = "MONGO_DB")]
    pub mongo_db: String,
}

#[derive(Debug)]
pub(crate) struct S3ReproxySetup {
    pub config: Config,
    pub args: AppArgs,
}

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("Failed to read config file {0}: {1}")]
    Io(PathBuf, #[source] std::io::Error),

    #[error("Failed to parse config file {0}:\n {1}")]
    Serde(PathBuf, #[source] serde_yaml::Error),

    #[error("At least one readable target must be specified")]
    MissingReadableTarget,
}

impl S3ReproxySetup {
    #[instrument(name = "setup")]
    pub async fn new(args: AppArgs) -> Result<Self, SpanErr<Error>> {
        let config_slice = fs::read(&args.config_file)
            .await
            .map_err(|e| Error::Io(args.config_file.clone(), e))?;

        let config: Config = serde_yaml::from_slice(&config_slice)
            .map_err(|e| Error::Serde(args.config_file.clone(), e))?;

        let setup = Self { config, args };

        Self::validate_config(&setup)?;

        Ok(setup)
    }

    /// TODO: 名前の重複に対してエラーを出
    #[instrument(name = "setup/validation")]
    fn validate_config(setup: &Self) -> Result<(), SpanErr<Error>> {
        if setup
            .config
            .targets
            .iter()
            .filter(|t| t.read_request)
            .count()
            < 1
        {
            Err(Error::MissingReadableTarget)?;
        }

        Ok(())
    }
}
