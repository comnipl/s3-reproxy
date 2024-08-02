use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::Client;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{info, instrument, warn, Instrument};

use crate::config::s3_target::S3Target;

#[derive(Debug)]
pub struct S3Remote {
    pub name: String,
    pub priority: u32,
    pub read_request: bool,
    pub tx: mpsc::Sender<RemoteMessage>,
}

pub enum RemoteMessage {
    HealthCheck {
        reply: oneshot::Sender<bool>,
    },
    ListObjects {
        prefix: Option<String>,
        delimiter: Option<String>,
        max_keys: Option<i32>,
        start_after: Option<String>,
        reply: oneshot::Sender<Option<ListObjectsV2Output>>,
    },
    Shutdown,
}

// TODO: ここらへんのunwrap削減するぞ！
#[instrument(name = "remote", skip_all, fields(name = target.name, bucket = target.s3.bucket))]
pub fn spawn_remote(target: S3Target, set: &mut JoinSet<()>) -> S3Remote {
    let s3_config = aws_sdk_s3::config::Builder::new()
        .endpoint_url(target.s3.endpoint)
        .credentials_provider(Credentials::new(
            target.s3.access_key,
            target.s3.secret_key,
            None,
            None,
            "loaded-from-s3reproxy-config",
        ))
        .region(Region::new(""))
        .force_path_style(true)
        .behavior_version_latest()
        .build();

    let client = Client::from_conf(s3_config);

    info!("Created new remote client.");

    let (tx, mut rx) = mpsc::channel(32);

    set.spawn(
        async move {
            let mut health: Option<bool> = None;

            loop {
                tokio::select! {
                    Some(msg) = rx.recv() => match msg {
                        RemoteMessage::HealthCheck { reply } => {
                            info!("Checking health...");
                            let q = client.head_bucket().bucket(target.s3.bucket.clone()).send().await;
                            map_health(&mut health, &q);
                            let _ = reply.send(match q {
                                Ok(_) => true,
                                Err(e) => {
                                    warn!("Health check failed: {:?}", e);
                                    false
                                },
                            });
                        }
                        RemoteMessage::ListObjects { prefix, delimiter, max_keys, start_after, reply } => {
                            info!("Listing objects...");
                            let q = client.list_objects_v2()
                                .bucket(target.s3.bucket.clone())
                                .set_prefix(prefix)
                                .set_start_after(start_after)
                                .set_delimiter(delimiter)
                                .set_max_keys(max_keys)
                                .send()
                                .await;
                            map_health(&mut health, &q);

                            let a = q.map_err(|e| {
                                warn!("List objects failed: {:?}", e);
                                s3s::dto::ListObjectsV2Output::default()
                            }).ok();

                            reply.send(a).unwrap();
                        }
                        RemoteMessage::Shutdown => {
                            break;
                        }
                    }
                }
            }

            info!("Remote shutting down.");
        }
        .in_current_span(),
    );
    S3Remote {
        name: target.name,
        priority: target.priority,
        read_request: target.read_request,
        tx,
    }
}

#[instrument(name = "remote/health", skip_all)]
fn map_health<T, E>(self_health: &mut Option<bool>, query: &Result<T, E>) {
    let health = query.is_ok();
    if *self_health != Some(health) {
        if health {
            info!("remote is UP")
        } else {
            warn!("remote is DOWN")
        }
        *self_health = Some(health);
    }
}
