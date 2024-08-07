use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadInput, CompleteMultipartUploadOutput,
};
use aws_sdk_s3::operation::create_multipart_upload::{
    CreateMultipartUploadError, CreateMultipartUploadInput, CreateMultipartUploadOutput,
};
use aws_sdk_s3::operation::delete_object::{
    DeleteObjectError, DeleteObjectInput, DeleteObjectOutput,
};
use aws_sdk_s3::operation::delete_objects::{
    DeleteObjectsError, DeleteObjectsInput, DeleteObjectsOutput,
};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectInput, GetObjectOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOutput};
use aws_sdk_s3::operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectInput, PutObjectOutput};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::orchestrator;
use aws_smithy_runtime_api::client::result::ServiceError;
use std::fmt::Debug;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
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
        reply: oneshot::Sender<
            Option<
                Result<
                    ListObjectsV2Output,
                    ServiceError<ListObjectsV2Error, orchestrator::HttpResponse>,
                >,
            >,
        >,
    },
    HeadObject {
        input: HeadObjectInput,
        reply: oneshot::Sender<
            Option<
                Result<HeadObjectOutput, ServiceError<HeadObjectError, orchestrator::HttpResponse>>,
            >,
        >,
    },
    GetObject {
        input: GetObjectInput,
        reply: oneshot::Sender<
            Option<
                Result<GetObjectOutput, ServiceError<GetObjectError, orchestrator::HttpResponse>>,
            >,
        >,
    },
    PutObject {
        input: PutObjectInput,
        reply: oneshot::Sender<
            Option<
                Result<PutObjectOutput, ServiceError<PutObjectError, orchestrator::HttpResponse>>,
            >,
        >,
    },
    DeleteObject {
        input: DeleteObjectInput,
        reply: oneshot::Sender<
            Option<
                Result<
                    DeleteObjectOutput,
                    ServiceError<DeleteObjectError, orchestrator::HttpResponse>,
                >,
            >,
        >,
    },
    DeleteObjects {
        input: DeleteObjectsInput,
        reply: oneshot::Sender<
            Option<
                Result<
                    DeleteObjectsOutput,
                    ServiceError<DeleteObjectsError, orchestrator::HttpResponse>,
                >,
            >,
        >,
    },
    CreateMultiPartUpload {
        input: CreateMultipartUploadInput,
        reply: oneshot::Sender<
            Option<
                Result<
                    CreateMultipartUploadOutput,
                    ServiceError<CreateMultipartUploadError, orchestrator::HttpResponse>,
                >,
            >,
        >,
    },
    CompleteMultiPartUpload {
        input: CompleteMultipartUploadInput,
        reply: oneshot::Sender<
            Option<
                Result<
                    CompleteMultipartUploadOutput,
                    ServiceError<CompleteMultipartUploadError, orchestrator::HttpResponse>,
                >,
            >,
        >,
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
                            let q = map_health(&mut health, q);
                            let _ = reply.send(match q {
                                Some(Ok(_)) => true,
                                e => {
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
                            let _ = reply.send(map_health(&mut health, q));
                        }
                        RemoteMessage::GetObject { input, reply } => {
                            info!("Get object...");

                            let q = client.get_object()
                                .bucket(target.s3.bucket.clone())
                                .set_checksum_mode(input.checksum_mode)
                                .set_expected_bucket_owner(input.expected_bucket_owner)
                                .set_if_match(input.if_match)
                                .set_if_modified_since(input.if_modified_since)
                                .set_if_none_match(input.if_none_match)
                                .set_if_unmodified_since(input.if_unmodified_since)
                                .set_key(input.key)
                                .set_part_number(input.part_number)
                                .set_range(input.range)
                                .set_request_payer(input.request_payer)
                                .set_response_cache_control(input.response_cache_control)
                                .set_response_content_disposition(input.response_content_disposition)
                                .set_response_content_encoding(input.response_content_encoding)
                                .set_response_content_language(input.response_content_language)
                                .set_response_content_type(input.response_content_type)
                                .set_response_expires(input.response_expires)
                                .set_sse_customer_algorithm(input.sse_customer_algorithm)
                                .set_sse_customer_key(input.sse_customer_key)
                                .set_sse_customer_key_md5(input.sse_customer_key_md5)
                                .set_version_id(input.version_id)
                                .send()
                                .await;

                            let _ = reply.send(map_health(&mut health, q));
                        }
                        RemoteMessage::PutObject { input, reply } => {
                            info!("Put object...");
                            let q = client.put_object()
                                .bucket(target.s3.bucket.clone())
                                .set_acl(input.acl)
                                .body(input.body)
                                .set_cache_control(input.cache_control)
                                .set_content_disposition(input.content_disposition)
                                .set_content_encoding(input.content_encoding)
                                .set_content_language(input.content_language)
                                .set_content_length(input.content_length)
                                .set_content_md5(input.content_md5)
                                .set_content_type(input.content_type)
                                .set_checksum_algorithm(input.checksum_algorithm)
                                .set_checksum_crc32(input.checksum_crc32)
                                .set_checksum_crc32_c(input.checksum_crc32_c)
                                .set_checksum_sha1(input.checksum_sha1)
                                .set_checksum_sha256(input.checksum_sha256)
                                .set_expires(input.expires)
                                .set_grant_full_control(input.grant_full_control)
                                .set_grant_read(input.grant_read)
                                .set_grant_read_acp(input.grant_read_acp)
                                .set_grant_write_acp(input.grant_write_acp)
                                .set_key(input.key)
                                .set_metadata(input.metadata)
                                .set_server_side_encryption(input.server_side_encryption)
                                .set_storage_class(input.storage_class)
                                .set_website_redirect_location(input.website_redirect_location)
                                .set_sse_customer_algorithm(input.sse_customer_algorithm)
                                .set_sse_customer_key(input.sse_customer_key)
                                .set_sse_customer_key_md5(input.sse_customer_key_md5)
                                .set_ssekms_key_id(input.ssekms_key_id)
                                .set_ssekms_encryption_context(input.ssekms_encryption_context)
                                .set_bucket_key_enabled(input.bucket_key_enabled)
                                .set_request_payer(input.request_payer)
                                .set_tagging(input.tagging)
                                .set_object_lock_mode(input.object_lock_mode)
                                .set_object_lock_retain_until_date(input.object_lock_retain_until_date)
                                .set_object_lock_legal_hold_status(input.object_lock_legal_hold_status)
                                .set_expected_bucket_owner(input.expected_bucket_owner)
                                .send()
                                .await;

                            let _ = reply.send(map_health(&mut health, q));
                        }
                        RemoteMessage::DeleteObject { input, reply } => {
                            info!("Delete object...");
                            let q = client.delete_object()
                                .bucket(target.s3.bucket.clone())
                                .set_key(input.key)
                                .set_mfa(input.mfa)
                                .set_version_id(input.version_id)
                                .set_request_payer(input.request_payer)
                                .set_bypass_governance_retention(input.bypass_governance_retention)
                                .set_expected_bucket_owner(input.expected_bucket_owner)
                                .send()
                                .await;

                            let _ = reply.send(map_health(&mut health, q));
                        }
                        RemoteMessage::DeleteObjects { input, reply } => {
                            info!("Delete objects...");
                            let q = client.delete_objects()
                                .bucket(target.s3.bucket.clone())
                                .set_delete(input.delete)
                                .set_mfa(input.mfa)
                                .set_request_payer(input.request_payer)
                                .set_bypass_governance_retention(input.bypass_governance_retention)
                                .set_expected_bucket_owner(input.expected_bucket_owner)
                                .set_checksum_algorithm(input.checksum_algorithm)
                                .send()
                                .await;

                            let _ = reply.send(map_health(&mut health, q));
                        }
                        RemoteMessage::HeadObject { input, reply } => {
                            info!("Head object...");
                            let q = client.head_object()
                                .bucket(target.s3.bucket.clone())
                                .set_if_match(input.if_match)
                                .set_if_modified_since(input.if_modified_since)
                                .set_if_unmodified_since(input.if_unmodified_since)
                                .set_key(input.key)
                                .set_range(input.range)
                                .set_response_cache_control(input.response_cache_control)
                                .set_response_content_disposition(input.response_content_disposition)
                                .set_response_content_encoding(input.response_content_encoding)
                                .set_response_content_language(input.response_content_language)
                                .set_response_content_type(input.response_content_type)
                                .set_response_expires(input.response_expires)
                                .set_version_id(input.version_id)
                                .set_sse_customer_algorithm(input.sse_customer_algorithm)
                                .set_sse_customer_key(input.sse_customer_key)
                                .set_sse_customer_key_md5(input.sse_customer_key_md5)
                                .set_request_payer(input.request_payer)
                                .set_part_number(input.part_number)
                                .set_expected_bucket_owner(input.expected_bucket_owner)
                                .set_checksum_mode(input.checksum_mode)
                                .send()
                                .await;

                            let _ = reply.send(map_health(&mut health, q));
                        }
                        RemoteMessage::CreateMultiPartUpload { input, reply } => {
                            info!("Create multipart upload...");

                            let q = client.create_multipart_upload()
                                .bucket(target.s3.bucket.clone())
                                .set_acl(input.acl)
                                .set_cache_control(input.cache_control)
                                .set_content_disposition(input.content_disposition)
                                .set_content_encoding(input.content_encoding)
                                .set_content_language(input.content_language)
                                .set_content_type(input.content_type)
                                .set_expires(input.expires)
                                .set_grant_full_control(input.grant_full_control)
                                .set_grant_read(input.grant_read)
                                .set_grant_read_acp(input.grant_read_acp)
                                .set_grant_write_acp(input.grant_write_acp)
                                .set_key(input.key)
                                .set_metadata(input.metadata)
                                .set_server_side_encryption(input.server_side_encryption)
                                .set_storage_class(input.storage_class)
                                .set_website_redirect_location(input.website_redirect_location)
                                .set_sse_customer_algorithm(input.sse_customer_algorithm)
                                .set_sse_customer_key(input.sse_customer_key)
                                .set_sse_customer_key_md5(input.sse_customer_key_md5)
                                .set_ssekms_key_id(input.ssekms_key_id)
                                .set_ssekms_encryption_context(input.ssekms_encryption_context)
                                .set_bucket_key_enabled(input.bucket_key_enabled)
                                .set_request_payer(input.request_payer)
                                .set_tagging(input.tagging)
                                .set_object_lock_mode(input.object_lock_mode)
                                .set_object_lock_retain_until_date(input.object_lock_retain_until_date)
                                .set_object_lock_legal_hold_status(input.object_lock_legal_hold_status)
                                .set_expected_bucket_owner(input.expected_bucket_owner)
                                .set_checksum_algorithm(input.checksum_algorithm)
                                .send()
                                .await;

                            let _ = reply.send(map_health(&mut health, q));
                        }
                        RemoteMessage::CompleteMultiPartUpload { input, reply } => {
                            info!("Complete multipart upload...");

                            let q = client.complete_multipart_upload()
                                .bucket(target.s3.bucket.clone())
                                .set_key(input.key)
                                .set_multipart_upload(input.multipart_upload)
                                .set_upload_id(input.upload_id)
                                .set_checksum_crc32(input.checksum_crc32)
                                .set_checksum_crc32_c(input.checksum_crc32_c)
                                .set_checksum_sha1(input.checksum_sha1)
                                .set_checksum_sha256(input.checksum_sha256)
                                .set_request_payer(input.request_payer)
                                .set_expected_bucket_owner(input.expected_bucket_owner)
                                .set_sse_customer_algorithm(input.sse_customer_algorithm)
                                .set_sse_customer_key(input.sse_customer_key)
                                .set_sse_customer_key_md5(input.sse_customer_key_md5)
                                .send()
                                .await;

                            let _ = reply.send(map_health(&mut health, q));
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
fn map_health<T, E1: Debug, E2: Debug>(
    self_health: &mut Option<bool>,
    query: Result<T, SdkError<E1, E2>>,
) -> Option<Result<T, ServiceError<E1, E2>>> {
    // ServiceErrorはリモートが返してきたエラーなので, DOWNとは判断しない
    let (query, health) = match query {
        Ok(t) => (Some(Ok(t)), true),
        Err(SdkError::ServiceError(e)) => (Some(Err(e)), true),
        Err(e) => {
            warn!("remote unhealthy response: {} {:?}", e, e);
            (None, false)
        }
    };
    if *self_health != Some(health) {
        if health {
            info!("remote is UP")
        } else {
            warn!("remote is DOWN")
        }
        *self_health = Some(health);
    }
    query
}
