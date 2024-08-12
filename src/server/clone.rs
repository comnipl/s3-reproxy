use std::collections::HashMap;

use aws_sdk_s3::operation::put_object::PutObjectInput;
use aws_sdk_s3::operation::upload_part::UploadPartInput;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ObjectCannedAcl, ObjectLockLegalHoldStatus, ObjectLockMode, RequestPayer,
    ServerSideEncryption, StorageClass,
};
use aws_smithy_types::DateTime;

use super::stream::{ByteStreamMultiplier, FirstByteSignal};

pub struct UploadPartInputMultiplier {
    body: ByteStreamMultiplier,
    bucket: Option<String>,
    content_length: Option<i64>,
    content_md5: Option<String>,
    checksum_algorithm: Option<ChecksumAlgorithm>,
    checksum_crc32: Option<String>,
    checksum_crc32_c: Option<String>,
    checksum_sha1: Option<String>,
    checksum_sha256: Option<String>,
    key: Option<String>,
    part_number: Option<i32>,
    upload_id: Option<String>,
    sse_customer_algorithm: Option<String>,
    sse_customer_key: Option<String>,
    sse_customer_key_md5: Option<String>,
    request_payer: Option<RequestPayer>,
    expected_bucket_owner: Option<String>,
}

pub struct PutObjectInputMultiplier {
    body: ByteStreamMultiplier,
    acl: Option<ObjectCannedAcl>,
    bucket: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_length: Option<i64>,
    content_md5: Option<String>,
    content_type: Option<String>,
    checksum_algorithm: Option<ChecksumAlgorithm>,
    checksum_crc32: Option<String>,
    checksum_crc32_c: Option<String>,
    checksum_sha1: Option<String>,
    checksum_sha256: Option<String>,
    expires: Option<DateTime>,
    grant_full_control: Option<String>,
    grant_read: Option<String>,
    grant_read_acp: Option<String>,
    grant_write_acp: Option<String>,
    key: Option<String>,
    metadata: Option<HashMap<String, String>>,
    server_side_encryption: Option<ServerSideEncryption>,
    storage_class: Option<StorageClass>,
    website_redirect_location: Option<String>,
    sse_customer_algorithm: Option<String>,
    sse_customer_key: Option<String>,
    sse_customer_key_md5: Option<String>,
    ssekms_key_id: Option<String>,
    ssekms_encryption_context: Option<String>,
    bucket_key_enabled: Option<bool>,
    request_payer: Option<RequestPayer>,
    tagging: Option<String>,
    object_lock_mode: Option<ObjectLockMode>,
    object_lock_retain_until_date: Option<DateTime>,
    object_lock_legal_hold_status: Option<ObjectLockLegalHoldStatus>,
    expected_bucket_owner: Option<String>,
}

impl UploadPartInputMultiplier {
    pub fn from_input(input: UploadPartInput) -> (Self, FirstByteSignal) {
        let (body, signal) = ByteStreamMultiplier::from_bytestream(input.body);
        let multiplier = Self {
            body,
            bucket: input.bucket,
            content_length: input.content_length,
            content_md5: input.content_md5,
            checksum_algorithm: input.checksum_algorithm,
            checksum_crc32: input.checksum_crc32,
            checksum_crc32_c: input.checksum_crc32_c,
            checksum_sha1: input.checksum_sha1,
            checksum_sha256: input.checksum_sha256,
            key: input.key,
            part_number: input.part_number,
            upload_id: input.upload_id,
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
            request_payer: input.request_payer,
            expected_bucket_owner: input.expected_bucket_owner,
        };
        (multiplier, signal)
    }

    pub async fn input(&self) -> Option<UploadPartInput> {
        let body = self.body.subscribe_stream().await?;

        Some(
            UploadPartInput::builder()
                .body(body)
                .set_bucket(self.bucket.clone())
                .set_content_length(self.content_length.clone())
                .set_content_md5(self.content_md5.clone())
                .set_checksum_algorithm(self.checksum_algorithm.clone())
                .set_checksum_crc32(self.checksum_crc32.clone())
                .set_checksum_crc32_c(self.checksum_crc32_c.clone())
                .set_checksum_sha1(self.checksum_sha1.clone())
                .set_checksum_sha256(self.checksum_sha256.clone())
                .set_key(self.key.clone())
                .set_part_number(self.part_number.clone())
                .set_upload_id(self.upload_id.clone())
                .set_sse_customer_algorithm(self.sse_customer_algorithm.clone())
                .set_sse_customer_key(self.sse_customer_key.clone())
                .set_sse_customer_key_md5(self.sse_customer_key_md5.clone())
                .set_request_payer(self.request_payer.clone())
                .set_expected_bucket_owner(self.expected_bucket_owner.clone())
                .build()
                .unwrap(),
        )
    }

    pub fn close(&mut self) {
        self.body.close();
    }
}

impl PutObjectInputMultiplier {
    pub fn from_input(input: PutObjectInput) -> (Self, FirstByteSignal) {
        let (body, signal) = ByteStreamMultiplier::from_bytestream(input.body);
        let multiplier = Self {
            body,
            acl: input.acl,
            bucket: input.bucket,
            cache_control: input.cache_control,
            content_disposition: input.content_disposition,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            content_length: input.content_length,
            content_md5: input.content_md5,
            content_type: input.content_type,
            checksum_algorithm: input.checksum_algorithm,
            checksum_crc32: input.checksum_crc32,
            checksum_crc32_c: input.checksum_crc32_c,
            checksum_sha1: input.checksum_sha1,
            checksum_sha256: input.checksum_sha256,
            expires: input.expires,
            grant_full_control: input.grant_full_control,
            grant_read: input.grant_read,
            grant_read_acp: input.grant_read_acp,
            grant_write_acp: input.grant_write_acp,
            key: input.key,
            metadata: input.metadata,
            server_side_encryption: input.server_side_encryption,
            storage_class: input.storage_class,
            website_redirect_location: input.website_redirect_location,
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key,
            sse_customer_key_md5: input.sse_customer_key_md5,
            ssekms_key_id: input.ssekms_key_id,
            ssekms_encryption_context: input.ssekms_encryption_context,
            bucket_key_enabled: input.bucket_key_enabled,
            request_payer: input.request_payer,
            tagging: input.tagging,
            object_lock_mode: input.object_lock_mode,
            object_lock_retain_until_date: input.object_lock_retain_until_date,
            object_lock_legal_hold_status: input.object_lock_legal_hold_status,
            expected_bucket_owner: input.expected_bucket_owner,
        };
        (multiplier, signal)
    }

    pub async fn input(&self) -> Option<PutObjectInput> {
        let body = self.body.subscribe_stream().await?;

        Some(
            PutObjectInput::builder()
                .set_acl(self.acl.clone())
                .body(body)
                .set_bucket(self.bucket.clone())
                .set_cache_control(self.cache_control.clone())
                .set_content_disposition(self.content_disposition.clone())
                .set_content_encoding(self.content_encoding.clone())
                .set_content_language(self.content_language.clone())
                .set_content_length(self.content_length)
                .set_content_md5(self.content_md5.clone())
                .set_content_type(self.content_type.clone())
                .set_checksum_algorithm(self.checksum_algorithm.clone())
                .set_checksum_crc32(self.checksum_crc32.clone())
                .set_checksum_crc32_c(self.checksum_crc32_c.clone())
                .set_checksum_sha1(self.checksum_sha1.clone())
                .set_checksum_sha256(self.checksum_sha256.clone())
                .set_expires(self.expires)
                .set_grant_full_control(self.grant_full_control.clone())
                .set_grant_read(self.grant_read.clone())
                .set_grant_read_acp(self.grant_read_acp.clone())
                .set_grant_write_acp(self.grant_write_acp.clone())
                .set_key(self.key.clone())
                .set_metadata(self.metadata.clone())
                .set_server_side_encryption(self.server_side_encryption.clone())
                .set_storage_class(self.storage_class.clone())
                .set_website_redirect_location(self.website_redirect_location.clone())
                .set_sse_customer_algorithm(self.sse_customer_algorithm.clone())
                .set_sse_customer_key(self.sse_customer_key.clone())
                .set_sse_customer_key_md5(self.sse_customer_key_md5.clone())
                .set_ssekms_key_id(self.ssekms_key_id.clone())
                .set_ssekms_encryption_context(self.ssekms_encryption_context.clone())
                .set_bucket_key_enabled(self.bucket_key_enabled)
                .set_request_payer(self.request_payer.clone())
                .set_tagging(self.tagging.clone())
                .set_object_lock_mode(self.object_lock_mode.clone())
                .set_object_lock_retain_until_date(self.object_lock_retain_until_date)
                .set_object_lock_legal_hold_status(self.object_lock_legal_hold_status.clone())
                .set_expected_bucket_owner(self.expected_bucket_owner.clone())
                .build()
                .unwrap(),
        )
    }

    pub fn close(&mut self) {
        self.body.close();
    }
}
