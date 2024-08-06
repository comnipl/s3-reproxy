pub mod clone;
pub mod remote;
pub mod stream;
use crate::db::ListObjectTokens;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::RequestId;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::ServiceError;
use futures::StreamExt;
use itertools::{Either, Itertools};
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use s3s::dto::{
    Bucket, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput,
    GetBucketLocationInput, GetBucketLocationOutput, GetObjectInput, GetObjectOutput,
    HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListBucketsInput,
    ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output, PutObjectInput, PutObjectOutput,
};
use s3s::{s3_error, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, S3};
use s3s_aws::conv::AwsConversion;
use tokio::sync::oneshot;
use tracing::{error, info, instrument, warn};

use crate::db::MongoDB;

use self::clone::PutObjectInputMultiplier;
use self::remote::S3Remote;

pub struct S3Reproxy {
    pub bucket: String,
    pub remotes: Arc<Vec<S3Remote>>,
    pub db: Arc<MongoDB>,
}

#[inline(always)]
fn convert_sdk_err<E: ProvideErrorMetadata>(sdk: ServiceError<E, HttpResponse>) -> S3Error {
    let mut s3s = S3Error::new(S3ErrorCode::InternalError);
    let meta = sdk.err().meta();
    if let Some(s) = meta
        .code()
        .and_then(|s| S3ErrorCode::from_bytes(s.as_bytes()))
    {
        s3s.set_code(s);
    }
    if let Some(m) = meta.message() {
        s3s.set_message(m.to_owned());
    }
    if let Some(i) = meta.request_id() {
        s3s.set_request_id(i);
    }
    s3s.set_status_code(hyper::StatusCode::from_u16(sdk.raw().status().as_u16()).unwrap());
    s3s
}

#[async_trait]
impl S3 for S3Reproxy {
    #[instrument(skip_all)]
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        info!("(intercepted) {}", self.bucket);
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(vec![Bucket {
                creation_date: None,
                name: Some(self.bucket.clone()),
            }]),
            owner: None,
        }))
    }

    #[instrument(skip_all, fields(bucket = req.input.bucket))]
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        if req.input.bucket != self.bucket {
            warn!("(intercepted) not found");
            return Err(s3_error!(NoSuchBucket));
        }

        let output = GetBucketLocationOutput::default();
        info!("(intercepted) ok");
        Ok(S3Response::new(output))
    }

    #[instrument(skip_all, fields(bucket = req.input.bucket))]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        if req.input.bucket != self.bucket {
            warn!("(intercepted) not found");
            return Err(s3_error!(NoSuchBucket));
        }

        let output = HeadBucketOutput::default();
        info!("(intercepted) ok");
        Ok(S3Response::new(output))
    }

    #[instrument(skip_all, name = "s3s/put_object")]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = PutObjectInput::try_into_aws(req.input)?;
        let input_multiplier = PutObjectInputMultiplier::from_input(input);
        let remotes = futures::stream::iter(self.remotes.iter())
            .map(|remote| {
                let input = input_multiplier.input();
                async move { (remote, input.await.unwrap()) }
            })
            .boxed()
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;
        let results = futures::stream::iter(remotes.into_iter())
            .map(|(remote, input)| async move {
                let Some(result) = (try {
                    let (tx, rx) = oneshot::channel();
                    remote
                        .tx
                        .send(remote::RemoteMessage::PutObject { input, reply: tx })
                        .await
                        .ok()?;
                    rx.await.ok()??
                }) else {
                    warn!("remote({:?}) request failed. skipping", remote.name);
                    return None;
                };
                Some((remote.name.clone(), result))
            })
            .boxed()
            .buffer_unordered(8)
            .filter_map(|e| async { e })
            .collect::<Vec<_>>()
            .await;

        let output = output_remote_inconsistent(results)?;

        Ok(S3Response::new(PutObjectOutput::try_from_aws(output)?))
    }

    #[instrument(skip_all, name = "s3s/delete_objects")]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = DeleteObjectsInput::try_into_aws(req.input)?;
        let results = futures::stream::iter(self.remotes.iter())
            .map(|remote| async {
                let Some(result) = (try {
                    let (tx, rx) = oneshot::channel();
                    remote
                        .tx
                        .send(remote::RemoteMessage::DeleteObjects {
                            input: input.clone(),
                            reply: tx,
                        })
                        .await
                        .ok()?;
                    rx.await.ok()??
                }) else {
                    warn!("remote({:?}) request failed. skipping", remote.name);
                    return None;
                };
                Some((remote.name.clone(), result))
            })
            .boxed()
            .buffer_unordered(4)
            .filter_map(|e| async { e })
            .collect::<Vec<_>>()
            .await;

        let output = output_remote_inconsistent(results)?;

        Ok(S3Response::new(DeleteObjectsOutput::try_from_aws(output)?))
    }

    #[instrument(skip_all, name = "s3s/delete_object")]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = DeleteObjectInput::try_into_aws(req.input)?;
        let results = futures::stream::iter(self.remotes.iter())
            .map(|remote| async {
                let Some(result) = (try {
                    let (tx, rx) = oneshot::channel();
                    remote
                        .tx
                        .send(remote::RemoteMessage::DeleteObject {
                            input: input.clone(),
                            reply: tx,
                        })
                        .await
                        .ok()?;
                    rx.await.ok()??
                }) else {
                    warn!("remote({:?}) request failed. skipping", remote.name);
                    return None;
                };
                Some((remote.name.clone(), result))
            })
            .boxed()
            .buffer_unordered(4)
            .filter_map(|e| async { e })
            .collect::<Vec<_>>()
            .await;

        let output = output_remote_inconsistent(results)?;

        Ok(S3Response::new(DeleteObjectOutput::try_from_aws(output)?))
    }

    #[instrument(skip_all, name = "s3s/get_object")]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let read_remotes = self.remotes.iter().sorted_by(|a, b| {
            b.read_request
                .cmp(&a.read_request)
                .then_with(|| b.priority.cmp(&a.priority))
        });

        let input = GetObjectInput::try_into_aws(req.input)?;

        let Some((result, remote)) = ('request: {
            for remote in read_remotes {
                let Some(output) = (try {
                    let (tx, rx) = oneshot::channel();
                    remote
                        .tx
                        .send(remote::RemoteMessage::GetObject {
                            input: input.clone(),
                            reply: tx,
                        })
                        .await
                        .ok()?;
                    rx.await.ok()??
                }) else {
                    warn!("remote({:?}) request failed. skipping", remote.name);
                    continue;
                };
                break 'request Some((output, remote.name.clone()));
            }
            None
        }) else {
            warn!("no remotes available!");
            return Err(s3_error!(InternalError));
        };

        info!("ok (remote: {})", remote);

        let output = result
            .map_err(convert_sdk_err)
            .and_then(GetObjectOutput::try_from_aws)?;

        Ok(S3Response::new(output))
    }

    #[instrument(skip_all, name = "s3s/head_object")]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let read_remotes = self.remotes.iter().sorted_by(|a, b| {
            b.read_request
                .cmp(&a.read_request)
                .then_with(|| b.priority.cmp(&a.priority))
        });

        let input = HeadObjectInput::try_into_aws(req.input)?;

        let Some((result, remote)) = ('request: {
            for remote in read_remotes {
                let Some(output) = (try {
                    let (tx, rx) = oneshot::channel();
                    remote
                        .tx
                        .send(remote::RemoteMessage::HeadObject {
                            input: input.clone(),
                            reply: tx,
                        })
                        .await
                        .ok()?;
                    rx.await.ok()??
                }) else {
                    warn!("remote({:?}) request failed. skipping", remote.name);
                    continue;
                };
                break 'request Some((output, remote.name.clone()));
            }
            None
        }) else {
            warn!("no remotes available!");
            return Err(s3_error!(InternalError));
        };

        info!("ok (remote: {})", remote);

        let output = result
            .map_err(convert_sdk_err)
            .and_then(HeadObjectOutput::try_from_aws)?;

        Ok(S3Response::new(output))
    }

    #[instrument(skip_all, fields(token = &req.input.continuation_token), name = "s3s/list_objects_v2")]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        info!("{:?}", &req);

        let start_after = match req.input.continuation_token.clone() {
            Some(continuation_token) => {
                let list = self
                    .db
                    .list_object_tokens
                    .find_one_and_update(
                        doc! {
                            "_id": ObjectId::parse_str(continuation_token)
                                .map_err(|e| {
                                    warn!("(intercepted) invalid continuation token: {:?}", e);
                                    S3Error::new(s3s::S3ErrorCode::InvalidToken)
                                })?,
                        },
                        doc! {
                            "$set": {
                                "consumed_at": mongodb::bson::DateTime::now(),
                            },
                        },
                    )
                    .await
                    .map_err(|e| {
                        error!("mongodb error: {:?}", e);
                        S3Error::new(s3s::S3ErrorCode::InternalError)
                    })?
                    .ok_or_else(|| {
                        warn!("(intercepted) continuation token not found.");
                        S3Error::new(s3s::S3ErrorCode::InvalidToken)
                    })?;
                Some(list.start_after)
            }
            None => None,
        };

        let read_remotes = self.remotes.iter().sorted_by(|a, b| {
            b.read_request
                .cmp(&a.read_request)
                .then_with(|| b.priority.cmp(&a.priority))
        });

        let start_after = start_after.or(req.input.start_after.clone());

        let Some((result, remote)) = ('request: {
            for remote in read_remotes {
                let Some(output) = (try {
                    let (tx, rx) = oneshot::channel();
                    remote
                        .tx
                        .send(remote::RemoteMessage::ListObjects {
                            prefix: req.input.prefix.clone(),
                            delimiter: req.input.delimiter.clone(),
                            max_keys: req.input.max_keys,
                            start_after: start_after.clone(),
                            reply: tx,
                        })
                        .await
                        .ok()?;
                    rx.await.ok()??
                }) else {
                    warn!("remote({:?}) request failed. skipping", remote.name);
                    continue;
                };
                break 'request Some((output, remote.name.clone()));
            }
            None
        }) else {
            warn!("no remotes available!");
            return Err(s3_error!(InternalError));
        };

        info!("ok (remote: {})", remote);

        let mut output = result
            .map_err(convert_sdk_err)
            .and_then(ListObjectsV2Output::try_from_aws)?;

        output.continuation_token = req.input.continuation_token;
        output.next_continuation_token = match output.next_continuation_token {
            Some(_) => 'm: {
                let Some(last) = output
                    .contents
                    .as_ref()
                    .and_then(|e| e.last())
                    .and_then(|e| e.key.clone())
                else {
                    break 'm None;
                };

                let list = self
                    .db
                    .list_object_tokens
                    .insert_one(ListObjectTokens {
                        start_after: last,
                        created_at: mongodb::bson::DateTime::now(),
                        consumed_at: None,
                    })
                    .await
                    .map_err(|e| {
                        error!("mongodb error: {:?}", e);
                        S3Error::new(s3s::S3ErrorCode::InternalError)
                    })?;

                Some(list.inserted_id.as_object_id().unwrap().to_hex())
            }
            None => None,
        };

        Ok(S3Response::new(output))
    }
}

#[allow(clippy::type_complexity)]
fn output_remote_inconsistent<T, E: Debug + ProvideErrorMetadata>(
    results: Vec<(String, Result<T, ServiceError<E, HttpResponse>>)>,
) -> Result<T, S3Error> {
    let (successes, failures): (Vec<_>, Vec<_>) =
        results
            .into_iter()
            .partition_map(|(remote, result)| match result {
                Ok(output) => Either::Left((remote, output)),
                Err(e) => Either::Right((remote, e)),
            });

    if failures.is_empty() {
        let (remote, reply) = successes.into_iter().next().map_or_else(
            || {
                warn!("no remotes available!");
                Err(S3Error::new(S3ErrorCode::InternalError))
            },
            Result::Ok,
        )?;
        info!("all remote ok (replied remote: {})", remote);
        Ok(reply)
    } else if successes.is_empty() {
        let (remote, err) = failures.into_iter().next().unwrap();
        info!("all remote failed (replied remote: {})", remote);
        Err(convert_sdk_err(err))?
    } else {
        error!("some remote failed (inconsisted).");
        for (remote, _) in successes.iter() {
            info!("remote({:?}) ok", remote);
        }
        for (remote, err) in failures {
            error!("remote({:?}) failed: {:?}", remote, err);
        }
        let (remote, reply) = successes.into_iter().next().unwrap();
        info!("some remote ok (replied remote: {})", remote);
        Ok(reply)
    }
}
