pub mod remote;

use std::sync::Arc;

use async_trait::async_trait;
use s3s::dto::{
    Bucket, GetBucketLocationInput, GetBucketLocationOutput, HeadBucketInput, HeadBucketOutput,
    ListBucketsInput, ListBucketsOutput,
};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use tracing::{info, instrument, warn};

use self::remote::S3Remote;

#[derive(Debug)]
pub struct S3Reproxy {
    pub bucket: String,
    pub remotes: Arc<Vec<S3Remote>>,
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
}
