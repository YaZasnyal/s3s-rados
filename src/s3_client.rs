use std::fmt::Debug;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;

use crate::meta_store::{self};
use s3s::{dto::CreateBucketInput, S3Request, S3Response, S3Result, S3};
use s3s_aws::Proxy;

pub struct S3Client {
    cfg: std::sync::Arc<crate::config::Settings>,
    proxy: Proxy,
}

impl Debug for S3Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Client").finish()
    }
}

impl S3Client {
    pub async fn new(cfg: std::sync::Arc<crate::config::Settings>) -> Self {
        let key_id = cfg.storage.access_key.clone();
        let secret_key = cfg.storage.secret_key.to_owned();

        let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");

        let url = format!(
            "{}://{}:{}",
            if cfg.storage.insecure { "http" } else { "https" },
            cfg.storage.host,
            cfg.storage.port
        );
        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::v2023_11_09())
            .endpoint_url(url)
            .credentials_provider(cred)
            .region(Region::new("auto"))
            .force_path_style(true) // apply bucketname as path param instead of pre-domain
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);
        let buckets = client.list_buckets().send().await.unwrap();
        for bucket in buckets.buckets.unwrap() {
            println!("{:?}", bucket.name);
        }

        let proxy = s3s_aws::Proxy::from(client);

        Self { cfg, proxy }
    }
}

impl S3Client {
    pub fn default_region(&self) -> String {
        "default".into()
    }

    pub fn get_location(&self, region: &str, bucket: &str) -> meta_store::BlobLocation {
        // TODO: check if auto

        let new_name = if let Some(x) = &self.cfg.storage.bucket {
            x.clone()
        } else {
            format!("{}-{}", bucket, uuid::Uuid::new_v4().to_string())
        };
        meta_store::BlobLocation {
            region: region.to_owned(),
            backend: new_name,
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn create_bucket(&self, _name: &str, region: &meta_store::BlobLocation) -> Result<(), s3s::S3Error> {
        if self.cfg.storage.bucket.is_some() {
            return Ok(());
        }

        tracing::info!("creating bucket in the backing store");
        self.proxy
            .create_bucket(S3Request::new(try_!(CreateBucketInput::builder()
                .bucket(region.backend.clone())
                //TODO: set location
                .build())))
            .await?;
        tracing::info!("created bucket in the backing store");
        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn delete_bucket(&self, _name: &str, region: &meta_store::BlobLocation) -> Result<(), s3s::S3Error> {
        if self.cfg.storage.bucket.is_some() {
            return Ok(());
        }

        self.proxy
            .delete_bucket(S3Request::new(try_!(s3s::dto::builders::DeleteBucketInputBuilder::default()
                .bucket(region.backend.clone())
                //TODO: set location
                .build())))
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn put_object(
        &self,
        req: s3s::dto::builders::PutObjectInputBuilder,
        region: &meta_store::BlobLocation,
    ) -> S3Result<S3Response<s3s::dto::PutObjectOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        tracing::info!(?req, "put_object: sending upstream request");
        let res = self.proxy.put_object(s3s::S3Request::new(req)).await?;
        tracing::info!(res = ?res.output, "put_object: upstream response");
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn get_object(
        &self,
        req: s3s::dto::builders::GetObjectInputBuilder,
        region: &meta_store::BlobLocation,
    ) -> S3Result<S3Response<s3s::dto::GetObjectOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        tracing::info!(?req, "get_object: sending upstream request");
        let res = self.proxy.get_object(s3s::S3Request::new(req)).await?;
        tracing::info!(res = ?res.output, "get_object: upstream response");
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn head_object(
        &self,
        req: s3s::dto::builders::HeadObjectInputBuilder,
        region: &meta_store::BlobLocation,
    ) -> S3Result<S3Response<s3s::dto::HeadObjectOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        tracing::info!(?req, "head_object: sending upstream request");
        let res = self.proxy.head_object(s3s::S3Request::new(req)).await?;
        tracing::info!(res = ?res.output, "head_object: upstream response");
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn delete_object(
        &self,
        req: s3s::dto::builders::DeleteObjectInputBuilder,
        region: &meta_store::BlobLocation,
    ) -> S3Result<S3Response<s3s::dto::DeleteObjectOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        tracing::info!(?req, "delete_object: sending upstream request");
        let res = self.proxy.delete_object(s3s::S3Request::new(req)).await?;
        tracing::info!(res = ?res.output, "delete_object: upstream response");
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn create_multipart_upload(
        &self,
        req: s3s::dto::builders::CreateMultipartUploadInputBuilder,
        region: &meta_store::BlobLocation,
    ) -> S3Result<S3Response<s3s::dto::CreateMultipartUploadOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        tracing::info!(?req, "create_multipart_upload: sending upstream request");
        let res = self.proxy.create_multipart_upload(s3s::S3Request::new(req)).await?;
        tracing::info!(res = ?res.output, "create_multipart_upload: upstream response");
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn complete_multipart_upload(
        &self,
        req: s3s::dto::builders::CompleteMultipartUploadInputBuilder,
        region: &meta_store::BlobLocation,
    ) -> S3Result<S3Response<s3s::dto::CompleteMultipartUploadOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        tracing::info!(?req, "complete_multipart_upload: sending upstream request");
        let res = self.proxy.complete_multipart_upload(s3s::S3Request::new(req)).await?;
        tracing::info!(res = ?res.output, "complete_multipart_upload: upstream response");
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn upload_part(
        &self,
        req: s3s::dto::builders::UploadPartInputBuilder,
        region: &meta_store::BlobLocation,
    ) -> S3Result<S3Response<s3s::dto::UploadPartOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        tracing::info!(?req, "upload_part: sending upstream request");
        let res = self.proxy.upload_part(s3s::S3Request::new(req)).await?;
        tracing::info!(res = ?res.output, "upload_part: sending upstream request");
        Ok(res)
    }
}

// impl Deref for S3Client {
//     type Target = dyn S3;

//     fn deref(&self) -> &Self::Target {
//         &self.proxy
//     }
// }
