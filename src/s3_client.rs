use std::{borrow::Borrow, fmt::Debug, ops::Deref};

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use aws_sdk_s3::config::Credentials;
use hyper::Uri;

use crate::meta_store::{self, Bucket};
use s3s::{dto::CreateBucketInput, S3Request, S3Response, S3Result, S3};
use s3s_aws::Proxy;

pub struct S3Client {
    proxy: Proxy,
}

impl Debug for S3Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Client").finish()
    }
}

impl S3Client {
    pub async fn new() -> Self {
        let key_id = "qwe".to_owned(); //dotenv_codegen::dotenv!("MINIO_ACCESS_KEY_ID").to_string();
        let secret_key = "asd".to_owned(); //dotenv_codegen::dotenv!("MINIO_SECRET_ACCESS_KEY").to_string();

        let cred = Credentials::new(key_id, secret_key, None, None, "loaded-from-custom-env");

        let url = "http://localhost:8015".to_owned(); //dotenv_codegen::dotenv!("MINIO_URL");
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

        Self { proxy }
    }
}

impl S3Client {
    pub fn default_region(&self) -> String {
        "default".into()
    }

    pub fn get_location(&self, region: &str, bucket: &str) -> meta_store::BlobLocation {
        // TODO: check if auto

        meta_store::BlobLocation {
            region: region.to_owned(),
            backend: bucket.to_owned(),
        }
    }

    pub async fn create_bucket(&self, name: &str, _region: &meta_store::BlobLocation) -> Result<(), s3s::S3Error> {
        //try_!(self.client.create_bucket().set_bucket(Some(name.to_owned())).send().await);
        self.proxy
            .create_bucket(S3Request::new(try_!(CreateBucketInput::builder()
                .bucket(name.to_owned())
                //TODO: set location
                .build())))
            .await?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn put_object(&self, req: s3s::dto::builders::PutObjectInputBuilder, region: &meta_store::BlobLocation) -> S3Result<S3Response<s3s::dto::PutObjectOutput>> {
        let req = try_!(req.bucket(region.backend.clone()).build());
        self.proxy.put_object(s3s::S3Request::new(req)).await
    }
}

// impl Deref for S3Client {
//     type Target = dyn S3;

//     fn deref(&self) -> &Self::Target {
//         &self.proxy
//     }
// }
