use s3s::{dto::CreateBucketInput, S3Request};

use crate::meta_store::{self, Object};

#[async_trait::async_trait]
pub trait BlobStore: Send + Sync + std::fmt::Debug + 'static {
    async fn create_bucket_location(&self, req: S3Request<CreateBucketInput>) -> meta_store::BlobLocation;

    // async fn put_object(s3s::dto::PutObjectInput, placement) -> Result(s3s::dto::PutObjectOutput, s3s::S3Error);
    // create_multipart_ipload -> Result()
    // complete_multipart_upload

    async fn get_writer(&self, key: &str) -> Result<core::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>, s3s::S3Error>;
    //async fn write_blob(&self, object: &Object, blob: &blob, );
    async fn get_reader(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<core::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, s3s::S3Error>> + Send + Sync>>, s3s::S3Error>;
}

pub trait BlobStoreManager {
    // async fn put_object(input: s3s::dto::PutObjectInput, placement) -> Result(s3s::dto::PutObjectOutput, s3s::S3Error);
    // create_multipart_ipload -> Result()
    // complete_multipart_upload
}