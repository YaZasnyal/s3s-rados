#[async_trait::async_trait]
pub trait BlobStore: Send + Sync + std::fmt::Debug + 'static {
    async fn get_writer(&self, key: &str) -> Result<core::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>, s3s::S3Error>;
    async fn get_reader(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<core::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, s3s::S3Error>> + Send + Sync>>, s3s::S3Error>;
}
