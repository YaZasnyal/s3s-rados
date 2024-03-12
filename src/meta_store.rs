use s3s::{
    self,
    dto::{ChecksumSHA1, ChecksumSHA256},
    S3Error,
};
use std::result::Result;
use uuid::Uuid;

#[derive(Debug)]
pub enum TransactionError {
    Conflict,
}

/// Transaction MUST be rolled back on Drop if commit has not been called.
///
///
///
/// This can be done asynchronously.
#[async_trait::async_trait]
pub trait Transaction: Send {
    fn chain(&mut self, next: Box<dyn Transaction>);

    /// transactions are commited in the reverse order
    async fn commit(self: Box<Self>) -> Result<(), TransactionError>;
    async fn rollback(self: Box<Self>) -> Result<(), TransactionError>;
}

#[derive(Debug)]
pub enum MetaStoreError {
    NoSuchObject,
    AlreadyExists,
}

#[async_trait::async_trait]
pub trait MetaStore: Send + Sync + std::fmt::Debug + 'static {
    /// Write complete object metadata and commit temporary blob
    /// 
    /// TODO: Handle versioned
    async fn write_object_metadata_with_blob(
        &self,
        bucket: &Bucket,
        object: &Object,
        blob: &Blob,
    ) -> Result<(), s3s::S3Error>;


    async fn write_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        metadata: &s3s::dto::Metadata,
    ) -> Result<(), MetaStoreError>;

    /// load object metadata from the metadata storage
    ///
    /// MUST NOT be cached
    async fn load_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        version: &Option<s3s::dto::ObjectVersionId>,
    ) -> Result<Option<(Object, Option<Blob>)>, s3s::S3Error>;

    /// This function does not delete object from the store. It is done by GC
    ///
    /// TODO: Handle versioned
    async fn delete_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        version: &Option<s3s::dto::ObjectVersionId>,
    ) -> Result<(), s3s::S3Error>;

    /// Writes blob metadata to the temp storage. This is a first stage of the two-phase-commit
    /// 2PC allow to clean data from the storage if an error occures.
    async fn write_temp_blob(&self, blob: &Blob) -> Result<(), s3s::S3Error>;

    /// Does not return any error because GC should handle failures
    async fn clean_temp_blob(&self, blob: &Blob);
    /// Remove commited blob asynchronously
    async fn add_blob_gc(&self, blob: &Blob) -> Result<User, s3s::S3Error>;


    // list objects (with prefix)

    async fn create_bucket(&self, owner: &str, bucket: &str) -> Result<Bucket, S3Error>;
    async fn delete_bucket(&self, bucket: &str) -> Result<(), S3Error>;
    /// Should be cached
    async fn get_bucket_metadata(&self, bucket: &str) -> Result<Option<Bucket>, s3s::S3Error>;
    async fn list_buckets_by_user(&self, user: &str) -> Result<Vec<Bucket>, s3s::S3Error>;

    // May be cached
    // user metadata
    async fn get_user_by_access_key(&self, key: &str) -> Result<User, s3s::S3Error>;

    // config log
    async fn get_blob_gc(&self) -> anyhow::Result<Vec<Uuid>>;
}

pub type AccountId = s3s::dto::AccountId;
pub type Timestamp = time::PrimitiveDateTime;

pub struct User {
    pub id: AccountId,
    pub name: String,
    pub email: String,
}

pub struct Key {
    pub access_key: String,
    pub secret_key: String,
    pub account: AccountId,
    // key policy (read, write)
}

pub struct Bucket {
    pub name: String,
    pub owner: AccountId,
    pub creation_date: Timestamp,
    //versioning: bool,
    // lc policy
    // notification policy
    // retention_policy
}

// users
//  -> id: String (Unique)
//  -> email: String (Unique)

// keys
//  -> access_key: String
//  -> secret_key: String
//  -> user: String (Indexed, ON DELETE CASCADE)

// buckets
//  -> name: String
//  -> owner: users->id (ON DELETE RESTRICT)

// temp_blobs (not attached to any version yet)
//  -> id: Uuid
//  -> created: Timestamp
//  -> metadata: String (json)

// multipart_uploads (not finished multipart uploads)
//  -> id: Uuid

// object metadata
//
//
// partition by bucket
// versions:
//  -> bucket: buckets->name (ON DELETE RESTRICT)
//  -> oid: String,
//  -> version_id: Option<Uuid>,
//  -> last_modified: s3s::dto::Timestamp
//  -> metadata: s3s::dto::Metadata,
//  -> retain_untill Option<Timestamp> // placed by the user or by the bucket option
//  -> retention_mode:
//  -> legal_hold: bool
//  -> etag: String,
//  -> checksums (to speed up head operation)
//  -> blob: Uuid (blobs->id ON DELETE RESTRICT) Indexed
//
// +blobcache (LRU, redis)
// blobs:
//  -> id: Uuid
//  -> checksums
//  -> parts: u32,
//  -> part_size: u32,
//  -> storage_class

pub struct Object {
    pub bucket_name: String,
    pub oid: String,
    pub version_id: Option<String>,
    pub last_modified: Timestamp,

    /// Unique indentifier of the blob (acts as etag)
    pub blob_id: Option<Uuid>,

    pub metadata: Option<s3s::dto::Metadata>,
    
    // retain_untill
    // legal_hold
}

#[derive(Debug, Clone)]
pub struct Blob {
    pub id: Uuid,
    /// total size of the blob
    pub size: i64,
    /// number of parts for multipart blobs
    pub parts: Option<i32>,
    /// size of a single part (excluding the last one)
    pub part_size: Option<i64>,
    pub upload_timestamp: Timestamp,
    //storage_class: String,
    /// MD5 or MD5 of all the parts
    /// 
    /// TODO: select better database type
    pub etag: String,
    // pub checksum_algorithm: Option<String>,
    // pub checksum: Option<String>,
}
