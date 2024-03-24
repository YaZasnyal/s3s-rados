use s3s::{self};
use uuid::Uuid;

pub type AccountId = s3s::dto::AccountId;
pub type Timestamp = time::PrimitiveDateTime;

#[derive(Debug)]
pub struct User {
    pub id: AccountId,
    pub name: String,
    pub email: String,
    pub creation_date: Timestamp,
}

#[derive(Debug)]
pub struct Key {
    pub access_key: String,
    pub secret_key: String,
    pub account: AccountId,
    // key policy (read, write)
}

#[derive(Debug)]
pub struct Bucket {
    pub name: String,
    pub owner: AccountId,
    pub creation_date: Timestamp,
    pub location: BlobLocation,
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

#[derive(Debug)]
pub struct Object {
    pub bucket_name: String,
    pub oid: String,
    // pub version_id: Option<String>,
    pub last_modified: Timestamp,

    /// Unique indentifier of the blob (acts as etag)
    pub blob_id: Option<Uuid>,
    // pub metadata: Option<s3s::dto::Metadata>,
    // retain_untill
    // legal_hold
}

#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(type_name = "blob_location")]
pub struct BlobLocation {
    pub region: String,
    pub backend: String,
}

#[derive(Debug, Clone)]
pub struct Blob {
    pub id: Uuid,
    // total size of the blob
    pub size: i64,
    // number of parts for multipart blobs
    // pub parts: Option<i32>,
    // size of a single part (excluding the last one)
    // pub part_size: Option<i64>,
    // pub upload_timestamp: Timestamp,
    //storage_class: String,
    pub placement: BlobLocation,
    // MD5 or MD5 of all the parts
    //
    // TODO: select better database type
    pub etag: String,
    // pub checksum_algorithm: Option<s3s::dto::ChecksumAlgorithm>,
    // pub checksum: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MultipartUpload {
    pub bucket: String,
    pub oid: String,
    pub upload_id: String,
    pub blob_id: Uuid,
    pub uploaded_at: Timestamp,
    pub location: BlobLocation,
}

#[derive(Debug)]
pub struct ListResult {
    pub objects: Vec<(Object, Option<Blob>)>,
    pub common_prefixes: Vec<String>,
    pub marker: Option<String>,
    pub version_marker: Option<String>,
}