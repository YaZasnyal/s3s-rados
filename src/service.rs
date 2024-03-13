use futures::StreamExt;
use hyper::body;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use tokio::io::AsyncWriteExt;
use tracing::{debug_span, Instrument};
use uuid::Uuid;

use crate::blob_store::BlobStore;
use crate::ceph_store::RadosBlobStore;
use crate::meta_store::{Blob, ListOptions, ListResult, MetaStore};
use crate::pg_database::PostgresDatabase;

#[derive(Debug)]
pub struct RadosStore {
    db: Box<dyn MetaStore>,
    blob: Box<dyn BlobStore>,
}

impl RadosStore {
    pub async fn new() -> Self {
        Self {
            db: Box::new(PostgresDatabase::new().await),
            blob: Box::new(RadosBlobStore::new().await),
        }
    }
}

#[async_trait::async_trait]
impl S3 for RadosStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let Some(creds) = &req.credentials else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        };

        // TODO: get real user name
        let user = self.db.get_user_by_access_key(&creds.access_key).await?;
        let _res = self.db.create_bucket(&user.id, &req.input.bucket).await?;

        let output = CreateBucketOutput::default(); // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        if req.credentials.is_none() {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }

        let Some(_bucket) = self.db.get_bucket_metadata(&req.input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };
        // TODO: check ownership

        self.db.delete_bucket(&req.input.bucket).await?;
        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        // check acl
        // check bucket lock
        //self.db.

        self.db
            .delete_object_metadata(&req.input.bucket, &req.input.key, &req.input.version_id)
            .await?;

        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: false, // TODO: handle versioned
            request_charged: None,
            version_id: None,
        }))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        //TODO: validate user
        let Some((object, blob)) = self.db.load_object_metadata(&req.input.bucket, &req.input.key, &None).await? else {
            return Err(s3_error!(NoSuchKey, "Key not found"));
        };

        let Some(blob) = blob else {
            return Err(s3_error!(NoSuchKey, "Versioning is not supported yet"));
        };

        let bytes = self.blob.get_reader(&blob.id.to_string(), 0, blob.size as u64).await?;
        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(bytes)),
            content_length: blob.size,
            content_range: None,
            last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
                object.last_modified.date(),
                object.last_modified.time(),
                time::UtcOffset::UTC,
            ))),
            metadata: None, // TODO: handle metadata
            e_tag: Some(blob.etag),
            // checksum_crc32: None,
            // checksum_crc32c: None,
            // checksum_sha1: None,
            // checksum_sha256: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
        //Err(s3_error!(NotImplemented, "GetObject is not implemented yet"))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let Some(_bucket) = self.db.get_bucket_metadata(&req.input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };
        // check ownership
        Ok(S3Response::new(HeadBucketOutput {}))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let Some((object, blob)) = self.db.load_object_metadata(&req.input.bucket, &req.input.key, &None).await? else {
            return Err(s3_error!(NoSuchKey, "Key not found"));
        };

        let Some(blob) = blob else {
            return Err(s3_error!(NoSuchKey, "Versioning is not supported yet"));
        };
        // check ownership

        let output = HeadObjectOutput {
            content_length: blob.size,
            content_type: None,
            last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
                object.last_modified.date(),
                object.last_modified.time(),
                time::UtcOffset::UTC,
            ))),
            metadata: None,
            e_tag: Some(blob.etag),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "info")]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        let Some(creds) = &req.credentials else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        };

        // get user
        let user = self.db.get_user_by_access_key(&creds.access_key).await?;

        // get buckets
        let buckets = self
            .db
            .list_buckets_by_user(&user.id)
            .await?
            .into_iter()
            .map(|b| s3s::dto::Bucket {
                creation_date: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
                    b.creation_date.date(),
                    b.creation_date.time(),
                    time::UtcOffset::UTC,
                ))),
                name: Some(b.name),
            })
            .collect();
        let output = s3s::dto::ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(s3s::dto::Owner {
                id: Some(user.id),
                display_name: Some(user.name),
            }),
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        let list_result = self
            .db
            .list_objects(ListOptions {
                bucket: &req.input.bucket,
                prefix: Some(""),
                delim: "/",
                marker: None,
                max_keys: 1000,
                with_versions: false,
                version_marker: None,
            })
            .await?;

        let ListResult {
            objects,
            common_prefixes,
            marker,
            version_marker,
        } = list_result;

        let objects = objects
            .into_iter()
            .map(|(o, b)| s3s::dto::Object {
                checksum_algorithm: None,
                e_tag: if let Some(b) = &b { Some(b.etag.clone()) } else { None },
                key: Some(o.oid),
                last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
                    o.last_modified.date(),
                    o.last_modified.time(),
                    time::UtcOffset::UTC,
                ))),
                owner: None,
                restore_status: None,
                size: if let Some(b) = &b { b.size } else { 0 },
                storage_class: None,
            })
            .collect();

        let common_prefixes = common_prefixes
            .into_iter()
            .map(|p| s3s::dto::CommonPrefix { prefix: Some(p) })
            .collect();

        let output = s3s::dto::ListObjectsOutput {
            common_prefixes: Some(common_prefixes),
            contents: Some(objects),
            delimiter: None,
            encoding_type: None,
            is_truncated: false, // TODO handle truncated
            marker: None,
            max_keys: 1000,
            name: None,
            next_marker: version_marker,
            prefix: None,
            request_charged: None,
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        let list_result = self
            .db
            .list_objects(ListOptions {
                bucket: &req.input.bucket,
                prefix: Some(""),
                delim: "/",
                marker: None,
                max_keys: 1000,
                with_versions: false,
                version_marker: None,
            })
            .await?;

        let ListResult {
            objects,
            common_prefixes,
            marker,
            version_marker,
        } = list_result;

        let objects: Vec<s3s::dto::Object> = objects
            .into_iter()
            .map(|(o, b)| s3s::dto::Object {
                checksum_algorithm: None,
                e_tag: if let Some(b) = &b { Some(b.etag.clone()) } else { None },
                key: Some(o.oid),
                last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
                    o.last_modified.date(),
                    o.last_modified.time(),
                    time::UtcOffset::UTC,
                ))),
                owner: None,
                restore_status: None,
                size: if let Some(b) = &b { b.size } else { 0 },
                storage_class: None,
            })
            .collect();

        let common_prefixes = common_prefixes
            .into_iter()
            .map(|p| s3s::dto::CommonPrefix { prefix: Some(p) })
            .collect();

        let output = s3s::dto::ListObjectsV2Output {
            common_prefixes: Some(common_prefixes),
            key_count: objects.len() as i32,
            contents: Some(objects),
            delimiter: None,
            encoding_type: None,
            is_truncated: false,
            max_keys: 1000,
            name: None,
            prefix: None,
            request_charged: None,
            continuation_token: None,
            next_continuation_token: todo!(),
            start_after: None,
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD"].contains(&storage_class.as_str());
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let Some(bucket_md) = self.db.get_bucket_metadata(&input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };
        // validate acl
        if req.credentials.is_none() {
            tracing::info!("request is unatharized");
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            metadata,
            tagging,
            content_length,
            content_md5,
            ..
        } = input;
        // let Some(content_md5) = content_md5 else  {
        //     return Err(s3_error!(InvalidArgument, "No MD5 hash provided"));
        // };
        let Some(content_length) = content_length else {
            return Err(s3_error!(InvalidArgument, "content_length not provided"));
            // TODO: should not be an error
        };

        tracing::info!("Request validation is done");
        let Some(mut body) = body else { return Err(s3_error!(IncompleteBody)) };

        let mut new_blob = Blob {
            id: Uuid::new_v4(),
            size: content_length,
            parts: None,
            part_size: None,
            upload_timestamp: crate::meta_store::Timestamp::MIN,
            etag: String::default(), // TODO get md5-hash as AWS does
        };
        self.db.write_temp_blob(&new_blob).await?;
        tracing::info!(blob=?new_blob, "temp blob has been written");

        let res: Result<crate::meta_store::Object, s3s::S3Error> = {
            // open rados file
            let mut writer = try_!(self.blob.get_writer(&new_blob.id.to_string()).await);
            let mut md5_hash = <Md5 as Digest>::new();
            while let Some(chunk) = body.next().instrument(debug_span!("read_user_input")).await {
                //let chunk = try_!(chunk);
                let Ok(chunk) = chunk else {
                    break;
                    // return error
                };
                md5_hash.update(chunk.as_ref());

                try_!(writer.write_all(&chunk).instrument(debug_span!("rados_write_chunk")).await);
                // calk checksum
                // write to the rados
            }
            try_!(writer.flush().instrument(debug_span!("rados_flush_remainig")).await);

            // validate checksums
            new_blob.etag = hex(md5_hash.finalize());

            let object = crate::meta_store::Object {
                bucket_name: bucket,
                oid: key,
                version_id: None,
                last_modified: crate::meta_store::Timestamp::MIN,
                blob_id: Some(new_blob.id),
                metadata: metadata,
            };
            try_!(self.db.write_object_metadata_with_blob(&bucket_md, &object, &new_blob).await);
            Ok(object)
        };

        let Ok(object) = res else {
            // TODO: delete from rados
            self.db.clean_temp_blob(&new_blob).await;
            return Err(s3_error!(InternalError, "Unable to put object"));
        };

        let output = PutObjectOutput {
            e_tag: Some(new_blob.etag),
            checksum_crc32: None,
            checksum_crc32c: None,
            checksum_sha1: None,
            checksum_sha256: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input.as_ref(), hex_simd::AsciiCase::Lower)
}
