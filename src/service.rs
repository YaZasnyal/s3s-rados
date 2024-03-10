use futures::StreamExt;
use hyper::body;
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use tokio::io::AsyncWriteExt;
use tracing::{debug_span, Instrument};
use uuid::Uuid;

use crate::blob_store::BlobStore;
use crate::ceph_store::RadosBlobStore;
use crate::meta_store::{Blob, MetaStore};
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

        let del_res = self
            .db
            .delete_object_metadata(&req.input.bucket, &req.input.key, &req.input.version_id)
            .await;
        //try_!(del_res);

        Err(s3_error!(NotImplemented, "DeleteObject is not implemented yet"))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_object(&self, _req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        Err(s3_error!(NotImplemented, "GetObject is not implemented yet"))
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
    async fn head_object(&self, _req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        Err(s3_error!(NotImplemented, "HeadObject is not implemented yet"))
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_objects(&self, _req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        Err(s3_error!(NotImplemented, "ListObjects is not implemented yet"))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_objects_v2(&self, _req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        Err(s3_error!(NotImplemented, "ListObjectsV2 is not implemented yet"))
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
        //validate acl
        if req.credentials.is_none() {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }

        let PutObjectInput {
            body,
            bucket,
            key,
            metadata,
            content_length,
            ..
        } = input;

        let Some(mut body) = body else { return Err(s3_error!(IncompleteBody)) };

        let new_blob = Blob {
            id: Uuid::new_v4(),
            parts: None,
            part_size: None,
            etag: String::new(), // TODO get md5-hash as AWS does
        };
        //let tx = self.db.write_blob(&new_blob).await?;
        let res: Result<(), s3s::S3Error> = {
            // open rados file
            let mut writer = try_!(self.blob.get_writer("test").await);

            while let Some(chunk) = body.next().instrument(debug_span!("read_user_input")).await {
                //let chunk = try_!(chunk);
                let Ok(chunk) = chunk else {
                    break;
                    // return error
                };

                try_!(writer.write_all(&chunk).instrument(debug_span!("rados_write_chunk")).await);
                // calk checksum
                // write to the rados
            }
            try_!(writer.flush().instrument(debug_span!("rados_flush_remainig")).await);

            // write object
            // validate checksums

            // write object meta to the datastore

            // commit blob (with new etag) and object in a single transaction
            //try_!(self.db.write_object_metadata_with_blob(&bucket_md, object, &new_blob).await);
            Ok(())
        };

        let Ok(res) = res else {
            //tx.rollback();
            return Err(s3_error!(NotImplemented, "PutObject is not implemented yet"));
        };
        //tx.commit();

        //Ok(res?)
        Err(s3_error!(NotImplemented, "PutObject is not implemented yet"))
    }
}
