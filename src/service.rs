use futures::StreamExt;
use hyper::body;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use tokio::io::AsyncWriteExt;
use tracing::{debug_span, Instrument};
use uuid::Uuid;

// use crate::blob_store::BlobStore;
// use crate::ceph_store::RadosBlobStore;
use crate::meta_store::{Blob, BlobLocation, ListOptions, ListResult, MetaStore};
use crate::pg_database::PostgresDatabase;
use crate::s3_client::S3Client;

#[derive(Debug)]
pub struct RadosStore {
    db: Box<PostgresDatabase>,
    blob: Box<S3Client>,
}

impl RadosStore {
    pub async fn new() -> Self {
        Self {
            db: Box::new(PostgresDatabase::new().await),
            blob: Box::new(S3Client::new().await),
        }
    }
}

#[async_trait::async_trait]
impl S3 for RadosStore {
    #[tracing::instrument(level = "info", skip(self))]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let Some(creds) = &req.credentials else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        };

        let user = self.db.get_user_by_access_key(&creds.access_key).await?;
        if let Some(bucket) = self.db.get_bucket(&req.input.bucket).await? {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::BucketAlreadyExists));
        }

        let location = (|| {
            let Some(conf) = req.input.create_bucket_configuration else {
                return self.blob.get_location(&self.blob.default_region(), &req.input.bucket);
            };
            let Some(region) = conf.location_constraint else {
                return self.blob.get_location(&self.blob.default_region(), &req.input.bucket);
            };
            self.blob.get_location(&region.as_str().to_owned(), &req.input.bucket)
        })();
        tracing::info!(?user, ?location, "creation bucket in location");

        self.db.create_bucket_temp(&req.input.bucket, &location).await?;
        if let Err(e) = self.blob.create_bucket(&req.input.bucket, &location).await {
            self.db.delete_bucket_temp(&req.input.bucket).await?;
            return Err(e);
        }
        if let Err(e) = self.db.commit_bucket(&req.input.bucket, &user, &location).await {
            // TODO: delete bucket from backing store
            self.db.delete_bucket_temp(&req.input.bucket).await?;
            return Err(e);
        }

        let output = CreateBucketOutput {
            location: Some(location.region),
        }; // TODO: handle other fields
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        todo!()
        // if req.credentials.is_none() {
        //     return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        // }

        // let Some(_bucket) = self.db.get_bucket_metadata(&req.input.bucket).await? else {
        //     return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        // };
        // // TODO: check ownership
        // // TODO: check empty

        // self.db.delete_bucket(&req.input.bucket).await?;
        // Ok(S3Response::new(DeleteBucketOutput {}))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        todo!()
        // // check acl
        // // check bucket lock
        // //self.db.

        // self.db
        //     .delete_object_metadata(&req.input.bucket, &req.input.key, &req.input.version_id)
        //     .await?;

        // Ok(S3Response::new(DeleteObjectOutput {
        //     delete_marker: Some(false), // TODO: handle versioned
        //     request_charged: None,
        //     version_id: None,
        // }))
    }

    async fn delete_objects(&self, _req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        Err(s3_error!(NotImplemented, "DeleteObjects is not implemented yet"))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        todo!()
        // //TODO: validate user
        // //TODO: conditions
        // let Some((object, blob)) = self.db.load_object_metadata(&req.input.bucket, &req.input.key, &None).await? else {
        //     return Err(s3_error!(NoSuchKey, "Key not found"));
        // };

        // let Some(blob) = blob else {
        //     return Err(s3_error!(NoSuchKey, "Versioning is not supported yet"));
        // };

        // let bytes = self.blob.get_reader(&blob.id.to_string(), 0, blob.size as u64).await?;
        // let output = GetObjectOutput {
        //     body: Some(StreamingBlob::wrap(bytes)),
        //     content_length: Some(blob.size),
        //     content_range: None,
        //     last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
        //         object.last_modified.date(),
        //         object.last_modified.time(),
        //         time::UtcOffset::UTC,
        //     ))),
        //     metadata: None, // TODO: handle metadata
        //     e_tag: Some(blob.etag),
        //     // checksum_crc32: None,
        //     // checksum_crc32c: None,
        //     // checksum_sha1: None,
        //     // checksum_sha256: None,
        //     ..Default::default()
        // };
        // Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        todo!()
        // let Some(_bucket) = self.db.get_bucket_metadata(&req.input.bucket).await? else {
        //     return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        // };
        // // check ownership
        // Ok(S3Response::new(HeadBucketOutput {
        //     access_point_alias: todo!(),
        //     bucket_location_name: todo!(),
        //     bucket_location_type: todo!(),
        //     bucket_region: todo!(),
        // }))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        todo!()
        // let Some((object, blob)) = self.db.load_object_metadata(&req.input.bucket, &req.input.key, &None).await? else {
        //     return Err(s3_error!(NoSuchKey, "Key not found"));
        // };

        // let Some(blob) = blob else {
        //     return Err(s3_error!(NoSuchKey, "Versioning is not supported yet"));
        // };
        // // check ownership

        // let output = HeadObjectOutput {
        //     content_length: Some(blob.size),
        //     content_type: None,
        //     last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
        //         object.last_modified.date(),
        //         object.last_modified.time(),
        //         time::UtcOffset::UTC,
        //     ))),
        //     metadata: None,
        //     e_tag: Some(blob.etag),
        //     ..Default::default()
        // };
        // Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "info")]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        todo!()
        // let Some(creds) = &req.credentials else {
        //     return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        // };

        // // get user
        // let user = self.db.get_user_by_access_key(&creds.access_key).await?;

        // // get buckets
        // let buckets = self
        //     .db
        //     .list_buckets_by_user(&user.id)
        //     .await?
        //     .into_iter()
        //     .map(|b| s3s::dto::Bucket {
        //         creation_date: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
        //             b.creation_date.date(),
        //             b.creation_date.time(),
        //             time::UtcOffset::UTC,
        //         ))),
        //         name: Some(b.name),
        //     })
        //     .collect();
        // let output = s3s::dto::ListBucketsOutput {
        //     buckets: Some(buckets),
        //     owner: Some(s3s::dto::Owner {
        //         id: Some(user.id),
        //         display_name: Some(user.name),
        //     }),
        // };
        // Ok(S3Response::new(output))
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        todo!()
        // let mut v2_resp = self
        //     .list_objects_v2(S3Request::new(ListObjectsV2Input {
        //         bucket: req.input.bucket,
        //         continuation_token: req.input.key_marker.clone(),
        //         delimiter: req.input.delimiter,
        //         encoding_type: req.input.encoding_type,
        //         expected_bucket_owner: req.input.expected_bucket_owner,
        //         fetch_owner: Some(false),
        //         max_keys: req.input.max_keys,
        //         optional_object_attributes: req.input.optional_object_attributes,
        //         prefix: req.input.prefix,
        //         request_payer: req.input.request_payer,
        //         start_after: req.input.key_marker.clone(),
        //     }))
        //     .await?;

        // let mut contents = None;
        // std::mem::swap(&mut v2_resp.output.contents, &mut contents);
        // let objects = contents
        //     .unwrap_or(Vec::default())
        //     .into_iter()
        //     .map(|o| {
        //         Some(s3s::dto::ObjectVersion {
        //             checksum_algorithm: o.checksum_algorithm,
        //             e_tag: o.e_tag,
        //             is_latest: Some(true), // TODO: handle versioning
        //             key: o.key,
        //             last_modified: o.last_modified,
        //             owner: o.owner,
        //             restore_status: o.restore_status,
        //             size: o.size,
        //             storage_class: None,
        //             version_id: Some("qwe".to_owned()), // TODO: put real version
        //         })
        //     })
        //     .collect();

        // let output = v2_resp.map_output(|v2| s3s::dto::ListObjectVersionsOutput {
        //     common_prefixes: v2.common_prefixes,
        //     delete_markers: None, // TODO: handle versioning
        //     delimiter: v2.delimiter,
        //     encoding_type: v2.encoding_type,
        //     is_truncated: v2.is_truncated,
        //     key_marker: req.input.key_marker,
        //     max_keys: v2.max_keys,
        //     name: v2.name,
        //     next_key_marker: v2.continuation_token,
        //     next_version_id_marker: None,
        //     prefix: v2.prefix,
        //     request_charged: v2.request_charged,
        //     version_id_marker: None,
        //     versions: objects,
        // });
        // Ok(output)
    }

    #[tracing::instrument(level = "debug")]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        todo!()
        // let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        // Ok(v2_resp.map_output(|v2| ListObjectsOutput {
        //     contents: v2.contents,
        //     common_prefixes: v2.common_prefixes,
        //     delimiter: v2.delimiter,
        //     encoding_type: v2.encoding_type,
        //     name: v2.name,
        //     prefix: v2.prefix,
        //     max_keys: v2.max_keys,
        //     is_truncated: v2.is_truncated,
        //     next_marker: v2.continuation_token,
        //     ..Default::default()
        // }))
    }

    #[tracing::instrument(level = "debug")]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        todo!()
        // let list_result = self
        //     .db
        //     .list_objects(ListOptions {
        //         bucket: &req.input.bucket,
        //         prefix: &req.input.prefix,
        //         delim: &req.input.delimiter.as_ref().map_or("/", |v| &v),
        //         marker: &req.input.start_after,
        //         max_keys: 1000,
        //         with_versions: false,
        //         version_marker: None,
        //     })
        //     .await?;

        // let ListResult {
        //     objects,
        //     common_prefixes,
        //     marker,
        //     version_marker,
        // } = list_result;

        // let objects: Vec<s3s::dto::Object> = objects
        //     .into_iter()
        //     .map(|(o, b)| s3s::dto::Object {
        //         checksum_algorithm: None,
        //         e_tag: if let Some(b) = &b { Some(b.etag.clone()) } else { None },
        //         key: Some(o.oid),
        //         last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
        //             o.last_modified.date(),
        //             o.last_modified.time(),
        //             time::UtcOffset::UTC,
        //         ))),
        //         owner: None,
        //         restore_status: None,
        //         size: if let Some(b) = &b { Some(b.size) } else { Some(0) },
        //         storage_class: None,
        //     })
        //     .collect();

        // let common_prefixes = common_prefixes
        //     .into_iter()
        //     .map(|p| s3s::dto::CommonPrefix { prefix: Some(p) })
        //     .collect();

        // let output = s3s::dto::ListObjectsV2Output {
        //     common_prefixes: Some(common_prefixes),
        //     key_count: Some(objects.len() as i32),
        //     contents: Some(objects),
        //     delimiter: req.input.delimiter,
        //     encoding_type: None,
        //     is_truncated: Some(marker.is_some()),
        //     max_keys: Some(1000),
        //     name: Some(req.input.bucket),
        //     prefix: req.input.prefix,
        //     request_charged: None,
        //     continuation_token: req.input.continuation_token,
        //     next_continuation_token: marker,
        //     start_after: None,
        // };

        // Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD"].contains(&storage_class.as_str()); // todo handle storage tiers
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        let Some(bucket) = self.db.get_bucket(&input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };
        if req.credentials.is_none() {
            tracing::info!("request is unatharized");
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }

        let new_blob = Uuid::new_v4();
        self.db.create_blob_temp(&new_blob).await?;

        let PutObjectInput {
            body,
            // bucket,
            // key,
            metadata,
            tagging,
            content_length,
            content_md5,
            // acl,
            // bucket_key_enabled,
            cache_control,
            checksum_algorithm,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            // expected_bucket_owner,
            // expires,
            // grant_full_control,
            // grant_read,
            // grant_read_acp,
            // grant_write_acp,
            // object_lock_legal_hold_status,
            // object_lock_mode,
            // object_lock_retain_until_date,
            // request_payer,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_encryption_context,
            ssekms_key_id,
            server_side_encryption,
            // storage_class,
            // website_redirect_location,
            ..
        } = input;

        // put addiotional metadata for redundancy

        let put_request = s3s::dto::PutObjectInput::builder()
            .body(body)
            .key(new_blob.to_string())
            .metadata(metadata)
            .tagging(tagging)
            .content_length(content_length)
            .content_md5(content_md5)
            .cache_control(cache_control)
            .checksum_algorithm(checksum_algorithm)
            .checksum_crc32(checksum_crc32)
            .checksum_crc32c(checksum_crc32c)
            .checksum_sha1(checksum_sha1)
            .checksum_sha256(checksum_sha256)
            .content_disposition(content_disposition)
            .content_encoding(content_encoding)
            .content_language(content_language)
            .content_type(content_type)
            .sse_customer_algorithm(sse_customer_algorithm)
            .sse_customer_key(sse_customer_key)
            .sse_customer_key_md5(sse_customer_key_md5)
            .ssekms_encryption_context(ssekms_encryption_context)
            .ssekms_key_id(ssekms_key_id)
            .server_side_encryption(server_side_encryption);

        let put_response = match self.blob.put_object(put_request, &bucket.location).await {
            Ok(x) => x,
            Err(e) => {
                self.db.delete_blob_temp(&new_blob).await?;
                return Err(e);
            }
        };

        // create database record

        Ok(put_response)

        // let Some(bucket_md) = self.db.get_bucket_metadata(&input.bucket).await? else {
        //     return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        // };
        // // validate acl
        // if req.credentials.is_none() {
        //     tracing::info!("request is unatharized");
        //     return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        // }

        // let PutObjectInput {
        //     body,
        //     bucket,
        //     key,
        //     metadata,
        //     tagging,
        //     content_length,
        //     content_md5,
        //     ..
        // } = input;
        // // let Some(content_md5) = content_md5 else  {
        // //     return Err(s3_error!(InvalidArgument, "No MD5 hash provided"));
        // // };
        // let Some(content_length) = content_length else {
        //     return Err(s3_error!(InvalidArgument, "content_length not provided"));
        //     // TODO: should not be an error
        // };

        // tracing::info!("Request validation is done");
        // let Some(mut body) = body else { return Err(s3_error!(IncompleteBody)) };

        // let mut new_blob = Blob {
        //     id: Uuid::new_v4(),
        //     size: content_length,
        //     parts: None,
        //     part_size: None,
        //     upload_timestamp: crate::meta_store::Timestamp::MIN,
        //     etag: String::default(), // TODO get md5-hash as AWS does
        // };
        // self.db.write_temp_blob(&new_blob).await?;
        // tracing::info!(blob=?new_blob, "temp blob has been written");

        // let res: Result<crate::meta_store::Object, s3s::S3Error> = {
        //     // open rados file
        //     let mut writer = try_!(self.blob.get_writer(&new_blob.id.to_string()).await);
        //     let mut md5_hash = <Md5 as Digest>::new();
        //     while let Some(chunk) = body.next().instrument(debug_span!("read_user_input")).await {
        //         //let chunk = try_!(chunk);
        //         let Ok(chunk) = chunk else {
        //             break;
        //             // return error
        //         };
        //         md5_hash.update(chunk.as_ref());

        //         try_!(writer.write_all(&chunk).instrument(debug_span!("rados_write_chunk")).await);
        //         // calk checksum
        //         // write to the rados
        //     }
        //     try_!(writer.flush().instrument(debug_span!("rados_flush_remainig")).await);

        //     // validate checksums
        //     new_blob.etag = hex(md5_hash.finalize());

        //     let object = crate::meta_store::Object {
        //         bucket_name: bucket,
        //         oid: key,
        //         version_id: None,
        //         last_modified: crate::meta_store::Timestamp::MIN,
        //         blob_id: Some(new_blob.id),
        //         metadata: metadata,
        //     };
        //     try_!(self.db.write_object_metadata_with_blob(&bucket_md, &object, &new_blob).await);
        //     Ok(object)
        // };

        // let Ok(object) = res else {
        //     // TODO: delete from rados
        //     self.db.clean_temp_blob(&new_blob).await;
        //     return Err(s3_error!(InternalError, "Unable to put object"));
        // };

        // let output = PutObjectOutput {
        //     e_tag: Some(new_blob.etag),
        //     checksum_crc32: None,
        //     checksum_crc32c: None,
        //     checksum_sha1: None,
        //     checksum_sha256: None,
        //     ..Default::default()
        // };
        // Ok(S3Response::new(output))
    }
}

fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input.as_ref(), hex_simd::AsciiCase::Lower)
}
