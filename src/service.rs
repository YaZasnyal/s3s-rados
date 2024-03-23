use futures::StreamExt;
use hyper::body;
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use time::PrimitiveDateTime;
use tokio::io::AsyncWriteExt;
use tracing::{debug_span, Instrument};
use uuid::Uuid;

// use crate::blob_store::BlobStore;
// use crate::ceph_store::RadosBlobStore;
use crate::meta_store::{Blob, BlobLocation, ListOptions, ListResult, MetaStore, MultipartUpload, Object};
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
    async fn abort_multipart_upload(
        &self,
        _req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        Err(s3_error!(NotImplemented, "AbortMultipartUpload is not implemented yet"))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        if req.credentials.is_none() {
            tracing::info!("request is unatharized");
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }
        let Some(bucket) = self.db.get_bucket(&input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };

        let Some(upload) = self.db.get_multipart(&input.bucket, &input.key, &input.upload_id).await? else {
            return Err(s3_error!(NoSuchUpload, "UploadPart with such ID not found"));
        };

        let upstream_request = s3s::dto::builders::CompleteMultipartUploadInputBuilder::default()
            .checksum_crc32(input.checksum_crc32)
            .checksum_crc32c(input.checksum_crc32c)
            .checksum_sha1(input.checksum_sha1)
            .checksum_sha256(input.checksum_sha256)
            .key(upload.blob_id.to_string())
            .multipart_upload(input.multipart_upload)
            .upload_id(input.upload_id);
        let upstream_response = self
            .blob
            .complete_multipart_upload(upstream_request, &bucket.location)
            .await?
            .output;

        self.db
            .complete_multipart(
                &Object {
                    bucket_name: upload.bucket.clone(),
                    oid: upload.oid.clone(),
                    last_modified: time::PrimitiveDateTime::MIN,
                    blob_id: Some(upload.blob_id.clone()),
                },
                &Blob {
                    id: upload.blob_id.clone(),
                    size: 0, // TODO: handle by the backing store or move completely to the proxy
                    placement: upload.location.clone(),
                    etag: upstream_response
                        .e_tag
                        .as_ref()
                        .expect("No etag returned from the backing store")
                        .clone(),
                },
                &upload,
            )
            .await?;

        let response = s3s::dto::CompleteMultipartUploadOutput {
            bucket: Some(upload.bucket),
            checksum_crc32: upstream_response.checksum_crc32,
            checksum_crc32c: upstream_response.checksum_crc32c,
            checksum_sha1: upstream_response.checksum_sha1,
            checksum_sha256: upstream_response.checksum_sha256,
            e_tag: upstream_response.e_tag,
            key: Some(upload.oid),
            ..Default::default()
        };
        Ok(S3Response::new(response))
    }

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

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        if let Some(ref storage_class) = input.storage_class {
            let is_valid = ["STANDARD"].contains(&storage_class.as_str()); // todo handle storage tiers
            if !is_valid {
                return Err(s3_error!(InvalidStorageClass));
            }
        }

        if req.credentials.is_none() {
            tracing::info!("request is unatharized");
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }
        let Some(bucket) = self.db.get_bucket(&input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };

        let new_blob = Uuid::new_v4();

        let CreateMultipartUploadInput {
            // acl,
            // bucket: bucket,
            // bucket_key_enabled,
            cache_control,
            checksum_algorithm,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            // expected_bucket_owner,
            // expires,
            key,
            metadata,
            // object_lock_legal_hold_status, // TODO: handle locking
            // object_lock_mode,
            // object_lock_retain_until_date,
            // storage_class,
            tagging,
            // website_redirect_location,
            ..
        } = input;

        let upstream_request = s3s::dto::builders::CreateMultipartUploadInputBuilder::default()
            .cache_control(cache_control)
            .checksum_algorithm(checksum_algorithm)
            .content_disposition(content_disposition)
            .content_encoding(content_encoding)
            .content_language(content_language)
            .content_type(content_type)
            .key(new_blob.to_string())
            .metadata(metadata)
            .tagging(tagging);

        let CreateMultipartUploadOutput { upload_id, .. } = self
            .blob
            .create_multipart_upload(upstream_request, &bucket.location)
            .await?
            .output;

        self.db
            .create_multipart(&MultipartUpload {
                bucket: bucket.name.clone(),
                oid: key.clone(),
                upload_id: upload_id
                    .as_ref()
                    .expect("no upload id returned from the upstream")
                    .to_owned(),
                blob_id: new_blob.clone(),
                uploaded_at: time::PrimitiveDateTime::MIN,
                location: bucket.location.clone(),
            })
            .await?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket.name.clone()),
            key: Some(key),
            upload_id,
            ..Default::default()
        };
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
        // //TODO: validate user
        // //TODO: conditions
        let Some((object, blob)) = self.db.get_object(&req.input).await? else {
            return Err(s3_error!(NoSuchKey, "Key not found"));
        };

        let Some(blob) = blob else {
            return Err(s3_error!(NoSuchKey, "Versioning is not supported yet"));
        };

        let GetObjectInput {
            // bucket,
            checksum_mode,
            // expected_bucket_owner,
            // if_match,
            // if_modified_since,
            // if_none_match,
            // if_unmodified_since,
            // key,
            part_number,
            range,
            // request_payer,
            response_cache_control,
            response_content_disposition,
            response_content_encoding,
            response_content_language,
            response_content_type,
            response_expires,
            // sse_customer_algorithm,
            // sse_customer_key,
            // sse_customer_key_md5,
            // version_id, // TODO: handle versioning
            ..
        } = req.input;

        let blob_req = s3s::dto::builders::GetObjectInputBuilder::default()
            .key(blob.id.to_string())
            .checksum_mode(checksum_mode)
            .part_number(part_number)
            .range(range)
            .response_cache_control(response_cache_control)
            .response_content_disposition(response_content_disposition)
            .response_content_encoding(response_content_encoding)
            .response_content_language(response_content_language)
            .response_content_type(response_content_type)
            .response_expires(response_expires);

        let s3s::dto::GetObjectOutput {
            accept_ranges,
            body,
            // bucket_key_enabled,
            cache_control,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            content_disposition,
            content_encoding,
            content_language,
            content_length,
            content_range,
            content_type,
            delete_marker,
            // e_tag,
            expiration,
            expires,
            // last_modified,
            metadata,
            missing_meta,
            // object_lock_legal_hold_status, // TODO: handle locking
            // object_lock_mode,
            // object_lock_retain_until_date,
            parts_count,
            // replication_status,
            // request_charged,
            // restore,
            // sse_customer_algorithm,
            // sse_customer_key_md5,
            // ssekms_key_id,
            // server_side_encryption,
            // storage_class,
            tag_count,
            // version_id, // TODO: handle versioning
            // website_redirect_location,
            ..
        } = self.blob.get_object(blob_req, &blob.placement).await?.output;

        // let bytes = self.blob.get_reader(&blob.id.to_string(), 0, blob.size as u64).await?;
        let output = GetObjectOutput {
            accept_ranges,
            body,
            cache_control,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            content_disposition,
            content_encoding,
            content_language,
            content_length,
            content_range,
            content_type,
            delete_marker,
            e_tag: Some(blob.etag),
            expiration,
            expires,
            last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
                object.last_modified.date(),
                object.last_modified.time(),
                time::UtcOffset::UTC,
            ))),
            metadata,
            missing_meta,
            parts_count,
            tag_count,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let Some(_bucket) = self.db.get_bucket(&req.input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };
        // check ownership
        Ok(S3Response::new(HeadBucketOutput {
            // access_point_alias: todo!(),
            // bucket_location_name: todo!(),
            // bucket_location_type: todo!(),
            // bucket_region: todo!(),
            ..Default::default()
        }))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let HeadObjectInput {
            bucket,
            checksum_mode,
            expected_bucket_owner,
            if_match,
            if_modified_since,
            if_none_match,
            if_unmodified_since,
            key,
            part_number,
            range,
            request_payer,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            version_id,
        } = req.input;
        let input = s3s::dto::GetObjectInput {
            bucket,
            checksum_mode,
            expected_bucket_owner,
            if_match,
            if_modified_since,
            if_none_match,
            if_unmodified_since,
            key,
            part_number,
            range,
            request_payer,
            response_cache_control: None,
            response_content_disposition: None,
            response_content_encoding: None,
            response_content_language: None,
            response_content_type: None,
            response_expires: None,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            version_id,
        };

        let Some((object, blob)) = self.db.get_object(&input).await? else {
            return Err(s3_error!(NoSuchKey, "Key not found"));
        };

        let Some(blob) = blob else {
            return Err(s3_error!(NoSuchKey, "Versioning is not supported yet"));
        };
        // check ownership

        let req = s3s::dto::builders::HeadObjectInputBuilder::default()
            .checksum_mode(input.checksum_mode)
            .key(blob.id.to_string())
            .part_number(input.part_number)
            .range(input.range);

        let mut res = self.blob.head_object(req, &blob.placement).await?;
        res.output.e_tag = Some(blob.etag.clone());
        res.output.last_modified = Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
            object.last_modified.date(),
            object.last_modified.time(),
            time::UtcOffset::UTC,
        )));

        Ok(S3Response::new(res.output))
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
        let v2_resp = self.list_objects_v2(req.map_input(Into::into)).await?;

        Ok(v2_resp.map_output(|v2| ListObjectsOutput {
            contents: v2.contents,
            common_prefixes: v2.common_prefixes,
            delimiter: v2.delimiter,
            encoding_type: v2.encoding_type,
            name: v2.name,
            prefix: v2.prefix,
            max_keys: v2.max_keys,
            is_truncated: v2.is_truncated,
            next_marker: v2.continuation_token,
            ..Default::default()
        }))
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
        self.db.create_blob_temp(&new_blob, &bucket.location).await?;

        let PutObjectInput {
            body,
            // bucket,
            key,
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
            .checksum_algorithm(checksum_algorithm.clone())
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
        let s3s::dto::PutObjectOutput {
            // bucket_key_enabled,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            e_tag,
            // expiration,
            // request_charged,
            // sse_customer_algorithm,
            // sse_customer_key_md5,
            // ssekms_encryption_context,
            // ssekms_key_id,
            // server_side_encryption,
            // version_id,
            ..
        } = put_response.output;

        let object = &crate::meta_store::Object {
            bucket_name: bucket.name,
            oid: key,
            last_modified: PrimitiveDateTime::MIN,
            blob_id: Some(new_blob),
        };
        let blob = crate::meta_store::Blob {
            id: new_blob,
            size: content_length.expect("content length is not provided"), // TODO: check that length exists
            placement: bucket.location,
            etag: e_tag.clone().expect("no etag returned from the backing store"),
        };

        let _timestamp = self.db.commit_object(&object, &blob).await?;
        let response = s3s::dto::PutObjectOutput {
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            e_tag,
            // version_id, // TODO: handle versioning
            ..Default::default()
        };
        Ok(S3Response::new(response))
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let Some(_bucket) = self.db.get_bucket(&input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };
        if req.credentials.is_none() {
            tracing::info!("request is unatharized");
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }

        let Some(upload) = self.db.get_multipart(&input.bucket, &input.key, &input.upload_id).await? else {
            return Err(s3_error!(NoSuchUpload, "UploadPart with such ID not found"));
        };

        let request = s3s::dto::builders::UploadPartInputBuilder::default()
            .body(input.body)
            .checksum_algorithm(input.checksum_algorithm)
            .checksum_crc32(input.checksum_crc32)
            .checksum_crc32c(input.checksum_crc32c)
            .checksum_sha1(input.checksum_sha1)
            .checksum_sha256(input.checksum_sha256)
            .content_length(input.content_length)
            .content_md5(input.content_md5)
            .key(upload.blob_id.to_string())
            .part_number(input.part_number)
            .upload_id(input.upload_id);

        let upstream_response = self.blob.upload_part(request, &upload.location).await?.output;

        let response = s3s::dto::UploadPartOutput {
            checksum_crc32: upstream_response.checksum_crc32,
            checksum_crc32c: upstream_response.checksum_crc32c,
            checksum_sha1: upstream_response.checksum_sha1,
            checksum_sha256: upstream_response.checksum_sha256,
            e_tag: upstream_response.e_tag,
            ..Default::default()
        };
        Ok(S3Response::new(response))
    }
}

fn hex(input: impl AsRef<[u8]>) -> String {
    hex_simd::encode_to_string(input.as_ref(), hex_simd::AsciiCase::Lower)
}
