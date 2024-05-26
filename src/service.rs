use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use time::PrimitiveDateTime;
use uuid::Uuid;

use crate::config::Settings;
use crate::meta_store::{Blob, MultipartUpload, Object};
use crate::pg_database::PostgresDatabase;
use crate::s3_client::S3Client;

#[derive(Debug)]
pub struct RadosStore {
    _cfg: std::sync::Arc<Settings>,
    db: Box<PostgresDatabase>,
    blob: Box<S3Client>,
}

impl RadosStore {
    pub async fn new(cfg: std::sync::Arc<Settings>) -> Self {
        Self {
            _cfg: cfg.clone(),
            db: Box::new(PostgresDatabase::new(cfg.clone()).await),
            blob: Box::new(S3Client::new(cfg).await),
        }
    }
}

#[async_trait::async_trait]
impl S3 for RadosStore {
    #[tracing::instrument(level = "info", skip_all)]
    async fn abort_multipart_upload(
        &self,
        _req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        Err(s3_error!(NotImplemented, "AbortMultipartUpload is not implemented yet"))
    }

    #[tracing::instrument(level = "info", skip_all)]
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
        let upstream_response = try_!(self.blob.complete_multipart_upload(upstream_request, &bucket.location).await).output;

        // make head request to fetch blob length
        let content_length = try_!(
            self.blob
                .head_object(
                    s3s::dto::builders::HeadObjectInputBuilder::default().key(upload.blob_id.to_string()),
                    &bucket.location
                )
                .await
        )
        .output
        .content_length
        .expect("backing store must return content_length");

        let old_blob = self
            .db
            .complete_multipart(
                &Object {
                    bucket_name: upload.bucket.clone(),
                    oid: upload.oid.clone(),
                    last_modified: time::PrimitiveDateTime::MIN,
                    blob_id: Some(upload.blob_id.clone()),
                },
                &Blob {
                    id: upload.blob_id.clone(),
                    size: content_length,
                    placement: upload.location.clone(),
                    etag: upstream_response
                        .e_tag
                        .as_ref()
                        .expect("no etag returned from the backing store")
                        .clone(),
                },
                &upload,
            )
            .await?;
        if let Some(old_blob) = old_blob {
            if let Ok(_) = self
                .blob
                .delete_object(
                    s3s::dto::builders::DeleteObjectInputBuilder::default().key(old_blob.id.to_string()),
                    &old_blob.placement,
                )
                .await
            {
                // ignore the error. GC must handle it jost fine
                self.db.delete_blob_gc(&old_blob).await.ok();
            }
        }

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

    #[tracing::instrument(level = "info", skip_all)]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let Some(creds) = &req.credentials else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        };

        let user = self.db.get_user_by_access_key(&creds.access_key).await?;
        if let Some(_bucket) = self.db.get_bucket(&req.input.bucket).await? {
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

    #[tracing::instrument(level = "info", skip_all)]
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

        let upstream_request = s3s::dto::builders::CreateMultipartUploadInputBuilder::default()
            .cache_control(input.cache_control)
            .checksum_algorithm(input.checksum_algorithm)
            .content_disposition(input.content_disposition)
            .content_encoding(input.content_encoding)
            .content_language(input.content_language)
            .content_type(input.content_type)
            .key(new_blob.to_string())
            // TODO: handle object lock
            .metadata(input.metadata)
            .tagging(input.tagging);

        let CreateMultipartUploadOutput { upload_id, .. } = self
            .blob
            .create_multipart_upload(upstream_request, &bucket.location)
            .await?
            .output;

        self.db
            .create_multipart(&MultipartUpload {
                bucket: bucket.name.clone(),
                oid: input.key.clone(),
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
            key: Some(input.key),
            upload_id,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "info", skip(self))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        tracing::info!(?req, "delete_bucket");
        if req.credentials.is_none() {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        }

        let Some(bucket) = self.db.get_bucket(&req.input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };
        // TODO: check ownership

        let gc_job_id = self.db.delete_bucket(&req.input.bucket, &bucket.location).await?;
        if let Ok(_) = self.blob.delete_bucket(&req.input.bucket, &bucket.location).await {
            // remove bucket from the gc queue. Ignore the error because GC should handle it just fine
            self.db.delete_bucket_complete(&gc_job_id).await.ok();
        }
        // even if backing store returned an error we return Ok. GC must retry this operation untill it completes
        tracing::info!(?req, "delete_bucket finished");
        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        tracing::info!(?req, "delete_object");
        // // check acl
        // // check bucket lock

        let blob = self.db.delete_object(&req.input).await?;

        // delete blob from the backing store
        if let Ok(_) = self
            .blob
            .delete_object(
                s3s::dto::builders::DeleteObjectInputBuilder::default().key(blob.id.to_string()),
                &blob.placement,
            )
            .await
        {
            self.db.delete_blob_gc(&blob).await.ok();
        };

        let response = DeleteObjectOutput {
            delete_marker: Some(false), // TODO: handle versioned
            request_charged: None,
            version_id: None,
        };
        tracing::info!(?response, "delete_object finished");
        Ok(S3Response::new(response))
    }

    async fn delete_objects(&self, _req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        Err(s3_error!(NotImplemented, "DeleteObjects is not implemented yet"))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        tracing::info!(?req, "get_object");
        // //TODO: validate user
        // //TODO: conditions
        let Some((object, blob)) = self.db.get_object(&req.input).await? else {
            return Err(s3_error!(NoSuchKey, "Key not found"));
        };

        let Some(blob) = blob else {
            return Err(s3_error!(NoSuchKey, "Versioning is not supported yet"));
        };

        let blob_req = s3s::dto::builders::GetObjectInputBuilder::default()
            .key(blob.id.to_string())
            .checksum_mode(req.input.checksum_mode)
            .part_number(req.input.part_number)
            .range(req.input.range)
            .response_cache_control(req.input.response_cache_control)
            .response_content_disposition(req.input.response_content_disposition)
            .response_content_encoding(req.input.response_content_encoding)
            .response_content_language(req.input.response_content_language)
            .response_content_type(req.input.response_content_type)
            .response_expires(req.input.response_expires);

        let upstream_output = try_!(self.blob.get_object(blob_req, &blob.placement).await).output;

        let output = GetObjectOutput {
            accept_ranges: upstream_output.accept_ranges,
            body: upstream_output.body,
            cache_control: upstream_output.cache_control,
            checksum_crc32: upstream_output.checksum_crc32,
            checksum_crc32c: upstream_output.checksum_crc32c,
            checksum_sha1: upstream_output.checksum_sha1,
            checksum_sha256: upstream_output.checksum_sha256,
            content_disposition: upstream_output.content_disposition,
            content_encoding: upstream_output.content_encoding,
            content_language: upstream_output.content_language,
            content_length: upstream_output.content_length,
            content_range: upstream_output.content_range,
            content_type: upstream_output.content_type,
            delete_marker: upstream_output.delete_marker,
            e_tag: Some(blob.etag),
            expiration: upstream_output.expiration,
            expires: upstream_output.expires,
            last_modified: Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
                object.last_modified.date(),
                object.last_modified.time(),
                time::UtcOffset::UTC,
            ))),
            metadata: upstream_output.metadata,
            missing_meta: upstream_output.missing_meta,
            parts_count: upstream_output.parts_count,
            tag_count: upstream_output.tag_count,
            ..Default::default()
        };
        tracing::info!(?output, "get_object finished");
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "info", skip_all)]
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

    #[tracing::instrument(level = "info", skip_all)]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        tracing::info!(?req, "head_object");
        let input = s3s::dto::GetObjectInput {
            bucket: req.input.bucket,
            checksum_mode: req.input.checksum_mode,
            expected_bucket_owner: req.input.expected_bucket_owner,
            if_match: req.input.if_match,
            if_modified_since: req.input.if_modified_since,
            if_none_match: req.input.if_none_match,
            if_unmodified_since: req.input.if_unmodified_since,
            key: req.input.key,
            part_number: req.input.part_number,
            range: req.input.range,
            request_payer: req.input.request_payer,
            response_cache_control: None,
            response_content_disposition: None,
            response_content_encoding: None,
            response_content_language: None,
            response_content_type: None,
            response_expires: None,
            sse_customer_algorithm: req.input.sse_customer_algorithm,
            sse_customer_key: req.input.sse_customer_key,
            sse_customer_key_md5: req.input.sse_customer_key_md5,
            version_id: req.input.version_id,
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

        let mut res = try_!(self.blob.head_object(req, &blob.placement).await);
        res.output.e_tag = Some(blob.etag.clone());
        res.output.last_modified = Some(s3s::dto::Timestamp::from(time::OffsetDateTime::new_in_offset(
            object.last_modified.date(),
            object.last_modified.time(),
            time::UtcOffset::UTC,
        )));
        tracing::info!(output = ?res.output, "head_object");
        Ok(S3Response::new(res.output))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        tracing::info!(?req, "list_buckets");
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
        tracing::info!(?output, "list_buckets finished");
        Ok(S3Response::new(output))
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        //Err(s3_error!(NotImplemented, "ListMultipartUploads is not implemented yet"))
        tracing::info!(?req, "list_multipart_uploads");
        let Some(creds) = &req.credentials else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::AccessDenied));
        };

        let Some(bucket) = self.db.get_bucket(&req.input.bucket).await? else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchBucket));
        };

        let res = self.db.list_multipart(&req.input).await;
        todo!()
    }

    async fn list_object_versions(
        &self,
        _req: S3Request<ListObjectVersionsInput>,
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

    #[tracing::instrument(level = "info", skip_all)]
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

    #[tracing::instrument(level = "info", skip_all)]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        tracing::info!(?req, "list_objects_v2");
        let list_result = try_!(self.db.list_objects(&req.input).await);

        let crate::meta_store::ListResult {
            objects,
            common_prefixes,
            marker,
            //version_marker,
            ..
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
                size: if let Some(b) = &b { Some(b.size) } else { Some(0) },
                storage_class: None,
            })
            .collect();

        let common_prefixes = common_prefixes
            .into_iter()
            .map(|p| s3s::dto::CommonPrefix { prefix: Some(p) })
            .collect();

        let output = s3s::dto::ListObjectsV2Output {
            common_prefixes: Some(common_prefixes),
            key_count: Some(objects.len() as i32),
            contents: Some(objects),
            delimiter: req.input.delimiter,
            encoding_type: None,
            is_truncated: Some(marker.is_some()),
            max_keys: Some(1000),
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            request_charged: None,
            continuation_token: req.input.continuation_token,
            next_continuation_token: marker,
            start_after: None,
        };

        tracing::trace!(?output, "list_objects_v2 finished");
        tracing::info!("list_objects_v2 finished successfully");
        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "info", skip_all, err)]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        tracing::info!(?req, "received put request");
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
        tracing::info!(location = ?bucket.location, "creating temp blob");
        self.db.create_blob_temp(&new_blob, &bucket.location).await?;

        // TODO: put additional metadata for redundancy
        let put_request = s3s::dto::PutObjectInput::builder()
            .body(input.body)
            .key(new_blob.to_string())
            .metadata(input.metadata)
            .tagging(input.tagging)
            .content_length(input.content_length)
            .content_md5(input.content_md5)
            .cache_control(input.cache_control)
            .checksum_algorithm(input.checksum_algorithm.clone())
            .checksum_crc32(input.checksum_crc32)
            .checksum_crc32c(input.checksum_crc32c)
            .checksum_sha1(input.checksum_sha1)
            .checksum_sha256(input.checksum_sha256)
            .content_disposition(input.content_disposition)
            .content_encoding(input.content_encoding)
            .content_language(input.content_language)
            .content_type(input.content_type)
            .sse_customer_algorithm(input.sse_customer_algorithm)
            .sse_customer_key(input.sse_customer_key)
            .sse_customer_key_md5(input.sse_customer_key_md5)
            .ssekms_encryption_context(input.ssekms_encryption_context)
            .ssekms_key_id(input.ssekms_key_id)
            .server_side_encryption(input.server_side_encryption);

        let put_response = match self.blob.put_object(put_request, &bucket.location).await {
            Ok(x) => x,
            Err(e) => {
                self.db.delete_blob_temp(&new_blob).await?;
                return Err(e);
            }
        };

        let object = &crate::meta_store::Object {
            bucket_name: bucket.name,
            oid: input.key,
            last_modified: PrimitiveDateTime::MIN,
            blob_id: Some(new_blob),
        };
        let blob = crate::meta_store::Blob {
            id: new_blob,
            size: input.content_length.expect("content length is not provided"), // TODO: check that length exists
            placement: bucket.location,
            etag: put_response
                .output
                .e_tag
                .clone()
                .expect("no etag returned from the backing store"),
        };

        let (_timestamp, old_blob) = try_!(self.db.commit_object(&object, &blob).await);
        if let Some(old_blob) = old_blob {
            if let Ok(_) = self
                .blob
                .delete_object(
                    s3s::dto::builders::DeleteObjectInputBuilder::default().key(old_blob.id.to_string()),
                    &old_blob.placement,
                )
                .await
            {
                // ignore the error. GC must handle it jost fine
                self.db.delete_blob_gc(&old_blob).await.ok();
            }
        }

        let response = s3s::dto::PutObjectOutput {
            checksum_crc32: put_response.output.checksum_crc32,
            checksum_crc32c: put_response.output.checksum_crc32c,
            checksum_sha1: put_response.output.checksum_sha1,
            checksum_sha256: put_response.output.checksum_sha256,
            e_tag: put_response.output.e_tag,
            // version_id, // TODO: handle versioning
            ..Default::default()
        };
        tracing::info!(?response, "finished blob upload");
        Ok(S3Response::new(response))
    }

    #[tracing::instrument(level = "info", skip_all, err)]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        tracing::info!(?req, "upload_part");
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
        tracing::info!(?response, "finished part upload");
        Ok(S3Response::new(response))
    }
}
