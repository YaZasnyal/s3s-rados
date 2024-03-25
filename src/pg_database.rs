use std::fmt::Debug;

use tracing::{debug_span, Instrument};
use uuid::Uuid;

use crate::meta_store::{self, Blob, BlobLocation, Bucket, MultipartUpload, Object};
use crate::meta_store::{ListResult, User};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::Row;
use sqlx::{ConnectOptions, Connection, Postgres};

macro_rules! retry_transaction {
    ($func:block) => {
        async {
            for i in 1..=10 {
                let e = match async { $func }.instrument(tracing::debug_span!("db_try_transaction")).await {
                    Ok(x) => return Ok(x),
                    Err(e) => e,
                };
                if i == 10 {
                    tracing::error!(?e, "transaction failed with an error");
                }

                match e {
                    TransactionError::S3Error(e) => {
                        return Err(e);
                    }
                    TransactionError::SqlError(e) => {
                        tracing::debug!(?e, "transaction failed with an error");
                    }
                }
            }

            tracing::warn!("unable to commit database transaction after multiple retries");
            Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::InternalError,
                "unable to commit database transaction after multiple retries",
            ))
        }
        .await
    };
}

pub struct PostgresDatabase {
    db_conn: PgPool,
}

impl PostgresDatabase {
    pub async fn new() -> Self {
        let url = "postgresql://localhost:5433/?user=yugabyte&password=yugabyte";
        let mut conn = sqlx::PgConnection::connect(url)
            .await
            .expect("unable to connect to the database");

        // TODO: replace with config values
        let res = sqlx::query("SELECT * FROM pg_catalog.pg_database WHERE datname = $1")
            .bind("s3srados")
            .fetch_optional(&mut conn)
            .await
            .expect("");
        if res.is_none() {
            tracing::info!("database not found... creating one");
            sqlx::query(
                r#"
            CREATE DATABASE s3srados
                WITH
                OWNER = yugabyte
                ENCODING = 'UTF8'
                LC_COLLATE = 'C'
                LC_CTYPE = 'en_US.UTF-8'
                CONNECTION LIMIT = -1
                IS_TEMPLATE = False;
            "#,
            )
            .execute(&mut conn)
            .await
            .expect("unable to create database");
            tracing::info!("database was created successfully");
        }

        let url = "postgresql://localhost:5433/s3srados?user=yugabyte&password=yugabyte";
        let pool = PgPoolOptions::new()
            .min_connections(20)
            .max_connections(50)
            .connect(url)
            .await
            .unwrap();

        tracing::info!("starting database migration");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("unable to perform migrations");
        tracing::info!("finished database migration");

        Self { db_conn: pool }
    }
}

impl Debug for PostgresDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgDatabase").finish()
    }
}

impl PostgresDatabase {
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn get_user_by_access_key(&self, key: &str) -> Result<User, s3s::S3Error> {
        let res = sqlx::query("SELECT users.* from keys JOIN users ON keys.user_id = users.id WHERE keys.access_key = $1")
            .bind(key)
            .fetch_optional(&self.db_conn)
            .await;
        let Some(res) = try_!(res) else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchKey));
        };

        Ok(User {
            id: try_!(res.try_get("id")),
            name: try_!(res.try_get("name")),
            email: try_!(res.try_get("email")),
            creation_date: try_!(res.try_get("creation_date")),
        })
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn get_bucket(&self, name: &str) -> Result<Option<Bucket>, s3s::S3Error> {
        retry_transaction!({
            let row = sqlx::query("SELECT name, user_id, creation_date, location FROM buckets WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.db_conn)
                .await?;

            let Some(row) = row else {
                return Ok(None);
            };

            Ok(Some(Bucket {
                name: row.try_get("name")?,
                owner: row.try_get("user_id")?,
                creation_date: row.try_get("creation_date")?,
                location: row.try_get("location")?,
            }))
        })
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn create_bucket_temp(&self, name: &str, region: &BlobLocation) -> Result<(), s3s::S3Error> {
        try_!(
            sqlx::query("INSERT INTO buckets_temp (name, location, creation_date) VALUES ($1, ($2, $3)::blob_location, CURRENT_TIMESTAMP)")
                .bind(name)
                .bind(&region.region)
                .bind(&region.backend)
                .execute(&self.db_conn)
                .await
        );
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn delete_bucket_temp(&self, name: &str) -> Result<(), s3s::S3Error> {
        try_!(
            sqlx::query("DELETE FROM buckets_temp WHERE name = $1")
                .bind(name)
                .execute(&self.db_conn)
                .await
        );
        Ok(())
    }

    /// deletes buckets from the database and places it for garbage collector
    ///
    /// GC should remove all buckets from the backing stores
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn delete_bucket(&self, name: &str, location: &BlobLocation) -> Result<Uuid, s3s::S3Error> {
        let mut tx = try_!(self.db_conn.begin().await);
        let row = try_!(
            sqlx::query("SELECT 1 FROM objects WHERE bucket = $1 LIMIT 1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await
        );

        if let Some(_) = row {
            tracing::info!(?name, ?location, "bucket is not empty");
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::BucketNotEmpty));
        }

        try_!(
            sqlx::query("DELETE FROM buckets WHERE name = $1")
                .bind(&name)
                .execute(&mut *tx)
                .await
        );

        let job_uuid = Uuid::new_v4();
        try_!(
            sqlx::query("INSERT INTO buckets_gc (id, location) VALUES ($1, ($2, $3)::blob_location)")
                .bind(job_uuid)
                .bind(&location.region)
                .bind(&location.backend)
                .execute(&mut *tx)
                .await
        );

        try_!(
            sqlx::query(&format!("DROP TABLE {}", format!("objects_bucket_{}", name.replace("-", "_"))))
                .execute(&mut *tx)
                .await
        );

        try_!(tx.commit().await);
        tracing::info!(?name, ?location, "bucket removed from the database");
        Ok(job_uuid)
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn delete_bucket_complete(&self, job_id: &Uuid) -> Result<(), s3s::S3Error> {
        try_!(
            sqlx::query("DELETE FROM buckets_gc WHERE id = $1")
                .bind(&job_id)
                .execute(&self.db_conn)
                .await
        );
        Ok(())
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn commit_bucket(&self, name: &str, user: &User, region: &BlobLocation) -> Result<(), s3s::S3Error> {
        let mut tx = try_!(self.db_conn.begin().await);
        try_!(
            sqlx::query("INSERT INTO buckets (name, user_id, creation_date, location) VALUES ($1, $2, CURRENT_TIMESTAMP, ($3, $4)::blob_location)")
                .bind(name)
                .bind(&user.id)
                .bind(&region.region)
                .bind(&region.backend)
                .execute(&mut *tx)
                .await
        );
        try_!(
            sqlx::query("DELETE FROM buckets_temp WHERE name = $1")
                .bind(name)
                .execute(&mut *tx)
                .await
        );
        // TODO: create partition
        try_!(
            // does not want to bind table name for some reason
            sqlx::query(&format!(
                "CREATE TABLE {} PARTITION OF objects FOR VALUES IN ( '{}' );",
                format!("objects_bucket_{}", name.replace("-", "_")),
                name
            ))
            .execute(&mut *tx)
            .instrument(debug_span!("db_create_table_partition"))
            .await
        );

        try_!(tx.commit().await);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_buckets_by_user(&self, user_id: &str) -> Result<Vec<Bucket>, s3s::S3Error> {
        let res = sqlx::query("SELECT * FROM buckets WHERE user_id = $1 ORDER BY NAME ASC")
            .bind(user_id)
            .fetch_all(&self.db_conn)
            .await;
        let res = try_!(res);

        res.into_iter()
            .map(|r| {
                Ok(Bucket {
                    name: try_!(r.try_get("name")),
                    owner: try_!(r.try_get("user_id")),
                    creation_date: try_!(r.try_get("creation_date")),
                    location: try_!(r.try_get("location")),
                })
            })
            .collect()
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn create_blob_temp(&self, id: &Uuid, location: &BlobLocation) -> Result<(), s3s::S3Error> {
        try_!(
            sqlx::query(
                "INSERT INTO temp_blobs (id, uploaded_at, location) VALUES($1, CURRENT_TIMESTAMP, ($2, $3)::blob_location)"
            )
            .bind(id)
            .bind(&location.region)
            .bind(&location.backend)
            .execute(&self.db_conn)
            .await
        );
        Ok(())
    }

    /// the object has never been bushed to the backing store
    #[tracing::instrument(level = "info", skip(self))]
    pub async fn delete_blob_temp(&self, id: &Uuid) -> Result<(), s3s::S3Error> {
        try_!(
            sqlx::query("DELETE FROM temp_blobs WHERE id = $1")
                .bind(id)
                .execute(&self.db_conn)
                .await
        );
        Ok(())
    }

    /// Inserts a new object and returns the old blob if present
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn commit_object(
        &self,
        object: &Object,
        blob: &Blob,
    ) -> Result<(meta_store::Timestamp, Option<Blob>), s3s::S3Error> {
        retry_transaction!({
            let mut tx = self.db_conn.begin().instrument(debug_span!("db_begin_transaction")).await?;
            sqlx::query(
                r#"
                WITH DELETED AS (DELETE FROM temp_blobs WHERE id = $1)
                INSERT INTO blobs (id, location, etag, size) VALUES($1, ($2, $3)::blob_location, $4, $5)"#,
            )
            .bind(&blob.id)
            .bind(&blob.placement.region)
            .bind(&blob.placement.backend)
            .bind(&blob.etag)
            .bind(blob.size)
            .execute(&mut *tx)
            .instrument(debug_span!("db_insert_blob"))
            .await?;

            let row = sqlx::query("SELECT * FROM OBJECTS JOIN BLOBS ON BLOBS.ID = OBJECTS.BLOB WHERE BUCKET = $1 AND OID = $2 ORDER BY LAST_MODIFIED DESC LIMIT 1")
                .bind(&object.bucket_name)
                .bind(&object.oid)
                .fetch_optional(&mut *tx)
                .instrument(debug_span!("db_select_old_object"))
                .await?;
            let old_blob = if let Some(row) = row {
                let blob_id: Uuid = row.try_get("id")?;
                let last_modified: meta_store::Timestamp = row.try_get("last_modified")?;
                sqlx::query("INSERT INTO BLOBS_GC (ID) VALUES ($1)")
                    .bind(&blob_id)
                    .execute(&mut *tx)
                    .instrument(debug_span!("db_gc_old_blob"))
                    .await?;
                sqlx::query("DELETE FROM OBJECTS WHERE BUCKET = $1 AND OID = $2 AND LAST_MODIFIED = $3")
                    .bind(&object.bucket_name)
                    .bind(&object.oid)
                    .bind(&last_modified)
                    .execute(&mut *tx)
                    .instrument(debug_span!("db_delete_old_object"))
                    .await?;

                Some(Blob {
                    id: row.try_get("id")?,
                    size: row.try_get("size")?,
                    etag: row.try_get("etag")?,
                    placement: row.try_get("location")?,
                })
            } else {
                None
            };

            let timestamp = sqlx::query("INSERT INTO objects (bucket, oid, last_modified, blob) VALUES($1, $2, CURRENT_TIMESTAMP, $3) RETURNING last_modified")
                .bind(&object.bucket_name)
                .bind(&object.oid)
                .bind(&blob.id)
                .fetch_one(&mut *tx)
                .instrument(debug_span!("db_insert_object"))
                .await?;
            let timestamp: meta_store::Timestamp = timestamp.try_get("last_modified")?;

            tx.commit().instrument(debug_span!("db_commit_transaction")).await?;
            Ok((timestamp, old_blob))
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_object(&self, req: &s3s::dto::GetObjectInput) -> Result<Option<(Object, Option<Blob>)>, s3s::S3Error> {
        // TODO: handle version
        let row = try_!(
            sqlx::query(
                r#"SELECT
                        *
                    FROM
                        OBJECTS
                        LEFT OUTER JOIN BLOBS ON OBJECTS.BLOB = BLOBS.ID
                    WHERE
                        OBJECTS.BUCKET = $1
                        AND OBJECTS.OID = $2
                    ORDER BY
                        OBJECTS.LAST_MODIFIED DESC
                    LIMIT
                        1"#
            )
            .bind(&req.bucket)
            .bind(&req.key)
            .fetch_optional(&self.db_conn)
            .await
        );

        let Some(row) = row else {
            return Ok(None);
        };

        let object = Object {
            bucket_name: req.bucket.to_owned(),
            oid: req.key.to_owned(),
            last_modified: try_!(row.try_get("last_modified")),
            blob_id: try_!(row.try_get("blob")),
        };

        let blob = if object.blob_id.is_some() {
            Some(Blob {
                id: try_!(row.try_get("id")),
                size: try_!(row.try_get("size")),
                // upload_timestamp: try_!(row.try_get("uploaded_at")),
                etag: try_!(row.try_get("etag")),
                placement: try_!(row.try_get("location")),
            })
        } else {
            None
        };

        Ok(Some((object, blob)))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn delete_object(&self, req: &s3s::dto::DeleteObjectInput) -> Result<Blob, s3s::S3Error> {
        // TODO: handle versioning
        let mut tx = try_!(self.db_conn.begin().await);
        let row = try_!(
            sqlx::query(
                r#"
                WITH removed AS (
                    DELETE FROM objects WHERE bucket=$1 AND oid=$2 RETURNING *
                )
                SELECT * FROM removed JOIN blobs ON removed.blob = blobs.id"#
            )
            .bind(&req.bucket)
            .bind(&req.key)
            .fetch_optional(&mut *tx)
            .await
        );

        let Some(row) = row else {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchKey));
        };

        let blob = Blob {
            id: try_!(row.try_get("id")),
            size: try_!(row.try_get("size")),
            etag: try_!(row.try_get("etag")),
            placement: try_!(row.try_get("location")),
        };

        try_!(
            sqlx::query("INSERT INTO blobs_gc(id) VALUES ($1)")
                .bind(&blob.id)
                .execute(&mut *tx)
                .await
        );

        try_!(tx.commit().await);
        Ok(blob)
    }

    pub async fn list_objects(&self, req: &s3s::dto::ListObjectsV2Input) -> Result<ListResult, s3s::S3Error> {
        // TODO: Handle versions
        // TODO: sanitize input
        let max_keys = req.max_keys.unwrap_or(1000) as i64;
        let substr_regex = format!(
            "#\"{}%#\"{}%",
            req.prefix.as_ref().map_or("", |v| &v),
            req.delimiter.as_ref().map_or("/", |x| &x)
        );
        let like_regex = format!("{}%", req.prefix.as_ref().map_or("", |v| &v));
        // if no offset
        let rows = try_!(sqlx::query(r#"
            WITH all_oids AS (SELECT *, SUBSTRING(oid FROM $1 FOR '#') AS dir FROM objects WHERE bucket = $3 AND oid > $5 AND oid LIKE $2),
                 dirs AS (SELECT DISTINCT dir AS oid, CAST(NULL AS UUID) AS blob, TRUE AS is_dir FROM all_oids WHERE dir IS NOT NULL),
                 OIDS_WITH_DIR AS (SELECT *, CASE WHEN DIR IS NULL THEN FALSE WHEN DIR IS NOT NULL THEN TRUE END AS IS_DIR FROM ALL_OIDS),
                 JOINED_OIDS AS (SELECT OID, BLOB, IS_DIR FROM OIDS_WITH_DIR WHERE IS_DIR = FALSE UNION ALL SELECT OID, BLOB, IS_DIR FROM DIRS ORDER BY OID ASC LIMIT $4)

                 SELECT JOINED_OIDS.oid, JOINED_OIDS.is_dir, JOINED_OIDS.blob, ALL_OIDS.last_modified, blobs.size, blobs.etag, blobs.location FROM JOINED_OIDS
	                LEFT JOIN ALL_OIDS ON JOINED_OIDS.oid = ALL_OIDS.oid
	                LEFT JOIN blobs ON JOINED_OIDS.blob = blobs.id
            "#)
            .bind(substr_regex)
            .bind(like_regex)
            .bind(&req.bucket)
            .bind(max_keys)
            .bind(req.start_after.as_ref().map_or("", |v| &v))
            .fetch_all(&self.db_conn)
            .instrument(debug_span!("db_list_objects"))
            .await);

        //         // // hanle offset

        let mut count = 0;
        let mut common_prefixes: Vec<String> = Vec::default();
        let mut objetcs: Vec<(Object, Option<Blob>)> = Vec::default();
        rows.into_iter().try_for_each(|r| {
            count = count + 1;
            let name: String = try_!(r.try_get("oid"));
            let is_dir: bool = try_!(r.try_get("is_dir"));
            if is_dir {
                common_prefixes.push(name);
                return Ok(());
            }

            let obj = Object {
                bucket_name: req.bucket.to_owned(),
                oid: name.to_owned(),
                // version_id: None, // TODO handle version
                last_modified: try_!(r.try_get("last_modified")),
                blob_id: try_!(r.try_get("blob")),
                // metadata: None, // TODO: handle metadata
            };

            let blob = if obj.blob_id.is_some() {
                Some(Blob {
                    id: obj.blob_id.clone().unwrap(),
                    size: try_!(r.try_get("size")),
                    placement: try_!(r.try_get("location")),
                    etag: try_!(r.try_get("etag")),
                })
            } else {
                None
            };

            objetcs.push((obj, blob));
            return Ok(());
        })?;

        let marker = if let Some(l) = objetcs.last() {
            Some(l.0.oid.clone())
        } else {
            None
        };

        Ok(ListResult {
            objects: objetcs,
            common_prefixes: common_prefixes,
            marker: if count >= max_keys { marker } else { None },
            version_marker: None,
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn delete_blob_gc(&self, blob: &Blob) -> Result<(), s3s::S3Error> {
        try_!(
            sqlx::query(
                r#"
                WITH BLOBDEL AS (DELETE FROM blobs WHERE id = $1)
                DELETE FROM blobs_gc WHERE id = $1"#
            )
            .bind(blob.id)
            .execute(&self.db_conn)
            .await
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn create_multipart(&self, upload: &MultipartUpload) -> Result<(), s3s::S3Error> {
        try_!(
            sqlx::query("INSERT INTO active_multipart_uploads (bucket, oid, upload_id, blob_id, uploaded_at, location) VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, ($5, $6)::blob_location)")
            .bind(&upload.bucket)
            .bind(&upload.oid)
            .bind(&upload.upload_id)
            .bind(&upload.blob_id)
            .bind(&upload.location.region)
            .bind(&upload.location.backend)
            .execute(&self.db_conn)
            .await
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_multipart(&self, bucket: &str, oid: &str, upload_id: &str) -> Result<Option<MultipartUpload>, s3s::S3Error> {
        let row = try_!(
            sqlx::query("SELECT * FROM active_multipart_uploads WHERE bucket = $1 AND oid = $2 AND upload_id = $3")
                .bind(bucket)
                .bind(oid)
                .bind(upload_id)
                .fetch_optional(&self.db_conn)
                .await
        );

        let Some(row) = row else {
            return Ok(None);
        };

        Ok(Some(MultipartUpload {
            bucket: try_!(row.try_get("bucket")),
            oid: try_!(row.try_get("oid")),
            upload_id: try_!(row.try_get("upload_id")),
            blob_id: try_!(row.try_get("blob_id")),
            uploaded_at: try_!(row.try_get("uploaded_at")),
            location: try_!(row.try_get("location")),
        }))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn list_multipart(&self, bucket: &str) -> Result<Vec<MultipartUpload>, s3s::S3Error> {
        todo!()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn complete_multipart(&self, object: &Object, blob: &Blob, upload: &MultipartUpload) -> Result<(), s3s::S3Error> {
        let mut tx = try_!(self.db_conn.begin().instrument(debug_span!("db_begin_transaction")).await);
        try_!(
            sqlx::query("INSERT INTO blobs (id, location, etag, size) VALUES($1, ($2, $3)::blob_location, $4, $5)")
                .bind(&blob.id)
                .bind(&blob.placement.region)
                .bind(&blob.placement.backend)
                .bind(&blob.etag)
                .bind(blob.size)
                .execute(&mut *tx)
                .instrument(debug_span!("db_insert_blob"))
                .await
        );

        try_!(
            sqlx::query("INSERT INTO objects (bucket, oid, last_modified, blob) VALUES($1, $2, CURRENT_TIMESTAMP, $3)")
                .bind(&object.bucket_name)
                .bind(&object.oid)
                .bind(&blob.id)
                .execute(&mut *tx)
                .instrument(debug_span!("db_insert_object"))
                .await
        );

        try_!(
            sqlx::query("DELETE FROM active_multipart_uploads WHERE bucket = $1 AND oid = $2 AND upload_id = $3")
                .bind(&upload.bucket)
                .bind(&upload.oid)
                .bind(&upload.upload_id)
                .execute(&mut *tx)
                .instrument(debug_span!("db_delete_active_multipart"))
                .await
        );

        try_!(tx.commit().instrument(debug_span!("db_commit_transaction")).await);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn abort_multipart(&self, upload: &MultipartUpload) -> Result<(), s3s::S3Error> {
        todo!()
    }
}

#[derive(thiserror::Error, Debug)]
enum TransactionError {
    #[error("S3 error")]
    S3Error(#[from] s3s::S3Error),
    #[error("SQL error")]
    SqlError(#[from] sqlx::Error),
}
