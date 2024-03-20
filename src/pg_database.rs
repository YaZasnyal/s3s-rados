use std::fmt::Debug;

use s3s::dto::Timestamp;
use sqlx::pool::PoolConnection;
use tracing::{debug_span, Instrument};
use uuid::Uuid;

use crate::meta_store::{self, Blob, BlobLocation, Bucket, MetaStore, MetaStoreError, Object, Transaction, TransactionError};
use crate::meta_store::{ListOptions, ListResult, User};
use sqlx::postgres::PgPool;
use sqlx::Row;
use sqlx::{Connection, PgConnection, Postgres};

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
        let pool = PgPool::connect(url).await.expect("Unable to establish database connection");

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
        let row = try_!(
            sqlx::query("SELECT name, user_id, creation_date, location FROM buckets WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.db_conn)
                .await
        );

        let Some(row) = row else {
            return Ok(None);
        };

        Ok(Some(Bucket {
            name: try_!(row.try_get("name")),
            owner: try_!(row.try_get("user_id")),
            creation_date: try_!(row.try_get("creation_date")),
            location: try_!(row.try_get("location")),
        }))
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

    #[tracing::instrument(level = "info", skip(self))]
    pub async fn commit_object(&self, object: &Object, blob: &Blob) -> Result<meta_store::Timestamp, s3s::S3Error> {
        let mut tx = try_!(self.db_conn.begin().instrument(debug_span!("db_begin_transaction")).await);
        try_!(
            sqlx::query("INSERT INTO blobs (id, location, etag, size) VALUES($1, ($2, $3)::blob_location, $4, $5)")
                .bind(&blob.id)
                .bind(&blob.placament.region)
                .bind(&blob.placament.backend)
                .bind(&blob.etag)
                .bind(blob.size)
                .execute(&mut *tx)
                .instrument(debug_span!("db_insert_blob"))
                .await
        );

        let timestamp = try_!(
            sqlx::query("INSERT INTO objects (bucket, oid, last_modified, blob) VALUES($1, $2, CURRENT_TIMESTAMP, $3) RETURNING last_modified")
                .bind(&object.bucket_name)
                .bind(&object.oid)
                .bind(&blob.id)
                .fetch_one(&mut *tx)
                .instrument(debug_span!("db_insert_object"))
                .await
        );
        let timestamp: meta_store::Timestamp = try_!(timestamp.try_get("last_modified"));

        try_!(
            sqlx::query("DELETE FROM temp_blobs WHERE id = $1")
                .bind(&blob.id)
                .execute(&mut *tx)
                .instrument(debug_span!("db_delete_temp_blob"))
                .await
        );

        try_!(tx.commit().instrument(debug_span!("db_commit_transaction")).await);
        Ok(timestamp)
    }
}

// #[async_trait::async_trait]
// impl MetaStore for PostgresDatabase {
//     async fn write_object_metadata_with_blob(&self, bucket: &Bucket, object: &Object, blob: &Blob) -> Result<(), s3s::S3Error> {
//         todo!()
//         // let mut tx = try_!(self.db_conn.begin().instrument(debug_span!("db_begin_transaction")).await);
//         // try_!(
//         //     sqlx::query("DELETE FROM temp_blobs WHERE blob_id = $1;")
//         //         .bind(&blob.id)
//         //         .execute(&mut *tx)
//         //         .instrument(debug_span!("db_remove_temp_blob"))
//         //         .await
//         // );

//         // // check etag not empty
//         // try_!(
//         //     sqlx::query("INSERT INTO blobs (id, size, uploaded_at, etag) VALUES ($1, $2, CURRENT_TIMESTAMP, $3);")
//         //         .bind(&blob.id)
//         //         .bind(blob.size)
//         //         .bind(&blob.etag)
//         //         .execute(&mut *tx)
//         //         .instrument(debug_span!("db_insert_permanent_blob"))
//         //         .await
//         // );

//         // // TODO: handle versioned
//         // let old = try_!(
//         //     sqlx::query("SELECT (blob) FROM objects WHERE objects.bucket = $1 AND objects.oid = $2")
//         //         .bind(&object.bucket_name)
//         //         .bind(&object.oid)
//         //         .fetch_optional(&mut *tx)
//         //         .instrument(debug_span!("db_fetch_previous_version"))
//         //         .await
//         // );
//         // if let Some(old) = old {
//         //     let old_blob_id: Uuid = old.get("blob");
//         //     try_!(
//         //         sqlx::query("INSERT INTO blobs_gc (id) VALUES ($1);")
//         //             .bind(&old_blob_id)
//         //             .execute(&mut *tx)
//         //             .instrument(debug_span!("db_put_old_blob_gc"))
//         //             .await
//         //     );
//         //     try_!(
//         //         sqlx::query("DELETE FROM objects WHERE objects.bucket = $1 AND objects.oid = $2")
//         //             .bind(&object.bucket_name)
//         //             .bind(&object.oid)
//         //             .execute(&mut *tx)
//         //             .instrument(debug_span!("db_delete_old_object"))
//         //             .await
//         //     );
//         // }

//         // // TODO: manage object raplacement
//         // try_!(
//         //     sqlx::query("INSERT INTO objects (bucket, oid, last_modified, blob) VALUES ($1, $2, CURRENT_TIMESTAMP, $3)")
//         //         .bind(&object.bucket_name)
//         //         .bind(&object.oid)
//         //         .bind(&blob.id)
//         //         .execute(&mut *tx)
//         //         .instrument(debug_span!("db_insert_object_info"))
//         //         .await
//         // );
//         // // create object or object version
//         // // put blob metadata and remove temp_blob
//         // //
//         // try_!(tx.commit().await);
//         // Ok(())
//     }

//     async fn write_object_metadata(
//         &self,
//         bucket: &str,
//         object: &str,
//         metadata: &s3s::dto::Metadata,
//     ) -> Result<(), MetaStoreError> {
//         todo!()
//     }

//     /// load object metadata from the metadata storage
//     async fn load_object_metadata(
//         &self,
//         bucket: &str,
//         object: &str,
//         _version: &Option<s3s::dto::ObjectVersionId>,
//     ) -> Result<Option<(Object, Option<Blob>)>, s3s::S3Error> {
//         todo!()
//         // // TODO: handle version
//         // let row = try_!(
//         //     sqlx::query(
//         //         r#"SELECT
//         //                 *
//         //             FROM
//         //                 OBJECTS
//         //                 LEFT OUTER JOIN BLOBS ON OBJECTS.BLOB = BLOBS.ID
//         //             WHERE
//         //                 OBJECTS.BUCKET = $1
//         //                 AND OBJECTS.OID = $2
//         //             ORDER BY
//         //                 OBJECTS.LAST_MODIFIED DESC
//         //             LIMIT
//         //                 1"#
//         //     )
//         //     .bind(bucket)
//         //     .bind(object)
//         //     .fetch_optional(&self.db_conn)
//         //     .await
//         // );

//         // let Some(row) = row else {
//         //     return Ok(None);
//         // };

//         // let object = Object {
//         //     bucket_name: bucket.to_owned(),
//         //     oid: object.to_owned(),
//         //     version_id: None, // TODO: handle version
//         //     last_modified: try_!(row.try_get("last_modified")),
//         //     blob_id: try_!(row.try_get("blob")),
//         //     metadata: None, // TODO: handle metadata
//         // };

//         // let blob = if object.blob_id.is_some() {
//         //     Some(Blob {
//         //         id: try_!(row.try_get("id")),
//         //         size: try_!(row.try_get("size")),
//         //         parts: try_!(row.try_get("parts")),
//         //         part_size: try_!(row.try_get("part_size")),
//         //         upload_timestamp: try_!(row.try_get("uploaded_at")),
//         //         etag: try_!(row.try_get("etag")),
//         //     })
//         // } else {
//         //     None
//         // };

//         // Ok(Some((object, blob)))
//     }

//     async fn delete_object_metadata(
//         &self,
//         bucket: &str,
//         object: &str,
//         _version: &Option<s3s::dto::ObjectVersionId>,
//     ) -> Result<(), s3s::S3Error> {
//         let mut tx = try_!(self.db_conn.begin().instrument(debug_span!("db_begin_transaction")).await);
//         let row = try_!(
//             sqlx::query("SELECT (blob) FROM objects WHERE bucket = $1 AND oid = $2")
//                 .bind(bucket)
//                 .bind(object)
//                 .fetch_optional(&mut *tx)
//                 .instrument(debug_span!("db_check_object_exists"))
//                 .await
//         );

//         // TODO: Handle versioned
//         let Some(row) = row else {
//             return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchKey));
//         };
//         let blob: Option<Uuid> = try_!(row.try_get("blob"));
//         if let Some(blob) = blob {
//             try_!(
//                 sqlx::query("INSERT INTO blobs_gc (id) VALUES ($1)")
//                     .bind(blob)
//                     .execute(&mut *tx)
//                     .instrument(debug_span!("db_insert_blob_gc"))
//                     .await
//             );
//         }

//         try_!(
//             sqlx::query("DELETE FROM objects WHERE bucket = $1 AND oid = $2")
//                 .bind(bucket)
//                 .bind(object)
//                 .execute(&mut *tx)
//                 .instrument(debug_span!("db_delete_object"))
//                 .await
//         );

//         try_!(tx.commit().await);
//         Ok(())
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn write_temp_blob(&self, blob: &Blob) -> Result<(), s3s::S3Error> {
//         try_!(
//             sqlx::query("INSERT INTO temp_blobs (blob_id, uploaded_at) VALUES ($1, CURRENT_TIMESTAMP)")
//                 .bind(&blob.id)
//                 .execute(&self.db_conn)
//                 .await
//         );

//         Ok(())
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn clean_temp_blob(&self, blob: &Blob) {
//         sqlx::query("DELETE FROM temp_blobs WHERE blob_id = $1;")
//             .bind(&blob.id)
//             .execute(&self.db_conn)
//             .await
//             .ok();
//     }

//     async fn add_blob_gc(&self, blob: &Blob) -> Result<User, s3s::S3Error> {
//         todo!()
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn create_bucket(&self, owner: &str, bucket: &str) -> Result<Bucket, s3s::S3Error> {
//         let mut tx = try_!(self.db_conn.begin().instrument(debug_span!("db_begin_transaction")).await);
//         // check if already exist
//         let res = sqlx::query("SELECT name, user_id, creation_date FROM buckets WHERE name = $1;")
//             .bind(bucket)
//             .fetch_optional(&mut *tx)
//             .instrument(debug_span!("db_select_bucket_info"))
//             .await;
//         let res = try_!(res);
//         if res.is_some() {
//             return Err(s3s::S3Error::new(s3s::S3ErrorCode::BucketAlreadyExists));
//         }

//         // insert new bucket info
//         let res = sqlx::query("INSERT INTO buckets (name, user_id, creation_date) VALUES ($1, $2, CURRENT_TIMESTAMP);")
//             .bind(bucket)
//             .bind(owner)
//             .execute(&mut *tx)
//             .instrument(debug_span!("db_insert_bucket_info"))
//             .await;
//         try_!(res);

//         // TODO: create partition
//         // try_!(
//         //     // does not want to bind table name for some reason
//         //     sqlx::query(&format!(
//         //         "CREATE TABLE {} PARTITION OF objects FOR VALUES IN ( '{}' );",
//         //         format!("objects_bucket_{}", bucket.replace("-", "_")), bucket
//         //     ))
//         //     .execute(&mut *tx)
//         //     .instrument(debug_span!("db_create_table_partition"))
//         //     .await
//         // );

//         // fetch the result
//         let res = sqlx::query("SELECT name, user_id, creation_date FROM buckets WHERE name = $1;")
//             .bind(bucket)
//             .fetch_one(&mut *tx)
//             .instrument(debug_span!("db_select_bucket_info"))
//             .await;
//         let res = try_!(res);

//         let bucket = Bucket {
//             name: try_!(res.try_get("name")),
//             owner: try_!(res.try_get("user_id")),
//             creation_date: try_!(res.try_get("creation_date")),
//         };

//         try_!(tx.commit().instrument(debug_span!("db_commit_transaction")).await);

//         Ok(bucket)
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn delete_bucket(&self, bucket: &str) -> Result<(), s3s::S3Error> {
//         let res = sqlx::query("DELETE FROM buckets WHERE name = $1;")
//             .bind(bucket)
//             .execute(&self.db_conn)
//             .await;
//         let _res = try_!(res);

//         Ok(())
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn get_bucket_metadata(&self, bucket: &str) -> Result<Option<Bucket>, s3s::S3Error> {
//         let res = sqlx::query("SELECT name, user_id, creation_date FROM buckets WHERE name = $1;")
//             .bind(bucket)
//             .fetch_optional(&self.db_conn)
//             .await;
//         let Some(res) = try_!(res) else {
//             return Ok(None);
//         };

//         Ok(Some(Bucket {
//             name: try_!(res.try_get("name")),
//             owner: try_!(res.try_get("user_id")),
//             creation_date: try_!(res.try_get("creation_date")),
//         }))
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn list_buckets_by_user(&self, user_id: &str) -> Result<Vec<Bucket>, s3s::S3Error> {
//         let res = sqlx::query("SELECT * FROM buckets WHERE user_id = $1 ORDER BY NAME ASC")
//             .bind(user_id)
//             .fetch_all(&self.db_conn)
//             .await;
//         let res = try_!(res);

//         res.into_iter()
//             .map(|r| {
//                 Ok(Bucket {
//                     name: try_!(r.try_get("name")),
//                     owner: try_!(r.try_get("user_id")),
//                     creation_date: try_!(r.try_get("creation_date")),
//                 })
//             })
//             .collect()
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn get_user_by_access_key(&self, key: &str) -> Result<User, s3s::S3Error> {
//         let res = sqlx::query("SELECT users.* from keys JOIN users ON keys.user_id = users.id WHERE keys.access_key = $1")
//             .bind(key)
//             .fetch_optional(&self.db_conn)
//             .await;
//         let Some(res) = try_!(res) else {
//             return Err(s3s::S3Error::new(s3s::S3ErrorCode::NoSuchKey));
//         };

//         Ok(User {
//             id: try_!(res.try_get("id")),
//             name: try_!(res.try_get("name")),
//             email: try_!(res.try_get("email")),
//         })
//     }

//     async fn get_blob_gc(&self) -> anyhow::Result<Vec<Uuid>> {
//         let res = sqlx::query("SELECT (id) FROM blobs_gc LIMIT 1000")
//             .fetch_all(&self.db_conn)
//             .await?;
//         Ok(res
//             .into_iter()
//             .map(|r| {
//                 let id: Uuid = r.try_get("id").expect("Unable to convert table");
//                 id
//             })
//             .collect())
//     }

//     #[tracing::instrument(level = "debug")]
//     async fn list_objects<'a>(&self, options: ListOptions<'a>) -> Result<ListResult, s3s::S3Error> {
//         todo!()
//         // // TODO: Handle versions
//         // // TODO: sanitize input
//         // let substr_regex = format!("#\"{}%#\"{}%", options.prefix.as_ref().map_or("", |v| &v), options.delim);
//         // let like_regex = format!("{}%", options.prefix.as_ref().map_or("", |v| &v));
//         // // if no offset
//         // let rows = try_!(sqlx::query(r#"
//         //     WITH all_oids AS (SELECT *, SUBSTRING(oid FROM $1 FOR '#') AS dir FROM objects WHERE bucket = $3 AND oid > $5 AND oid LIKE $2),
//         //          dirs AS (SELECT DISTINCT dir AS oid, CAST(NULL AS UUID) AS blob, TRUE AS is_dir FROM all_oids WHERE dir IS NOT NULL),
//         //          OIDS_WITH_DIR AS (SELECT *, CASE WHEN DIR IS NULL THEN FALSE WHEN DIR IS NOT NULL THEN TRUE END AS IS_DIR FROM ALL_OIDS),
//         //          JOINED_OIDS AS (SELECT OID, BLOB, IS_DIR FROM OIDS_WITH_DIR WHERE IS_DIR = FALSE UNION ALL SELECT OID, BLOB, IS_DIR FROM DIRS ORDER BY OID ASC LIMIT $4)

//         //          SELECT JOINED_OIDS.oid, JOINED_OIDS.is_dir, JOINED_OIDS.blob, ALL_OIDS.last_modified, blobs.size, blobs.parts, blobs.part_size, blobs.uploaded_at, blobs.etag FROM JOINED_OIDS
// 	    //             LEFT JOIN ALL_OIDS ON JOINED_OIDS.oid = ALL_OIDS.oid
// 	    //             LEFT JOIN blobs ON JOINED_OIDS.blob = blobs.id
//         //     "#)
//         //     .bind(substr_regex)
//         //     .bind(like_regex)
//         //     .bind(options.bucket)
//         //     .bind(options.max_keys as i64)
//         //     .bind(options.marker.as_ref().map_or("", |v| &v))
//         //     .fetch_all(&self.db_conn)
//         //     .instrument(debug_span!("db_list_objects"))
//         //     .await);

//         // // hanle offset

//         // let mut count = 0;
//         // let mut common_prefixes: Vec<String> = Vec::default();
//         // let mut objetcs: Vec<(Object, Option<Blob>)> = Vec::default();
//         // rows.into_iter().try_for_each(|r| {
//         //     count = count + 1;
//         //     let name: String = try_!(r.try_get("oid"));
//         //     let is_dir: bool = try_!(r.try_get("is_dir"));
//         //     if is_dir {
//         //         common_prefixes.push(name);
//         //         return Ok(());
//         //     }

//         //     let obj = Object {
//         //         bucket_name: options.bucket.to_owned(),
//         //         oid: name.to_owned(),
//         //         version_id: None, // TODO handle version
//         //         last_modified: try_!(r.try_get("last_modified")),
//         //         blob_id: try_!(r.try_get("blob")),
//         //         metadata: None, // TODO: handle metadata
//         //     };

//         //     let blob = if obj.blob_id.is_some() {
//         //         Some(Blob {
//         //             id: obj.blob_id.clone().unwrap(),
//         //             size: try_!(r.try_get("size")),
//         //             parts: try_!(r.try_get("parts")),
//         //             part_size: try_!(r.try_get("part_size")),
//         //             upload_timestamp: try_!(r.try_get("uploaded_at")),
//         //             etag: try_!(r.try_get("etag")),
//         //         })
//         //     } else {
//         //         None
//         //     };

//         //     objetcs.push((obj, blob));
//         //     return Ok(());
//         // })?;

//         // let marker = if let Some(l) = objetcs.last() {
//         //     Some(l.0.oid.clone())
//         // } else {
//         //     None
//         // };

//         // Ok(ListResult {
//         //     objects: objetcs,
//         //     common_prefixes: common_prefixes,
//         //     marker: if count >= options.max_keys { marker } else { None },
//         //     version_marker: None,
//         // })
//     }
// }
