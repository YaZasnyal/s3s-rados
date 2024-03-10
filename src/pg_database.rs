use std::fmt::Debug;

use tracing::{debug_span, Instrument};

use crate::meta_store::{Blob, Bucket, MetaStore, MetaStoreError, Object, Transaction};
use crate::meta_store::User;
use sqlx::postgres::PgPool;
use sqlx::Connection;
use sqlx::Row;

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

#[async_trait::async_trait]
impl MetaStore for PostgresDatabase {
    async fn write_object_metadata_with_blob(&self, bucket: &Bucket, object: &Object, blob: &Blob) -> Result<(), MetaStoreError> {
        todo!()
    }

    async fn write_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        metadata: &s3s::dto::Metadata,
    ) -> Result<(), MetaStoreError> {
        todo!()
    }

    /// load object metadata from the metadata storage
    async fn load_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        version: &Option<s3s::dto::ObjectVersionId>,
    ) -> Result<s3s::dto::Metadata, MetaStoreError> {
        todo!()
    }

    async fn delete_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        version: &Option<s3s::dto::ObjectVersionId>,
    ) -> Result<(), MetaStoreError> {
        todo!()
    }

    async fn write_blob(&self, blob: &Blob) -> Result<Box<dyn Transaction>, s3s::S3Error> {
        todo!()
    }

    #[tracing::instrument(level = "debug")]
    async fn create_bucket(&self, owner: &str, bucket: &str) -> Result<Bucket, s3s::S3Error> {
        let mut tx = try_!(self.db_conn.begin().instrument(debug_span!("db_begin_transaction")).await);
        // check if already exist
        let res = sqlx::query("SELECT name, user_id, creation_date FROM buckets WHERE name = $1;")
            .bind(bucket)
            .fetch_optional(&mut *tx)
            .instrument(debug_span!("db_select_bucket_info"))
            .await;
        let res = try_!(res);
        if res.is_some() {
            return Err(s3s::S3Error::new(s3s::S3ErrorCode::BucketAlreadyExists));
        }

        // insert new bucket info
        let res = sqlx::query("INSERT INTO buckets (name, user_id, creation_date) VALUES ($1, $2, CURRENT_TIMESTAMP);")
            .bind(bucket)
            .bind(owner)
            .execute(&mut *tx)
            .instrument(debug_span!("db_insert_bucket_info"))
            .await;
        try_!(res);
        // TODO: insert partition

        // fetch the result
        let res = sqlx::query("SELECT name, user_id, creation_date FROM buckets WHERE name = $1;")
            .bind(bucket)
            .fetch_one(&mut *tx)
            .instrument(debug_span!("db_select_bucket_info"))
            .await;
        let res = try_!(res);

        let bucket = Bucket {
            name: try_!(res.try_get("name")),
            owner: try_!(res.try_get("user_id")),
            creation_date: try_!(res.try_get("creation_date")),
        };

        try_!(tx.commit().instrument(debug_span!("db_commit_transaction")).await);

        Ok(bucket)
    }

    #[tracing::instrument(level = "debug")]
    async fn delete_bucket(&self, bucket: &str) -> Result<(), s3s::S3Error> {
        let res = sqlx::query("DELETE FROM buckets WHERE name = $1;")
            .bind(bucket)
            .execute(&self.db_conn)
            .await;
        let _res = try_!(res);

        Ok(())
    }

    #[tracing::instrument(level = "debug")]
    async fn get_bucket_metadata(&self, bucket: &str) -> Result<Option<Bucket>, s3s::S3Error> {
        let res = sqlx::query("SELECT name, user_id, creation_date FROM buckets WHERE name = $1;")
            .bind(bucket)
            .fetch_optional(&self.db_conn)
            .await;
        let Some(res) = try_!(res) else {
            return Ok(None);
        };

        Ok(Some(Bucket {
            name: try_!(res.try_get("name")),
            owner: try_!(res.try_get("user_id")),
            creation_date: try_!(res.try_get("creation_date")),
        }))
    }

    #[tracing::instrument(level = "debug")]
    async fn list_buckets_by_user(&self, user_id: &str) -> Result<Vec<Bucket>, s3s::S3Error> {
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
                })
            })
            .collect()
    }

    #[tracing::instrument(level = "debug")]
    async fn get_user_by_access_key(&self, key: &str) -> Result<User, s3s::S3Error> {
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
        })
    }
}
