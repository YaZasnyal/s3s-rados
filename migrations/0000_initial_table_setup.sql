CREATE TABLE users (
    id varchar PRIMARY KEY,
    name varchar not null,
    email varchar not null UNIQUE,
    creation_date timestamp not null
);

CREATE TABLE keys (
    access_key varchar PRIMARY KEY,
    secret_key varchar not null,
    user_id varchar not null,
    creation_date timestamp not null,

	CONSTRAINT user_id_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);
CREATE INDEX keys_user_id ON keys(user_id);

CREATE TYPE blob_location AS (
    region varchar,
    backend varchar
);

CREATE TABLE buckets (
    name varchar(63) PRIMARY KEY,
    user_id varchar not null,
    creation_date timestamp not null,
    location blob_location not null,

    CONSTRAINT user_id_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);
CREATE INDEX buckets_user_id ON buckets(user_id);

CREATE TABLE buckets_temp (
    name varchar(63) PRIMARY KEY,
    location blob_location not null,
    creation_date timestamp not null
);

CREATE TABLE blobs (
    id uuid PRIMARY KEY,
    location blob_location not null,
    etag varchar not null,
    size bigint
);

CREATE TABLE objects (
    bucket varchar not null,
    oid varchar(1024) not null,
    last_modified timestamp not null,
    blob uuid,

    PRIMARY KEY(bucket, oid, last_modified),
    CONSTRAINT bucket_id_fk FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE RESTRICT,
    CONSTRAINT blob_id_fk FOREIGN KEY (blob) REFERENCES blobs(id) ON DELETE RESTRICT
)
PARTITION BY LIST (bucket);
CREATE INDEX ON objects(blob);

CREATE TABLE temp_blobs (
    id uuid PRIMARY KEY,
    uploaded_at timestamp not null,
    location blob_location not null
    -- no CONSTRAINTs
);
CREATE INDEX temp_blobs_uploaded_at ON temp_blobs(uploaded_at);

CREATE TABLE blobs_gc (
    id uuid PRIMARY KEY,
    location blob_location not null
);

INSERT INTO users (id, name, email, creation_date) VALUES ('root', 'Root user', 'root@example.org', CURRENT_TIMESTAMP);
-- TODO: remove
INSERT INTO keys ("access_key", secret_key, user_id, creation_date) VALUES ('qwe', 'asd', 'root', CURRENT_TIMESTAMP);
