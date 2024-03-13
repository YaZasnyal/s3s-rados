CREATE TABLE users (
    id varchar PRIMARY KEY,
    name varchar not null,
    email varchar not null UNIQUE
);

CREATE TABLE keys (
    access_key varchar PRIMARY KEY,
    secret_key varchar not null,
    user_id varchar not null,

	CONSTRAINT user_id_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);
CREATE INDEX keys_user_id ON keys(user_id);

CREATE TABLE buckets (
    name varchar(63) PRIMARY KEY,
    user_id varchar not null,
    creation_date timestamp not null,

    CONSTRAINT user_id_fk FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
);
CREATE INDEX buckets_user_id ON buckets(user_id);

CREATE TABLE blobs (
    id uuid PRIMARY KEY,
    size bigint not null,
    parts smallint,
    part_size bigint,
    uploaded_at timestamp not null,
    etag varchar not null
);

CREATE TABLE objects (
    bucket varchar not null,
    oid varchar not null,
    last_modified timestamp not null,
    blob uuid,

    PRIMARY KEY(bucket, oid, last_modified),
    CONSTRAINT bucket_id_fk FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE RESTRICT,
    CONSTRAINT blob_id_fk FOREIGN KEY (blob) REFERENCES blobs(id) ON DELETE RESTRICT
);
-- PARTITION BY LIST (bucket);
CREATE INDEX ON objects(blob);

CREATE TABLE temp_blobs (
    blob_id uuid PRIMARY KEY,
    uploaded_at timestamp not null

    -- no CONSTRAINTs
);
CREATE INDEX temp_blobs_uploaded_at ON temp_blobs(uploaded_at);

CREATE TABLE blobs_gc (
    id uuid PRIMARY KEY
);

INSERT INTO users (id, name, email) VALUES ('root', 'Root user', 'root@example.org');
-- TODO: remove
INSERT INTO keys ("access_key", secret_key, user_id) VALUES ('qwe', 'asd', 'root');
