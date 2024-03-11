use std::io::Write;
use std::sync::Arc;
use std::{fmt::Debug, os::raw::c_void};

use bytes::BufMut;

use {ceph::ceph as ceph_helpers, ceph::error::RadosError, std::str};

use crate::blob_store;

pub struct RadosBlobStore {
    rados: Arc<RadosWrp>,
}

impl RadosBlobStore {
    pub async fn new() -> Self {
        let user_id = "admin";
        let config_file = "/tmp/ceph/ceph.conf".to_owned();
        let pool_name = ".mgr";

        println!("Connecting to ceph");
        let cluster = ceph_helpers::connect_to_ceph(user_id, &config_file).expect("unable to to connect to the ceph");
        // let ioctx = cluster.get_rados_ioctx(cluster, ".mgr").unwrap();

        // let ioctx = cluster.get_rados_ioctx(".mgr").unwrap();
        // let rados_striper = ioctx.get_rados_striper().unwrap();

        // let mut written: u64 = 0;
        // let dummy_data = vec![0; 4 * 1024 * 1024];
        // while written < 40 * 1024 * 1024 {
        //     rados_striper.rados_object_write("test", &dummy_data, written).unwrap();
        //     written += dummy_data.len() as u64;
        // }

        Self {
            rados: Arc::new(RadosWrp::new(cluster, pool_name)),
        }
    }
}

// Rados is Sync then it should be ok to Send it. Some PullRequests add this functionality
unsafe impl Sync for RadosBlobStore {}
unsafe impl Send for RadosBlobStore {}

impl Debug for RadosBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RadosBlobStore").finish()
    }
}

#[async_trait::async_trait]
impl blob_store::BlobStore for RadosBlobStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_writer(&self, key: &str) -> Result<core::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send>>, s3s::S3Error> {
        let ioctx = self.rados.get_rados_ioctx();
        let ioctx = try_!(ioctx);
        Ok(Box::pin(RadosWriter::new(ioctx, key)))
    }
}

const STRIPE_SIZE: usize = 4 * 1024 * 1024;

struct RadosWriter {
    rados_striper: StriperWrp,
    name: String,
    buf: bytes::buf::Writer<bytes::BytesMut>,
    offset: u64,
}

impl RadosWriter {
    fn new(ioctx: ceph::ceph::IoCtx, name: &str) -> Self {
        let buf = bytes::BytesMut::with_capacity(2 * STRIPE_SIZE).writer();
        let rados_striper = ioctx.get_rados_striper().unwrap();

        Self {
            rados_striper: StriperWrp { inner: rados_striper },
            name: name.to_owned(),
            buf: buf,
            offset: 0,
        }
    }
}

impl tokio::io::AsyncWrite for RadosWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let writer = self.buf.by_ref();
        (*writer).write_all(buf)?;
        writer.flush()?;
        if writer.get_ref().len() > STRIPE_SIZE {
            // flush to the rados file
            let data = (*writer).get_mut().split().freeze();
            let offset = self.offset;
            self.rados_striper.inner.rados_object_write(&self.name, &data, offset).unwrap();
            self.offset = offset + data.len() as u64;
        }
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let writer = self.buf.by_ref();
        writer.flush()?;
        if writer.get_ref().len() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }

        let data = writer.get_mut().split().freeze();
        let offset = self.offset;
        self.rados_striper.inner.rados_object_write(&self.name, &data, offset).unwrap();
        self.offset = offset + data.len() as u64;
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}

struct RadosWrp {
    cluster: ceph_helpers::Rados,
    pool_name: String,
}

unsafe impl Sync for RadosWrp {}
unsafe impl Send for RadosWrp {}

impl RadosWrp {
    fn new(rados: ceph_helpers::Rados, pool: &str) -> Self {
        return Self {
            cluster: rados,
            pool_name: pool.to_owned(),
        };
    }

    fn get_rados_ioctx(&self) -> Result<ceph_helpers::IoCtx, RadosError> {
        self.cluster.get_rados_ioctx(&self.pool_name)
    }
}

struct StriperWrp {
    inner: ceph::ceph::RadosStriper,
}

unsafe impl Send for StriperWrp {}
