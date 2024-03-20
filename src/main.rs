use std::io::IsTerminal;
use std::net::TcpListener;

use clap::Parser;
use hyper::server::Server;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use service::RadosStore;

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{self, RandomIdGenerator, Sampler},
    Resource,
};
use std::time::Duration;
use tracing::info;
use tracing_subscriber::prelude::*;

#[macro_use]
mod error;

// mod blob_store;
// mod ceph_store;
mod s3_client;
mod meta_store;
mod pg_database;
mod service;

#[derive(Debug, Parser)]
#[command(version)]
struct Opt {
    /// Host name to listen on.
    #[arg(long, short, default_value = "/etc/ceph/ceph.conf")]
    config: String,

    #[arg(long, default_value = "localhost")]
    host: String,

    /// Port number to listen on.
    #[arg(long, default_value = "8014")] // The original design was finished on 2020-08-14.
    port: u16,

    /// Access key used for authentication.
    #[arg(long, short)]
    access_key: Option<String>,

    /// Secret key used for authentication.
    #[arg(long, short)]
    secret_key: Option<String>,

    /// Domain name used for virtual-hosted-style requests.
    #[arg(long)]
    domain_name: Option<String>,

    /// Root directory of stored data.
    #[arg(long, short)]
    pool: String,

    /// Opentelemetry endpoint (http://ip:port)
    #[arg(long)]
    otlp_endpoint: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();
    setup_tracing(&opt).unwrap();
    let store = RadosStore::new().await;

    let service = {
        let mut b = S3ServiceBuilder::new(store);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (opt.access_key, opt.secret_key) {
            b.set_auth(SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = opt.domain_name {
            b.set_base_domain(domain_name);
            info!("virtual-hosted-style requests are enabled");
        }

        b.build()
    };

    let listener = TcpListener::bind((opt.host.as_str(), opt.port))?;
    let local_addr = listener.local_addr()?;

    let server = Server::from_tcp(listener)?.serve(service.into_shared().into_make_service());

    info!("server is running at http://{local_addr}");
    server.with_graceful_shutdown(shutdown_signal()).await?;

    info!("server is stopped");
    Ok(())
}

fn setup_tracing(args: &Opt) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    if args.otlp_endpoint.is_none() {
        use tracing_subscriber::EnvFilter;

        let env_filter = EnvFilter::from_default_env();
        let enable_color = std::io::stdout().is_terminal();

        tracing_subscriber::fmt()
            .pretty()
            .with_env_filter(env_filter)
            .with_ansi(enable_color)
            .init();
        return Ok(());
    }

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&args.otlp_endpoint.clone().unwrap())
                .with_timeout(Duration::from_secs(3)),
        )
        .with_trace_config(
            trace::config()
                .with_sampler(Sampler::AlwaysOn)
                .with_id_generator(RandomIdGenerator::default())
                .with_max_events_per_span(64)
                .with_max_attributes_per_span(16)
                .with_max_events_per_span(16)
                .with_resource(Resource::new(vec![KeyValue::new("service.name", "s3s_rados")])),
        )
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let fmt_layer = tracing_subscriber::fmt::layer();
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let registry = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::filter::LevelFilter::DEBUG)
        .with(fmt_layer)
        .with(opentelemetry);
    registry.try_init()?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
