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

mod config;
mod meta_store;
mod pg_database;
mod s3_client;
mod service;

#[derive(Debug, Parser)]
#[command(version)]
struct Opt {
    #[arg(long, short, default_value = "config.yaml")]
    config: String,

    /// Opentelemetry endpoint (http://ip:port)
    #[arg(long)]
    otlp_endpoint: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();
    let config = std::sync::Arc::new(config::Settings::new(&opt.config)?);

    setup_tracing(&opt).unwrap();
    let store = RadosStore::new(config.clone()).await;

    let service = {
        let mut b = S3ServiceBuilder::new(store);

        // Enable authentication
        b.set_auth(SimpleAuth::from_single(config.auth.access_key.clone(), config.auth.secret_key.clone()));
        info!("authentication is enabled");

        // Enable parsing virtual-hosted-style requests
        if let Some(domain_name) = &config.api.domain {
            b.set_base_domain(domain_name);
            info!("virtual-hosted-style requests are enabled");
        }

        b.build()
    };

    let listener = TcpListener::bind((config.api.host.as_str(), config.api.port))?;
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

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_ids(true)
        .with_target(true)
        // .json();
        .event_format(logfmt::MyFormatter);
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let registry = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::filter::LevelFilter::INFO)
        .with(fmt_layer)
        .with(opentelemetry);
    registry.try_init()?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

mod logfmt {

    use std::fmt;
    use tracing_core::{Event, Subscriber};
    use tracing_subscriber::fmt::{
        format::{self, FormatEvent, FormatFields},
        FmtContext, FormattedFields,
    };
    use tracing_subscriber::registry::LookupSpan;

    pub struct MyFormatter;

    impl<S, N> FormatEvent<S, N> for MyFormatter
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: format::Writer<'_>, event: &Event<'_>) -> fmt::Result {
            let span = ctx.event_scope().map_or(None, |scope| scope.last());
            let metadata = event.metadata();
            write!(
                &mut writer,
                "[{}] {}: {:?} ",
                metadata.level(),
                metadata.target(),
                span.map_or("Id(None)".into(), |x| format!("{:?}", x.id())),
            )?;

            // Write fields on the event
            ctx.field_format().format_fields(writer.by_ref(), event)?;

            writeln!(writer)
        }
    }
}
