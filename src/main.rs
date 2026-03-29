mod config;
mod coordinator;
mod optimizer;
mod pattern;
mod s3;
mod supervisor;
mod worker;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::config::Config;

#[derive(Parser, Debug)]
#[command(name = "mega-obj-soaker", about = "S3 Optimized Downloader")]
struct Cli {
    /// Source S3 URI (`s3://bucket/prefix`)
    source: String,

    /// Destination local path
    destination: String,

    /// AWS region
    #[arg(long, default_value = "us-east-1")]
    region: String,

    /// Set the logging level
    #[arg(long, default_value = "INFO")]
    log_level: String,

    /// Custom S3 endpoint URL
    #[arg(long)]
    endpoint_url: Option<String>,

    /// Pattern to include files (can be used multiple times)
    #[arg(long = "include", action = clap::ArgAction::Append)]
    include: Vec<String>,

    /// Pattern to exclude files (can be used multiple times)
    #[arg(long = "exclude", action = clap::ArgAction::Append)]
    exclude: Vec<String>,
}

fn main() {
    let cli = Cli::parse();

    // Set up tracing/logging
    let filter = match cli.log_level.to_uppercase().as_str() {
        "DEBUG" => "debug",
        "WARNING" => "warn",
        "ERROR" => "error",
        _ => "info",
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(format!("mega_obj_soaker={filter}"))
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    // Parse S3 URI
    if !cli.source.starts_with("s3://") {
        tracing::error!("Source must be an S3 URI (s3://bucket/prefix)");
        std::process::exit(1);
    }

    let without_scheme = &cli.source[5..];
    let (bucket, prefix) = match without_scheme.split_once('/') {
        Some((b, p)) => (b.to_string(), p.to_string()),
        None => (without_scheme.to_string(), String::new()),
    };

    let config = Config::from_env();

    // Use rebar's RuntimeBuilder
    rebar::runtime::RuntimeBuilder::new(1)
        .thread_name("mega-obj-soaker")
        .start(move |runtime| async move {
            let client = s3::create_s3_client(
                &cli.region,
                cli.endpoint_url.as_deref(),
                config.verify_ssl,
            )
            .await;

            let objects = s3::list_objects(
                &client,
                &bucket,
                &prefix,
                &cli.include,
                &cli.exclude,
            )
            .await;

            // Set up signal handling
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.ok();
                info!("Received shutdown signal. Initiating graceful shutdown...");
            });

            supervisor::run_download(
                runtime,
                client,
                bucket,
                prefix,
                cli.destination,
                objects,
                config,
            )
            .await;

            let optimization_interval =
                std::env::var("OPTIMIZATION_INTERVAL").unwrap_or_else(|_| "10".to_string());
            info!(
                "Optimization interval set to {} seconds",
                optimization_interval
            );
        })
        .expect("Failed to start runtime");
}
