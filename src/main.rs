#![forbid(unsafe_code)]

mod client;
mod config;
mod metrics;
mod protocol;
mod server;

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use clap::Parser;
use prometheus_client::encoding::text::encode;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(about = "TCP session longevity monitor")]
struct Args {
    #[arg(long, default_value = "/etc/tcp-monitor/config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let initial_config = match config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    info!("Loaded config: node={}", initial_config.node.name);

    let metrics = metrics::Metrics::new();
    let (config_tx, config_rx) = watch::channel(initial_config.clone());

    // SIGHUP reloads the config file and broadcasts the new version.
    // Errors are logged; the running config is kept untouched on any failure.
    {
        let config_path = args.config.clone();
        let watcher_tx = config_tx.clone();
        tokio::spawn(async move {
            let mut sighup = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    error!("SIGHUP handler init failed: {}", e);
                    return;
                }
            };
            while sighup.recv().await.is_some() {
                match config::load(&config_path) {
                    Ok(new_cfg) => {
                        info!("Config reloaded (SIGHUP): node={}", new_cfg.node.name);
                        let _ = watcher_tx.send(new_cfg);
                    }
                    Err(e) => warn!("Config reload failed, keeping current config: {}", e),
                }
            }
        });
    }

    // Metrics HTTP server.
    {
        let metrics_port = initial_config.server.metrics_port;
        let metrics_for_http = metrics.clone();
        tokio::spawn(async move {
            let addr: SocketAddr = format!("0.0.0.0:{}", metrics_port)
                .parse()
                .expect("metrics addr parse failed");
            let app = Router::new()
                .route("/metrics", get(metrics_handler))
                .with_state(metrics_for_http);
            info!("Metrics available at http://{}/metrics", addr);
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .expect("metrics bind failed");
            axum::serve(listener, app)
                .await
                .expect("metrics server failed");
        });
    }

    // TCP server (always runs; listens for inbound peer connections).
    {
        let srv = initial_config.server.clone();
        let node = initial_config.node.name.clone();
        let m = metrics.clone();
        tokio::spawn(server::run(srv.bind, srv.port, srv.probe_port, srv.recv_timeout, node, m));
    }

    // Client manager (maintains outbound peer sessions; reacts to SIGHUP reloads).
    {
        let node = initial_config.node.name.clone();
        let m = metrics.clone();
        tokio::spawn(client::run_manager(node, m, config_rx));
    }

    shutdown_signal().await;
    info!("Shutting down");
}

/// Resolves on SIGTERM or Ctrl-C, whichever comes first.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let terminate = async {
        signal(SignalKind::terminate())
            .expect("SIGTERM handler failed")
            .recv()
            .await;
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn metrics_handler(State(metrics): State<Arc<metrics::Metrics>>) -> impl IntoResponse {
    let mut buf = String::new();
    if encode(&mut buf, &metrics.registry).is_err() {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "encode error").into_response();
    }
    (
        axum::http::StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}
