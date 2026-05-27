mod client;
mod config;
mod metrics;
mod protocol;
mod server;

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use clap::Parser;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use prometheus_client::encoding::text::encode;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
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

    // Spawn file watcher. Config errors are logged and the running config is kept.
    let config_path = args.config.clone();
    let watcher_tx = config_tx.clone();
    tokio::spawn(async move {
        let (notify_tx, mut notify_rx) = tokio::sync::mpsc::channel::<()>(4);

        let mut watcher = match RecommendedWatcher::new(
            move |res: notify::Result<notify::Event>| {
                if let Ok(event) = res {
                    use notify::EventKind::*;
                    if matches!(event.kind, Modify(_) | Create(_)) {
                        let _ = notify_tx.blocking_send(());
                    }
                }
            },
            notify::Config::default(),
        ) {
            Ok(w) => w,
            Err(e) => { error!("File watcher init failed: {}", e); return; }
        };

        if let Err(e) = watcher.watch(&config_path, RecursiveMode::NonRecursive) {
            error!("File watcher start failed: {}", e);
            return;
        }

        info!("Watching config file: {}", config_path.display());

        while notify_rx.recv().await.is_some() {
            // Debounce: drain any rapid-fire events before re-reading.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            while notify_rx.try_recv().is_ok() {}

            match config::load(&config_path) {
                Ok(new_cfg) => {
                    info!("Config reloaded: node={}", new_cfg.node.name);
                    let _ = watcher_tx.send(new_cfg);
                }
                Err(e) => warn!("Config reload failed (keeping current config): {}", e),
            }
        }
    });

    // Metrics HTTP server.
    let metrics_port = initial_config.server.metrics_port;
    let metrics_for_http = metrics.clone();
    tokio::spawn(async move {
        let addr: SocketAddr = format!("0.0.0.0:{}", metrics_port).parse().unwrap();
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics_for_http);
        info!("Metrics available at http://{}/metrics", addr);
        let listener = tokio::net::TcpListener::bind(addr).await.expect("metrics bind failed");
        axum::serve(listener, app).await.expect("metrics server failed");
    });

    // Server (always runs).
    let srv_cfg = initial_config.server.clone();
    let srv_node = initial_config.node.name.clone();
    let srv_metrics = metrics.clone();
    tokio::spawn(server::run(
        srv_cfg.bind,
        srv_cfg.port,
        srv_cfg.probe_port,
        srv_cfg.recv_timeout,
        srv_node,
        srv_metrics,
    ));

    // Client manager (starts peer sessions, reacts to config changes).
    let cli_node = initial_config.node.name.clone();
    let cli_metrics = metrics.clone();
    tokio::spawn(client::run_manager(cli_node, cli_metrics, config_rx));

    // Wait for SIGTERM or Ctrl-C.
    tokio::signal::ctrl_c().await.ok();
    info!("Shutting down");
}

async fn metrics_handler(
    State(metrics): State<Arc<metrics::Metrics>>,
) -> impl IntoResponse {
    let mut buf = String::new();
    if encode(&mut buf, &metrics.registry).is_err() {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "encode error").into_response();
    }
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        buf,
    )
        .into_response()
}
