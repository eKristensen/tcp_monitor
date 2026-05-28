#![forbid(unsafe_code)]

mod client;
mod config;
mod metrics;
mod protocol;
mod server;

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use prometheus_client::encoding::text::encode;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config_path = parse_config_path();

    let initial_config = match config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    info!("Loaded config: node={}", initial_config.node.name);

    let metrics = metrics::Metrics::new();
    let (config_tx, config_rx) = watch::channel(initial_config.clone());
    let cancel_token = CancellationToken::new();

    // SIGHUP reloads the config file and broadcasts the new version.
    // Errors are logged; the running config is kept untouched on any failure.
    {
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
    let server_handle = {
        let srv = initial_config.server.clone();
        tokio::spawn(server::run(server::ServerArgs {
            bind: srv.bind,
            port: srv.port,
            probe_port: srv.probe_port,
            heartbeat_recv_timeout: srv.heartbeat_recv_timeout,
            probe_idle_timeout: srv.probe_idle_timeout,
            node_name: initial_config.node.name.clone(),
            metrics: metrics.clone(),
            cancel: cancel_token.clone(),
            config_rx: config_rx.clone(),
        }))
    };

    // Client manager (maintains outbound peer sessions; reacts to SIGHUP reloads).
    let client_handle = tokio::spawn(client::run_manager(metrics.clone(), config_rx));

    cancel_token_signal().await;
    info!("Shutting down");

    // Activate the cancel token so the server accept loops stop.
    // Dropping config_tx causes run_manager's watch receiver to see the sender
    // gone and exit its loop, cancelling all peer tasks before returning.
    cancel_token.cancel();
    drop(config_tx);

    let _ = tokio::join!(server_handle, client_handle);
}

fn parse_config_path() -> PathBuf {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => PathBuf::from("/etc/tcp-monitor/config.toml"),
        Some("--config") => match args.next() {
            Some(p) => PathBuf::from(p),
            None => {
                eprintln!("error: --config requires a path argument");
                std::process::exit(1);
            }
        },
        Some("--help" | "-h") => {
            println!("Usage: tcp-monitor [--config <path>]");
            println!("  Default config path: /etc/tcp-monitor/config.toml");
            println!("  Log level:           LOG_LEVEL=debug (env var)");
            std::process::exit(0);
        }
        Some(arg) => {
            eprintln!("error: unknown argument '{arg}'");
            std::process::exit(1);
        }
    }
}

/// Resolves on SIGTERM or Ctrl-C, whichever comes first.
async fn cancel_token_signal() {
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
