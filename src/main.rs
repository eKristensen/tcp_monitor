use axum::{extract::State, response::IntoResponse, routing::get, Router};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tcp_monitor::{client, config, metrics, server};
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

    // Bind every port before spawning anything, so a port conflict is a clean
    // startup failure rather than a panic inside a background task.
    let listeners = match server::Listeners::bind(&initial_config.server).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to start server: {}", e);
            std::process::exit(1);
        }
    };
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], initial_config.server.metrics_port));
    let metrics_listener = match tokio::net::TcpListener::bind(metrics_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind metrics_port {}: {}", metrics_addr, e);
            std::process::exit(1);
        }
    };

    let metrics = metrics::Metrics::new();
    let (config_tx, config_rx) = watch::channel(initial_config.clone());
    let cancel_token = CancellationToken::new();

    // SIGHUP reloads the config file and broadcasts the new version.
    // Errors are logged; the running config is kept untouched on any failure.
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

    // Metrics HTTP server.
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(Arc::clone(&metrics));
    info!("Metrics available at http://{}/metrics", metrics_addr);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(metrics_listener, app).await {
            error!("Metrics server failed: {}", e);
        }
    });

    // TCP server (always runs; listens for inbound peer connections).
    let server_handle = tokio::spawn(server::run(server::ServerArgs {
        listeners,
        config: initial_config.server.clone(),
        node_name: initial_config.node.name.clone(),
        metrics: Arc::clone(&metrics),
        cancel: cancel_token.clone(),
        config_rx: config_rx.clone(),
    }));

    // Client manager (maintains outbound peer sessions; reacts to SIGHUP reloads).
    let client_handle = tokio::spawn(client::run_manager(
        Arc::clone(&metrics),
        config_rx,
        cancel_token.clone(),
    ));

    cancel_token_signal().await;
    info!("Shutting down");

    // Activate the cancel token so the server accept loops stop.
    // Dropping config_tx causes run_manager's watch receiver to see the sender
    // gone and exit its loop, cancelling all peer tasks before returning.
    cancel_token.cancel();
    drop(config_tx);

    let _ = tokio::join!(server_handle, client_handle);
}

#[expect(
    clippy::print_stdout,
    reason = "--help output belongs on stdout, before logging matters"
)]
fn parse_config_path() -> PathBuf {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => PathBuf::from("/etc/tcp-monitor/config.toml"),
        Some("--config") => {
            if let Some(p) = args.next() {
                PathBuf::from(p)
            } else {
                eprintln!("error: --config requires a path argument");
                std::process::exit(1);
            }
        }
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
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            // Without a SIGTERM handler, fall back to Ctrl-C only.
            Err(e) => {
                error!("SIGTERM handler init failed: {}", e);
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        _ = ctrl_c => {},
        () = terminate => {},
    }
}

async fn metrics_handler(State(metrics): State<Arc<metrics::Metrics>>) -> impl IntoResponse {
    let Ok(buf) = metrics.encode() else {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "encode error",
        )
            .into_response();
    };
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
