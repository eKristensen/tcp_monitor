//! The `tcp-monitor` binary: parses arguments, loads the config, binds every
//! port, then runs the server, client manager, metrics endpoint, and signal
//! handling until SIGTERM or Ctrl-C.

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tcp_monitor::{client, config, metrics, server};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Config file used when `--config` is not given.
const DEFAULT_CONFIG_PATH: &str = "/etc/tcp-monitor/config.toml";

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    /// Run with the config file at this path.
    Run(PathBuf),
    /// Print usage and exit.
    Help,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL")
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config_path = match parse_args(std::env::args().skip(1)) {
        Ok(Command::Run(path)) => path,
        Ok(Command::Help) => {
            print_help();
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            error!("{} (see --help)", e);
            return ExitCode::FAILURE;
        }
    };

    let initial_config = match config::load(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config: {}", e);
            return ExitCode::FAILURE;
        }
    };

    info!("Loaded config: node={}", initial_config.node.name);

    // Bind every port before spawning anything, so a port conflict is a clean
    // startup failure rather than a panic inside a background task.
    let listeners = match server::Listeners::bind(&initial_config.server).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to start server: {}", e);
            return ExitCode::FAILURE;
        }
    };
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], initial_config.server.metrics_port));
    let metrics_listener = match tokio::net::TcpListener::bind(metrics_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind metrics_port {}: {}", metrics_addr, e);
            return ExitCode::FAILURE;
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
                    if watcher_tx.send(new_cfg).is_err() {
                        // Every receiver is gone: the process is shutting down.
                        return;
                    }
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

    let (server_result, client_result) = tokio::join!(server_handle, client_handle);
    let mut exit = ExitCode::SUCCESS;
    for (task, result) in [("server", server_result), ("client manager", client_result)] {
        if let Err(e) = result {
            error!("The {} task ended abnormally: {}", task, e);
            exit = ExitCode::FAILURE;
        }
    }
    exit
}

/// Parse the command-line arguments (without the program name).
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Command, String> {
    let command = match args.next().as_deref() {
        None => Command::Run(PathBuf::from(DEFAULT_CONFIG_PATH)),
        Some("--config") => {
            let path = args.next().ok_or("--config requires a path argument")?;
            Command::Run(PathBuf::from(path))
        }
        Some("--help" | "-h") => Command::Help,
        Some(arg) => return Err(format!("unknown argument '{arg}'")),
    };
    args.next().map_or(Ok(command), |extra| {
        Err(format!("unexpected argument '{extra}'"))
    })
}

/// Print usage for `--help`.
#[expect(
    clippy::print_stdout,
    reason = "--help output is for the terminal, not the log"
)]
fn print_help() {
    println!("Usage: tcp-monitor [--config <path>]");
    println!("  Default config path: {DEFAULT_CONFIG_PATH}");
    println!("  Log level:           LOG_LEVEL=debug (env var)");
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

/// `GET /metrics`: every metric in the Prometheus text format.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Command, String> {
        parse_args(args.iter().map(|a| (*a).to_owned()))
    }

    mod parse_args {
        use super::*;

        #[test]
        fn uses_the_default_config_path_without_arguments() {
            assert_eq!(
                parse(&[]),
                Ok(Command::Run(PathBuf::from("/etc/tcp-monitor/config.toml")))
            );
        }

        #[test]
        fn takes_the_path_after_config() {
            assert_eq!(
                parse(&["--config", "/tmp/x.toml"]),
                Ok(Command::Run(PathBuf::from("/tmp/x.toml")))
            );
        }

        #[test]
        fn rejects_config_without_a_path() {
            assert!(parse(&["--config"])
                .unwrap_err()
                .contains("requires a path"));
        }

        #[test]
        fn recognises_both_help_flags() {
            assert_eq!(
                (parse(&["-h"]), parse(&["--help"])),
                (Ok(Command::Help), Ok(Command::Help))
            );
        }

        #[test]
        fn rejects_an_unknown_argument() {
            assert!(parse(&["--verbose"])
                .unwrap_err()
                .contains("unknown argument"));
        }

        #[test]
        fn rejects_a_trailing_argument() {
            assert!(parse(&["--config", "a.toml", "b.toml"])
                .unwrap_err()
                .contains("unexpected argument"));
        }
    }
}
