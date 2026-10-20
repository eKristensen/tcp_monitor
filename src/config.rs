//! Configuration file model, defaults, and validation.

use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub node: NodeConfig,
    pub server: ServerConfig,
    pub client: Option<ClientConfig>,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct NodeConfig {
    pub name: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ServerConfig {
    pub bind: String,
    pub heartbeat_port: u16,
    pub metrics_port: u16,
    pub probe_port: u16,
    pub heartbeat_recv_timeout: u64,
    pub probe_idle_timeout: u64,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,
    #[serde(default = "default_max_misses")]
    pub max_misses: u32,
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
}

#[derive(Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerConfig {
    pub name: String,
    pub host: String,
    #[serde(default = "default_peer_heartbeat_port")]
    pub heartbeat_port: u16,
}

fn default_peer_heartbeat_port() -> u16 {
    9700
}
fn default_heartbeat_interval() -> u64 {
    30
}
fn default_max_misses() -> u32 {
    3
}
fn default_reconnect_delay() -> u64 {
    10
}

/// Read, parse, and validate the config file at `path`.
///
/// # Errors
/// Returns a human-readable message if the file cannot be read, is not valid
/// TOML for this schema, or fails validation.
pub fn load(path: &Path) -> Result<Config, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    parse(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// Parse and validate config text.
///
/// # Errors
/// Returns a human-readable message if the text is not valid TOML for this
/// schema or fails validation.
pub fn parse(text: &str) -> Result<Config, String> {
    let cfg: Config = toml::from_str(text).map_err(|e| format!("parse: {e}"))?;
    cfg.validate()?;
    Ok(cfg)
}

impl Config {
    fn validate(&self) -> Result<(), String> {
        if self.node.name.is_empty() {
            return Err("node.name must not be empty".to_string());
        }

        let ports = [
            ("heartbeat_port", self.server.heartbeat_port),
            ("metrics_port", self.server.metrics_port),
            ("probe_port", self.server.probe_port),
        ];
        for (i, (name_a, port_a)) in ports.iter().enumerate() {
            for (name_b, port_b) in ports.iter().skip(i + 1) {
                if port_a == port_b {
                    return Err(format!(
                        "server.{name_a} and server.{name_b} must not share port {port_a}"
                    ));
                }
            }
        }

        if self.server.heartbeat_recv_timeout == 0 {
            return Err("server.heartbeat_recv_timeout must be at least 1".to_string());
        }

        if let Some(ref c) = self.client {
            if c.heartbeat_interval == 0 {
                return Err("client.heartbeat_interval must be at least 1".to_string());
            }
            if c.reconnect_delay == 0 {
                return Err("client.reconnect_delay must be at least 1".to_string());
            }
        }

        let mut seen: HashSet<&str> = HashSet::new();
        for peer in &self.peers {
            if peer.name.is_empty() {
                return Err("every [[peers]] entry must have a non-empty name".to_string());
            }
            if peer.host.is_empty() {
                return Err(format!("peer '{}' has an empty host", peer.name));
            }
            if !seen.insert(peer.name.as_str()) {
                return Err(format!("duplicate peer name: '{}'", peer.name));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SERVER_ONLY: &str = r#"
        [node]
        name = "n1"
        [server]
        bind                   = "0.0.0.0"
        heartbeat_port         = 9700
        probe_port             = 9701
        metrics_port           = 9702
        heartbeat_recv_timeout = 90
        probe_idle_timeout     = 30
    "#;

    fn with(extra: &str) -> String {
        format!("{SERVER_ONLY}\n{extra}")
    }

    fn parse_err(text: &str) -> String {
        parse(text).unwrap_err()
    }

    mod parse {
        use super::*;

        #[test]
        fn accepts_the_shipped_example_config() {
            let cfg = parse(include_str!("../config.example.toml")).unwrap();

            assert_eq!(cfg.peers.len(), 2);
        }

        #[test]
        fn accepts_server_only_config_without_client_or_peers() {
            let cfg = parse(SERVER_ONLY).unwrap();

            assert!(cfg.client.is_none() && cfg.peers.is_empty());
        }

        #[test]
        fn fills_client_defaults_for_empty_client_section() {
            let cfg = parse(&with("[client]")).unwrap();

            assert_eq!(
                cfg.client,
                Some(ClientConfig {
                    heartbeat_interval: 30,
                    max_misses: 3,
                    reconnect_delay: 10,
                })
            );
        }

        #[test]
        fn defaults_peer_heartbeat_port_to_9700() {
            let cfg = parse(&with("[[peers]]\nname = \"p\"\nhost = \"h\"")).unwrap();

            assert_eq!(cfg.peers[0].heartbeat_port, 9700);
        }

        #[test]
        fn rejects_server_section_with_missing_field() {
            let text = SERVER_ONLY.replace("probe_idle_timeout     = 30", "");

            assert!(parse_err(&text).contains("probe_idle_timeout"));
        }

        #[test]
        fn rejects_empty_node_name() {
            let text = SERVER_ONLY.replace("name = \"n1\"", "name = \"\"");

            assert!(parse_err(&text).contains("node.name"));
        }

        #[test]
        fn rejects_two_server_ports_that_collide() {
            let text = SERVER_ONLY.replace("metrics_port           = 9702", "metrics_port = 9700");

            assert!(parse_err(&text).contains("must not share port 9700"));
        }

        #[test]
        fn rejects_zero_heartbeat_recv_timeout() {
            let text =
                SERVER_ONLY.replace("heartbeat_recv_timeout = 90", "heartbeat_recv_timeout = 0");

            assert!(parse_err(&text).contains("heartbeat_recv_timeout"));
        }

        #[test]
        fn rejects_zero_client_heartbeat_interval() {
            assert!(
                parse_err(&with("[client]\nheartbeat_interval = 0")).contains("heartbeat_interval")
            );
        }

        #[test]
        fn rejects_zero_client_reconnect_delay() {
            assert!(parse_err(&with("[client]\nreconnect_delay = 0")).contains("reconnect_delay"));
        }

        #[test]
        fn rejects_peer_with_empty_name() {
            assert!(
                parse_err(&with("[[peers]]\nname = \"\"\nhost = \"h\"")).contains("non-empty name")
            );
        }

        #[test]
        fn rejects_peer_with_empty_host() {
            assert!(parse_err(&with("[[peers]]\nname = \"p\"\nhost = \"\"")).contains("empty host"));
        }

        #[test]
        fn rejects_duplicate_peer_names() {
            let peers =
                "[[peers]]\nname = \"p\"\nhost = \"a\"\n[[peers]]\nname = \"p\"\nhost = \"b\"";

            assert!(parse_err(&with(peers)).contains("duplicate peer name"));
        }

        #[test]
        fn rejects_invalid_toml() {
            assert!(parse_err("[node").starts_with("parse:"));
        }
    }
}
