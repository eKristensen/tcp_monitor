use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub node: NodeConfig,
    #[serde(default)]
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
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    #[serde(default = "default_probe_port")]
    pub probe_port: u16,
    /// Seconds with no heartbeat before a server session is declared timed out.
    #[serde(default = "default_recv_timeout")]
    pub recv_timeout: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind: default_bind(),
            port: default_port(),
            metrics_port: default_metrics_port(),
            probe_port: default_probe_port(),
            recv_timeout: default_recv_timeout(),
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
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
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_bind() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    9700
}
fn default_metrics_port() -> u16 {
    9701
}
fn default_probe_port() -> u16 {
    9702
}
fn default_recv_timeout() -> u64 {
    90
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

pub fn load(path: &Path) -> Result<Config, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read {}: {}", path.display(), e))?;
    let cfg: Config =
        toml::from_str(&text).map_err(|e| format!("parse {}: {}", path.display(), e))?;
    cfg.validate()?;
    Ok(cfg)
}

impl Config {
    fn validate(&self) -> Result<(), String> {
        if self.node.name.is_empty() {
            return Err("node.name must not be empty".to_string());
        }

        // Ports on the server must be distinct.
        let ports = [
            ("port", self.server.port),
            ("metrics_port", self.server.metrics_port),
            ("probe_port", self.server.probe_port),
        ];
        for i in 0..ports.len() {
            for j in (i + 1)..ports.len() {
                if ports[i].1 == ports[j].1 {
                    return Err(format!(
                        "server.{} and server.{} must not share port {}",
                        ports[i].0,
                        ports[j].0,
                        ports[i].1
                    ));
                }
            }
        }

        // Each peer must have a non-empty name and host; names must be unique.
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
