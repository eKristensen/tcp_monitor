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
    pub port: u16,
    pub metrics_port: u16,
    pub probe_port: u16,
    pub heartbeat_recv_timeout: u64,
    pub probe_idle_timeout: u64,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
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
    #[serde(default = "default_peer_port")]
    pub port: u16,
}

fn default_peer_port() -> u16 {
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
