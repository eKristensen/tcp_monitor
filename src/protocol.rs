use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Heartbeat packet: 8-byte u64 seq + 8-byte f64 timestamp = 16 bytes, big-endian.
pub const PACKET_SIZE: usize = 16;

pub struct Heartbeat {
    pub seq: u64,
    pub timestamp: f64,
}

/// Encode a heartbeat packet into a fixed-size buffer.
/// Used by both the client sender and the server echo path so the format stays in sync.
pub fn encode_packet(seq: u64, timestamp: f64) -> [u8; PACKET_SIZE] {
    let mut buf = [0u8; PACKET_SIZE];
    buf[..8].copy_from_slice(&seq.to_be_bytes());
    buf[8..].copy_from_slice(&timestamp.to_bits().to_be_bytes());
    buf
}

/// Client calls this immediately after connect to announce its node name.
/// Format: 4-byte BE u32 length + UTF-8 name bytes (max 256 bytes).
pub async fn send_handshake(stream: &mut TcpStream, name: &str) -> std::io::Result<()> {
    let bytes = name.as_bytes();
    let bytes = &bytes[..bytes.len().min(256)];
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    Ok(())
}

/// Server calls this to read the client's node name from the handshake.
/// Returns an error on timeout or I/O failure so the caller can close the connection
/// rather than admitting it as a session with an "unknown" label.
pub async fn recv_handshake(stream: &mut TcpStream, timeout_secs: u64) -> std::io::Result<String> {
    tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        read_handshake(stream),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "handshake timeout"))?
}

async fn read_handshake(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > 256 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid handshake length",
        ));
    }
    let mut name_buf = vec![0u8; len];
    stream.read_exact(&mut name_buf).await?;
    let raw = String::from_utf8(name_buf).unwrap_or_default();
    Ok(sanitize_peer_name(&raw))
}

/// Keep only characters that are safe in Prometheus label values and log lines.
/// Allows alphanumerics, hyphens, underscores, and dots; truncates to 64 chars.
fn sanitize_peer_name(name: &str) -> String {
    let s: String = name
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(64)
        .collect();
    if s.is_empty() {
        "unknown".to_string()
    } else {
        s
    }
}

pub async fn send_heartbeat(stream: &mut TcpStream, seq: u64) -> std::io::Result<f64> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    stream.write_all(&encode_packet(seq, ts)).await?;
    Ok(ts)
}

pub async fn recv_heartbeat(stream: &mut TcpStream) -> std::io::Result<Heartbeat> {
    let mut buf = [0u8; PACKET_SIZE];
    stream.read_exact(&mut buf).await?;
    let seq = u64::from_be_bytes(buf[..8].try_into().unwrap());
    let ts_bits = u64::from_be_bytes(buf[8..].try_into().unwrap());
    let timestamp = f64::from_bits(ts_bits);
    Ok(Heartbeat { seq, timestamp })
}
