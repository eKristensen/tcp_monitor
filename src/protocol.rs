//! Wire format shared by the client and server.
//!
//! All functions are generic over `AsyncRead`/`AsyncWrite` so they work on a
//! `TcpStream` in production and on an in-memory pipe in tests.

use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Heartbeat packet: 8-byte u64 seq + 8-byte f64 timestamp = 16 bytes, big-endian.
pub const PACKET_SIZE: usize = 16;

/// Longest node name, in bytes, that is sent or accepted in the handshake.
pub const MAX_HANDSHAKE_NAME_LEN: usize = 256;

/// Longest peer name kept after sanitising; longer names are truncated.
pub const MAX_PEER_NAME_CHARS: usize = 64;

/// One heartbeat packet as sent by the client and echoed by the server.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Heartbeat {
    /// Sequence number, counting up from 0 within a client session.
    pub seq: u64,
    /// Client wall-clock send time, seconds since the Unix epoch.
    pub timestamp: f64,
}

/// Encode a heartbeat packet into a fixed-size buffer.
/// Used by both the client sender and the server echo path so the format stays in sync.
#[must_use]
pub fn encode_packet(seq: u64, timestamp: f64) -> [u8; PACKET_SIZE] {
    let mut buf = [0_u8; PACKET_SIZE];
    let fields = seq
        .to_be_bytes()
        .into_iter()
        .chain(timestamp.to_bits().to_be_bytes());
    for (dst, src) in buf.iter_mut().zip(fields) {
        *dst = src;
    }
    buf
}

/// Client calls this immediately after connect to announce its node name.
/// Format: 4-byte BE u32 length + UTF-8 name bytes (max 256 bytes; longer
/// names are truncated).
///
/// # Errors
/// Returns any I/O error from writing to the stream.
pub async fn send_handshake<W>(stream: &mut W, name: &str) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let bytes = name.as_bytes();
    let bytes = bytes.get(..MAX_HANDSHAKE_NAME_LEN).unwrap_or(bytes);
    let len =
        u32::try_from(bytes.len()).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    Ok(())
}

/// Server calls this to read the client's node name from the handshake.
///
/// # Errors
/// Returns `TimedOut` if the handshake does not arrive within `timeout_secs`,
/// `InvalidData` if the length prefix is 0 or above 256, or any I/O error.
/// The caller closes the connection rather than admitting it as a session
/// with an "unknown" label.
pub async fn recv_handshake<R>(stream: &mut R, timeout_secs: u64) -> io::Result<String>
where
    R: AsyncRead + Unpin,
{
    tokio::time::timeout(Duration::from_secs(timeout_secs), read_handshake(stream))
        .await
        .map_err(|elapsed| io::Error::new(io::ErrorKind::TimedOut, elapsed))?
}

/// The handshake read itself, without the timeout.
async fn read_handshake<R>(stream: &mut R) -> io::Result<String>
where
    R: AsyncRead + Unpin,
{
    let len = usize::try_from(stream.read_u32().await?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if len == 0 || len > MAX_HANDSHAKE_NAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid handshake length",
        ));
    }
    let mut name_buf = vec![0_u8; len];
    stream.read_exact(&mut name_buf).await?;
    let raw = String::from_utf8(name_buf).unwrap_or_default();
    Ok(sanitize_peer_name(&raw))
}

/// Keep only characters that are safe in Prometheus label values and log lines.
/// Allows alphanumerics, hyphens, underscores, and dots; truncates to 64 chars.
/// Returns "unknown" if nothing is left.
#[must_use]
pub fn sanitize_peer_name(name: &str) -> String {
    let s: String = name
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(MAX_PEER_NAME_CHARS)
        .collect();
    if s.is_empty() {
        "unknown".to_owned()
    } else {
        s
    }
}

/// Send one heartbeat stamped with the current time.
/// Returns the timestamp that was sent, for RTT calculation.
///
/// # Errors
/// Returns any I/O error from writing to the stream.
pub async fn send_heartbeat<W>(stream: &mut W, seq: u64) -> io::Result<f64>
where
    W: AsyncWrite + Unpin,
{
    let ts = crate::now_f64();
    stream.write_all(&encode_packet(seq, ts)).await?;
    Ok(ts)
}

/// Read one heartbeat packet.
///
/// Not cancellation-safe across calls: if the returned future is dropped
/// mid-packet (e.g. by a timeout), the bytes already read are lost and the
/// stream is misaligned. Use a [`HeartbeatReader`] when the read can be
/// cancelled and the stream is used afterwards.
///
/// # Errors
/// Returns `UnexpectedEof` if the stream closes mid-packet, or any I/O error.
pub async fn recv_heartbeat<R>(stream: &mut R) -> io::Result<Heartbeat>
where
    R: AsyncRead + Unpin,
{
    HeartbeatReader::new().recv(stream).await
}

/// Reads heartbeat packets from one stream, cancellation-safely.
///
/// Partially received bytes live in the reader rather than in the future
/// returned by [`HeartbeatReader::recv`]. If that future is dropped — for
/// example by `tokio::time::timeout` while a packet is split across TCP
/// segments — the next call resumes where it stopped, so the stream never
/// falls out of alignment. Keep one reader per stream for its whole life.
#[derive(Debug, Default)]
pub struct HeartbeatReader {
    /// Bytes of the packet being received.
    buf: [u8; PACKET_SIZE],
    /// How many bytes of `buf` hold received data.
    filled: usize,
}

impl HeartbeatReader {
    /// A reader with no partial packet buffered.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            buf: [0; PACKET_SIZE],
            filled: 0,
        }
    }

    /// Read the next complete heartbeat packet.
    ///
    /// Cancellation-safe: dropping the future loses no data, because the
    /// only await point is `AsyncReadExt::read`, which tokio documents as
    /// cancel-safe, and progress is stored in `self` after every read.
    ///
    /// # Errors
    /// Returns `UnexpectedEof` if the stream closes, including mid-packet,
    /// or any I/O error.
    pub async fn recv<R>(&mut self, stream: &mut R) -> io::Result<Heartbeat>
    where
        R: AsyncRead + Unpin,
    {
        while self.filled < PACKET_SIZE {
            let unfilled = self
                .buf
                .get_mut(self.filled..)
                .ok_or_else(|| io::Error::other("heartbeat buffer overrun"))?;
            let read = stream.read(unfilled).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream closed mid-heartbeat",
                ));
            }
            self.filled += read;
        }
        self.filled = 0;
        Ok(decode_packet(self.buf))
    }
}

/// Inverse of [`encode_packet`]. Destructures the array instead of slicing,
/// so it cannot panic.
const fn decode_packet(buf: [u8; PACKET_SIZE]) -> Heartbeat {
    let [s0, s1, s2, s3, s4, s5, s6, s7, t0, t1, t2, t3, t4, t5, t6, t7] = buf;
    Heartbeat {
        seq: u64::from_be_bytes([s0, s1, s2, s3, s4, s5, s6, s7]),
        timestamp: f64::from_bits(u64::from_be_bytes([t0, t1, t2, t3, t4, t5, t6, t7])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    mod encode_packet {
        use super::*;

        #[test]
        fn writes_seq_then_timestamp_big_endian() {
            let packet = encode_packet(0x0102_0304_0506_0708, 1.5);

            // The wire format is a contract with peers running other versions,
            // so pin it with literal bytes. 1.5 in IEEE 754 is 0x3FF8_0000_0000_0000.
            assert_eq!(
                packet,
                [1, 2, 3, 4, 5, 6, 7, 8, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0]
            );
        }
    }

    mod heartbeat {
        use super::*;

        #[tokio::test]
        async fn round_trips_through_a_stream() {
            let (mut tx, mut rx) = duplex(64);

            let sent_ts = send_heartbeat(&mut tx, 42).await.unwrap();
            let received = recv_heartbeat(&mut rx).await.unwrap();

            assert_eq!(
                received,
                Heartbeat {
                    seq: 42,
                    timestamp: sent_ts
                }
            );
        }

        #[tokio::test]
        async fn recv_returns_unexpected_eof_on_truncated_packet() {
            let (mut tx, mut rx) = duplex(64);
            tx.write_all(&encode_packet(1, 2.0)[..10]).await.unwrap();
            drop(tx);

            let err = recv_heartbeat(&mut rx).await.unwrap_err();

            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    mod heartbeat_reader {
        use super::*;

        #[tokio::test(start_paused = true)]
        async fn resumes_a_packet_split_across_a_cancelled_read() {
            let (mut tx, mut rx) = duplex(64);
            let packet = encode_packet(9, 2.5);
            let mut reader = HeartbeatReader::new();
            tx.write_all(&packet[..10]).await.unwrap();

            // The timeout drops the read while 10 of 16 bytes have arrived.
            tokio::time::timeout(Duration::from_secs(1), reader.recv(&mut rx))
                .await
                .unwrap_err();
            tx.write_all(&packet[10..]).await.unwrap();
            let resumed = reader.recv(&mut rx).await.unwrap();

            assert_eq!(
                resumed,
                Heartbeat {
                    seq: 9,
                    timestamp: 2.5
                }
            );
        }

        #[tokio::test]
        async fn reads_back_to_back_packets_in_order() {
            let (mut tx, mut rx) = duplex(64);
            let mut both = encode_packet(1, 1.0).to_vec();
            both.extend_from_slice(&encode_packet(2, 2.0));
            tx.write_all(&both).await.unwrap();
            let mut reader = HeartbeatReader::new();

            let first = reader.recv(&mut rx).await.unwrap();
            let second = reader.recv(&mut rx).await.unwrap();

            assert_eq!((first.seq, second.seq), (1, 2));
        }

        #[tokio::test]
        async fn returns_unexpected_eof_when_stream_closes_between_packets() {
            let (tx, mut rx) = duplex(64);
            drop(tx);

            let err = HeartbeatReader::new().recv(&mut rx).await.unwrap_err();

            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    mod handshake {
        use super::*;

        async fn round_trip(name: &str) -> io::Result<String> {
            let (mut tx, mut rx) = duplex(1024);
            send_handshake(&mut tx, name).await?;
            recv_handshake(&mut rx, 1).await
        }

        async fn recv_raw(bytes: &[u8]) -> io::Result<String> {
            let (mut tx, mut rx) = duplex(1024);
            tx.write_all(bytes).await?;
            drop(tx);
            recv_handshake(&mut rx, 1).await
        }

        #[tokio::test]
        async fn round_trips_a_plain_name() {
            assert_eq!(round_trip("node-1.dc_a").await.unwrap(), "node-1.dc_a");
        }

        #[tokio::test]
        async fn send_truncates_names_longer_than_256_bytes() {
            let (mut tx, mut rx) = duplex(1024);

            send_handshake(&mut tx, &"a".repeat(300)).await.unwrap();

            assert_eq!(rx.read_u32().await.unwrap(), 256);
        }

        #[tokio::test]
        async fn recv_strips_characters_unsafe_for_labels() {
            assert_eq!(round_trip("bad\"name{x}\n").await.unwrap(), "badnamex");
        }

        #[tokio::test]
        async fn recv_truncates_sanitised_name_to_64_chars() {
            assert_eq!(round_trip(&"b".repeat(200)).await.unwrap().len(), 64);
        }

        #[tokio::test]
        async fn recv_maps_invalid_utf8_to_unknown() {
            let mut bytes = 2_u32.to_be_bytes().to_vec();
            bytes.extend_from_slice(&[0xff, 0xfe]);

            assert_eq!(recv_raw(&bytes).await.unwrap(), "unknown");
        }

        #[tokio::test]
        async fn recv_rejects_zero_length() {
            let err = recv_raw(&0_u32.to_be_bytes()).await.unwrap_err();

            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }

        #[tokio::test]
        async fn recv_rejects_length_above_256() {
            let err = recv_raw(&257_u32.to_be_bytes()).await.unwrap_err();

            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }

        #[tokio::test(start_paused = true)]
        async fn recv_times_out_when_client_sends_nothing() {
            let (_tx, mut rx) = duplex(64);

            let err = recv_handshake(&mut rx, 5).await.unwrap_err();

            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        }
    }

    mod sanitize_peer_name {
        use super::*;

        #[test]
        fn returns_unknown_when_nothing_survives() {
            assert_eq!(sanitize_peer_name("!!! ###"), "unknown");
        }

        #[test]
        fn keeps_unicode_alphanumerics() {
            assert_eq!(sanitize_peer_name("node-æøå"), "node-æøå");
        }
    }
}
