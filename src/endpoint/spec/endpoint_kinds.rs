use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::super::defaults::DEFAULT_TX_QUEUE_FRAMES;
use super::super::identity_flags::IdentityFlags;

/// Supported endpoint schemes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    Serial(SerialEndpoint),
    UdpServer(UdpServerEndpoint),
    UdpClient(UdpClientEndpoint),
    TcpServer(TcpServerEndpoint),
    TcpClient(TcpClientEndpoint),
}

impl EndpointKind {
    pub fn scheme(&self) -> &'static str {
        match self {
            EndpointKind::Serial(_) => "serial",
            EndpointKind::UdpServer(_) => "udps",
            EndpointKind::UdpClient(_) => "udpc",
            EndpointKind::TcpServer(_) => "tcps",
            EndpointKind::TcpClient(_) => "tcpc",
        }
    }

    pub fn identity(&self) -> IdentityFlags {
        match self {
            EndpointKind::Serial(e) => e.identity.clone(),
            EndpointKind::UdpServer(e) => e.identity.clone(),
            EndpointKind::UdpClient(e) => e.identity.clone(),
            EndpointKind::TcpServer(e) => e.identity.clone(),
            EndpointKind::TcpClient(e) => e.identity.clone(),
        }
    }

    pub fn tx_queue_frames(&self) -> usize {
        let common = match self {
            EndpointKind::Serial(e) => &e.common,
            EndpointKind::UdpServer(e) => &e.common,
            EndpointKind::UdpClient(e) => &e.common,
            EndpointKind::TcpServer(e) => &e.common,
            EndpointKind::TcpClient(e) => &e.common,
        };
        common.tx_queue_frames.unwrap_or(DEFAULT_TX_QUEUE_FRAMES)
    }
}

/// Plumbing knobs every endpoint type understands. Identity knobs (filters,
/// sniffer, group, learn/seq capacities) live on [`IdentityFlags`] alongside
/// the structures that consume them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommonQuery {
    pub read_buf_bytes: Option<usize>,
    pub tx_queue_frames: Option<usize>,
}

/// Hardware flow-control mode for `serial:`. The query-key name
/// `?flow_control=rtscts|none` is locked (CLAUDE.md Phase 4 contract bullet)
/// — `rtscts` over `hw` keeps the door open for adding DTR/DSR later without
/// claiming all hardware-handshake names under a single ambiguous knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SerialFlowControl {
    #[default]
    None,
    RtsCts,
}

/// `serial:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SerialEndpoint {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub serial_reopen_ms: Option<u64>,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

/// `udps:` endpoint config. `bind_addr` is fully resolved at parse time —
/// CLAUDE.md "malformed addresses are fatal" rules out hostnames here, so
/// every `udps:` reaches the spawner with a concrete `SocketAddr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpServerEndpoint {
    pub bind_addr: SocketAddr,
    pub idle_secs: Option<u64>,
    pub udps_peer_capacity: Option<usize>,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

impl Default for UdpServerEndpoint {
    fn default() -> Self {
        Self {
            // `std::net::SocketAddr` has no `Default`. Parsers always
            // overwrite this field via `parse_listen_addr`; tests can
            // override it before constructing a spec.
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            idle_secs: None,
            udps_peer_capacity: None,
            common: CommonQuery::default(),
            identity: IdentityFlags::default(),
        }
    }
}

/// `udpc:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UdpClientEndpoint {
    pub host: String,
    pub port: u16,
    pub latch_idle_secs: Option<u64>,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

/// `tcps:` endpoint config. `bind_addr` is fully resolved at parse time —
/// CLAUDE.md "malformed addresses are fatal" rules out hostnames here, so
/// every `tcps:` reaches the spawner with a concrete `SocketAddr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpServerEndpoint {
    pub bind_addr: SocketAddr,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

impl Default for TcpServerEndpoint {
    fn default() -> Self {
        Self {
            // `std::net::SocketAddr` has no `Default`. Parsers always
            // overwrite this field via `parse_listen_addr`; tests can
            // override it before constructing a spec.
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            common: CommonQuery::default(),
            identity: IdentityFlags::default(),
        }
    }
}

/// `tcpc:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TcpClientEndpoint {
    pub host: String,
    pub port: u16,
    pub reconnect_initial_ms: Option<u64>,
    pub reconnect_max_ms: Option<u64>,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::spec::EndpointSpec;

    #[test]
    fn endpoint_kind_scheme_roundtrip() {
        for input in &[
            "serial:/dev/ttyUSB0:115200",
            "udps:0.0.0.0:1",
            "udpc:1.2.3.4:1",
            "tcps:0.0.0.0:1",
            "tcpc:host:1",
        ] {
            let s = EndpointSpec::parse(input).expect("parse");
            assert!(input.starts_with(&format!("{}:", s.kind.scheme())));
        }
    }

    #[test]
    fn tx_queue_frames_returns_default_when_unset() {
        for input in &[
            "serial:/dev/ttyUSB0:115200",
            "udps:0.0.0.0:1",
            "udpc:1.2.3.4:1",
            "tcps:0.0.0.0:1",
            "tcpc:host:1",
        ] {
            let s = EndpointSpec::parse(input).expect("parse");
            assert_eq!(s.kind.tx_queue_frames(), DEFAULT_TX_QUEUE_FRAMES);
        }
    }

    #[test]
    fn tx_queue_frames_honours_override() {
        let s = EndpointSpec::parse("tcpc:host:1?tx_queue_frames=64").expect("parse");
        assert_eq!(s.kind.tx_queue_frames(), 64);
    }

    #[test]
    fn identity_carries_sniffer_flag() {
        let plain = EndpointSpec::parse("udps:0.0.0.0:1").expect("parse");
        assert!(!plain.kind.identity().sniffer);
        let sniffing = EndpointSpec::parse("udps:0.0.0.0:1?sniffer=true").expect("parse");
        assert!(sniffing.kind.identity().sniffer);
    }
}
