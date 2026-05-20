use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::super::identity_flags::IdentityFlags;
use super::Scheme;

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
    /// The [`Scheme`] this endpoint belongs to, without re-matching every variant.
    pub fn scheme(&self) -> Scheme {
        match self {
            EndpointKind::Serial(_) => Scheme::Serial,
            EndpointKind::UdpServer(_) => Scheme::UdpServer,
            EndpointKind::UdpClient(_) => Scheme::UdpClient,
            EndpointKind::TcpServer(_) => Scheme::TcpServer,
            EndpointKind::TcpClient(_) => Scheme::TcpClient,
        }
    }

    /// Borrow the identity knobs (filters, sniffer, group) every variant
    /// carries on its inner struct at the same `identity` field path.
    pub fn identity(&self) -> &IdentityFlags {
        match self {
            EndpointKind::Serial(endpoint) => &endpoint.identity,
            EndpointKind::UdpServer(endpoint) => &endpoint.identity,
            EndpointKind::UdpClient(endpoint) => &endpoint.identity,
            EndpointKind::TcpServer(endpoint) => &endpoint.identity,
            EndpointKind::TcpClient(endpoint) => &endpoint.identity,
        }
    }
}

/// Plumbing knobs every endpoint type understands. Identity knobs (filters,
/// sniffer, group) live on [`IdentityFlags`] alongside the structures that
/// consume them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommonQuery {
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
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}
