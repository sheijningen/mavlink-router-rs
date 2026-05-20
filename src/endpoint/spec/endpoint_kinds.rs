use std::fmt;
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
#[derive(Clone, PartialEq, Eq, Default)]
pub struct CommonQuery {
    pub tx_queue_frames: Option<usize>,
}

impl fmt::Debug for CommonQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("CommonQuery");
        if let Some(value) = self.tx_queue_frames {
            entry.field("tx_queue_frames", &value);
        }
        entry.finish()
    }
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
#[derive(Clone, PartialEq, Eq, Default)]
pub struct SerialEndpoint {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

impl fmt::Debug for SerialEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("SerialEndpoint");
        entry.field("path", &self.path);
        entry.field("baud", &self.baud);
        if self.flow_control != SerialFlowControl::default() {
            entry.field("flow_control", &self.flow_control);
        }
        debug_common_identity(&mut entry, &self.common, &self.identity);
        entry.finish()
    }
}

/// `udps:` endpoint config. `bind_addr` is fully resolved at parse time —
/// CLAUDE.md "malformed addresses are fatal" rules out hostnames here, so
/// every `udps:` reaches the spawner with a concrete `SocketAddr`.
#[derive(Clone, PartialEq, Eq)]
pub struct UdpServerEndpoint {
    pub bind_addr: SocketAddr,
    pub idle_secs: Option<u64>,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

impl fmt::Debug for UdpServerEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("UdpServerEndpoint");
        entry.field("bind_addr", &self.bind_addr);
        if let Some(value) = self.idle_secs {
            entry.field("idle_secs", &value);
        }
        debug_common_identity(&mut entry, &self.common, &self.identity);
        entry.finish()
    }
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
#[derive(Clone, PartialEq, Eq, Default)]
pub struct UdpClientEndpoint {
    pub host: String,
    pub port: u16,
    pub latch_idle_secs: Option<u64>,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

impl fmt::Debug for UdpClientEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("UdpClientEndpoint");
        entry.field("host", &self.host);
        entry.field("port", &self.port);
        if let Some(value) = self.latch_idle_secs {
            entry.field("latch_idle_secs", &value);
        }
        debug_common_identity(&mut entry, &self.common, &self.identity);
        entry.finish()
    }
}

/// `tcps:` endpoint config. `bind_addr` is fully resolved at parse time —
/// CLAUDE.md "malformed addresses are fatal" rules out hostnames here, so
/// every `tcps:` reaches the spawner with a concrete `SocketAddr`.
#[derive(Clone, PartialEq, Eq)]
pub struct TcpServerEndpoint {
    pub bind_addr: SocketAddr,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

impl fmt::Debug for TcpServerEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("TcpServerEndpoint");
        entry.field("bind_addr", &self.bind_addr);
        debug_common_identity(&mut entry, &self.common, &self.identity);
        entry.finish()
    }
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
#[derive(Clone, PartialEq, Eq, Default)]
pub struct TcpClientEndpoint {
    pub host: String,
    pub port: u16,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
}

impl fmt::Debug for TcpClientEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("TcpClientEndpoint");
        entry.field("host", &self.host);
        entry.field("port", &self.port);
        debug_common_identity(&mut entry, &self.common, &self.identity);
        entry.finish()
    }
}

fn debug_common_identity(
    entry: &mut fmt::DebugStruct<'_, '_>,
    common: &CommonQuery,
    identity: &IdentityFlags,
) {
    if *common != CommonQuery::default() {
        entry.field("common", common);
    }
    if *identity != IdentityFlags::default() {
        entry.field("identity", identity);
    }
}
