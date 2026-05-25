use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use super::super::identity_flags::IdentityFlags;
use super::Scheme;
use super::parse::sanitize_for_name;

/// Supported endpoint schemes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointKind {
    Serial(SerialEndpoint),
    UdpServer(UdpServerEndpoint),
    UdpClient(UdpClientEndpoint),
    TcpServer(TcpServerEndpoint),
    TcpClient(TcpClientEndpoint),
}

impl fmt::Display for EndpointKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EndpointKind::Serial(endpoint) => fmt::Display::fmt(endpoint, formatter),
            EndpointKind::UdpServer(endpoint) => fmt::Display::fmt(endpoint, formatter),
            EndpointKind::UdpClient(endpoint) => fmt::Display::fmt(endpoint, formatter),
            EndpointKind::TcpServer(endpoint) => fmt::Display::fmt(endpoint, formatter),
            EndpointKind::TcpClient(endpoint) => fmt::Display::fmt(endpoint, formatter),
        }
    }
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

    /// Derive a `scheme-addr-port` name for use when `#name` is omitted from
    /// the endpoint spec. Characters disallowed by the name regex
    /// (`[A-Za-z0-9_-]`) are replaced with `_` so the auto-name satisfies the
    /// same validation as explicit names.
    pub fn default_name(&self) -> String {
        match self {
            EndpointKind::Serial(endpoint) => {
                format!(
                    "serial-{}-{}",
                    sanitize_for_name(&endpoint.path),
                    endpoint.baud
                )
            }
            EndpointKind::UdpServer(endpoint) => format!(
                "udps-{}-{}",
                sanitize_for_name(&endpoint.bind_addr.ip().to_string()),
                endpoint.bind_addr.port()
            ),
            EndpointKind::UdpClient(endpoint) => format!(
                "udpc-{}-{}",
                sanitize_for_name(&endpoint.host),
                endpoint.port
            ),
            EndpointKind::TcpServer(endpoint) => format!(
                "tcps-{}-{}",
                sanitize_for_name(&endpoint.bind_addr.ip().to_string()),
                endpoint.bind_addr.port()
            ),
            EndpointKind::TcpClient(endpoint) => format!(
                "tcpc-{}-{}",
                sanitize_for_name(&endpoint.host),
                endpoint.port
            ),
        }
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SerialEndpoint {
    pub path: String,
    pub baud: u32,
    pub flow_control: SerialFlowControl,
    pub identity: IdentityFlags,
}

impl fmt::Display for SerialEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("SerialEndpoint");
        entry.field("path", &self.path);
        entry.field("baud", &self.baud);
        if self.flow_control != SerialFlowControl::default() {
            entry.field("flow_control", &self.flow_control);
        }
        fmt_identity(&mut entry, &self.identity);
        entry.finish()
    }
}

/// `udps:` endpoint config. `bind_addr` is fully resolved at parse time —
/// CLAUDE.md "malformed addresses are fatal" rules out hostnames here, so
/// every `udps:` reaches the spawner with a concrete `SocketAddr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpServerEndpoint {
    pub bind_addr: SocketAddr,
    pub idle_secs: Option<u64>,
    pub identity: IdentityFlags,
}

impl fmt::Display for UdpServerEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("UdpServerEndpoint");
        entry.field("bind_addr", &self.bind_addr);
        if let Some(value) = self.idle_secs {
            entry.field("idle_secs", &value);
        }
        fmt_identity(&mut entry, &self.identity);
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
    pub identity: IdentityFlags,
}

impl fmt::Display for UdpClientEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("UdpClientEndpoint");
        entry.field("host", &self.host);
        entry.field("port", &self.port);
        if let Some(value) = self.latch_idle_secs {
            entry.field("latch_idle_secs", &value);
        }
        fmt_identity(&mut entry, &self.identity);
        entry.finish()
    }
}

/// `tcps:` endpoint config. `bind_addr` is fully resolved at parse time —
/// CLAUDE.md "malformed addresses are fatal" rules out hostnames here, so
/// every `tcps:` reaches the spawner with a concrete `SocketAddr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpServerEndpoint {
    pub bind_addr: SocketAddr,
    pub identity: IdentityFlags,
}

impl fmt::Display for TcpServerEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("TcpServerEndpoint");
        entry.field("bind_addr", &self.bind_addr);
        fmt_identity(&mut entry, &self.identity);
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
            identity: IdentityFlags::default(),
        }
    }
}

/// `tcpc:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TcpClientEndpoint {
    pub host: String,
    pub port: u16,
    pub identity: IdentityFlags,
}

impl fmt::Display for TcpClientEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entry = formatter.debug_struct("TcpClientEndpoint");
        entry.field("host", &self.host);
        entry.field("port", &self.port);
        fmt_identity(&mut entry, &self.identity);
        entry.finish()
    }
}

fn fmt_identity(entry: &mut fmt::DebugStruct<'_, '_>, identity: &IdentityFlags) {
    if *identity != IdentityFlags::default() {
        entry.field("identity", identity);
    }
}
