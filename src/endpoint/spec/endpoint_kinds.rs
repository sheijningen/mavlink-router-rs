use super::super::filters::IdentityFlags;

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

/// `udps:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UdpServerEndpoint {
    pub host: String,
    pub port: u16,
    pub idle_secs: Option<u64>,
    pub udps_peer_capacity: Option<usize>,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
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

/// `tcps:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TcpServerEndpoint {
    pub host: String,
    pub port: u16,
    pub common: CommonQuery,
    pub identity: IdentityFlags,
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
}
