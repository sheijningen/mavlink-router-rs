/// Discriminator for the supported endpoint schemes. Each variant carries the
/// fully-typed config struct for that scheme — address, scheme-specific
/// knobs, and a [`CommonQuery`] substruct holding the knobs that every scheme
/// understands (filters, sniffer, group, routing table sizes, buffers).
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

/// Inclusive decimal range used inside `allow_msgid_*` and `block_msgid_*`
/// filter lists. Single values parse to `lo == hi`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsgIdRange {
    pub lo: u32,
    pub hi: u32,
}

impl MsgIdRange {
    pub fn single(n: u32) -> Self {
        Self { lo: n, hi: n }
    }

    pub fn contains(self, x: u32) -> bool {
        self.lo <= x && x <= self.hi
    }
}

/// Inclusive decimal range used inside `allow_src_sys_*`, `block_src_sys_*`,
/// `allow_src_comp_*`, `block_src_comp_*` filter lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct U8Range {
    pub lo: u8,
    pub hi: u8,
}

impl U8Range {
    pub fn single(n: u8) -> Self {
        Self { lo: n, hi: n }
    }

    pub fn contains(self, x: u8) -> bool {
        self.lo <= x && x <= self.hi
    }
}

/// Knobs that every scheme understands, embedded in each `*Endpoint` struct
/// under the `common` field. Filter lists, sniffer/group flags, routing
/// table sizes, and read/tx buffer sizes — all the fields that aren't
/// scheme-specific live here so adding a new scheme means declaring only
/// the scheme-specific fields plus `pub common: CommonQuery`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CommonQuery {
    pub sniffer: bool,
    pub group: Option<String>,
    pub learn_capacity: Option<usize>,
    pub seq_tracker_capacity: Option<usize>,
    pub read_buf_bytes: Option<usize>,
    pub tx_queue_frames: Option<usize>,
    pub allow_msgid_in: Option<Vec<MsgIdRange>>,
    pub block_msgid_in: Option<Vec<MsgIdRange>>,
    pub allow_msgid_out: Option<Vec<MsgIdRange>>,
    pub block_msgid_out: Option<Vec<MsgIdRange>>,
    pub allow_src_sys_in: Option<Vec<U8Range>>,
    pub block_src_sys_in: Option<Vec<U8Range>>,
    pub allow_src_sys_out: Option<Vec<U8Range>>,
    pub block_src_sys_out: Option<Vec<U8Range>>,
    pub allow_src_comp_in: Option<Vec<U8Range>>,
    pub block_src_comp_in: Option<Vec<U8Range>>,
    pub allow_src_comp_out: Option<Vec<U8Range>>,
    pub block_src_comp_out: Option<Vec<U8Range>>,
}

/// Fully-typed `serial:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SerialEndpoint {
    pub path: String,
    pub baud: u32,
    pub serial_reopen_ms: Option<u64>,
    pub common: CommonQuery,
}

/// Fully-typed `udps:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UdpServerEndpoint {
    pub host: String,
    pub port: u16,
    pub idle_secs: Option<u64>,
    pub udps_peer_capacity: Option<usize>,
    pub common: CommonQuery,
}

/// Fully-typed `udpc:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UdpClientEndpoint {
    pub host: String,
    pub port: u16,
    pub latch_idle_secs: Option<u64>,
    pub common: CommonQuery,
}

/// Fully-typed `tcps:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TcpServerEndpoint {
    pub host: String,
    pub port: u16,
    pub common: CommonQuery,
}

/// Fully-typed `tcpc:` endpoint config.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TcpClientEndpoint {
    pub host: String,
    pub port: u16,
    pub reconnect_initial_ms: Option<u64>,
    pub reconnect_max_ms: Option<u64>,
    pub common: CommonQuery,
}
