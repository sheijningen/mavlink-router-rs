/// Discriminator for the supported endpoint schemes. Each variant carries the
/// fully-typed config struct for that scheme — address, scheme-specific
/// knobs, and the flattened common knobs (filters, sniffer, group, routing
/// table sizes, read/tx buffer sizes).
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

/// Single source of truth for the flattened common-knob fieldset. Each
/// scheme-specific endpoint struct invokes this macro with its own
/// address + scheme-specific fields prepended; the macro tacks on the same
/// 18 common fields so they round-trip with one definition.
macro_rules! endpoint_struct {
    (
        $(#[$attr:meta])*
        $name:ident { $($field:ident : $ty:ty,)* }
    ) => {
        $(#[$attr])*
        #[derive(Debug, Clone, PartialEq, Eq, Default)]
        pub struct $name {
            $(pub $field: $ty,)*
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
    };
}

endpoint_struct! {
    /// Fully-typed `serial:` endpoint config.
    SerialEndpoint {
        path: String,
        baud: u32,
        serial_reopen_ms: Option<u64>,
    }
}

endpoint_struct! {
    /// Fully-typed `udps:` endpoint config.
    UdpServerEndpoint {
        host: String,
        port: u16,
        idle_secs: Option<u64>,
        udps_peer_capacity: Option<usize>,
    }
}

endpoint_struct! {
    /// Fully-typed `udpc:` endpoint config.
    UdpClientEndpoint {
        host: String,
        port: u16,
        latch_idle_secs: Option<u64>,
    }
}

endpoint_struct! {
    /// Fully-typed `tcps:` endpoint config.
    TcpServerEndpoint {
        host: String,
        port: u16,
    }
}

endpoint_struct! {
    /// Fully-typed `tcpc:` endpoint config.
    TcpClientEndpoint {
        host: String,
        port: u16,
        reconnect_initial_ms: Option<u64>,
        reconnect_max_ms: Option<u64>,
    }
}
