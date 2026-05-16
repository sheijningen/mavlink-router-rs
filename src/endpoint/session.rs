/// Why a per-connection read/write session terminated. Shared by every
/// endpoint that runs a [`crate::endpoint::tx_queue::TxQueue`]-fed session
/// loop over an `AsyncRead + AsyncWrite` transport (`serial:`, `tcpc:`,
/// `tcps:` accepted children).
///
/// `Cancelled` and `RouterGone` are handled identically by callers — both
/// terminate the endpoint task — but kept distinct so tracing/logs can tell
/// "the cancel token fired" apart from "the router task exited and dropped
/// the frame channel" during debugging. `Disconnected` is the recoverable
/// case: the caller drains its TxQueue, then re-opens / reconnects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionOutcome {
    Cancelled,
    RouterGone,
    Disconnected,
}
