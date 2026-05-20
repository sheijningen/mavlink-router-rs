pub mod client;
pub mod server;

// Max IP datagram payload plus headroom; one `recv_from` cannot return more
// than the kernel's MTU-bounded payload, but we size the buffer to the IP
// theoretical max so a fragmented giant datagram couldn't be truncated.
pub(super) const MAX_DATAGRAM_BYTES: usize = 65_536;
