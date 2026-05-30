use std::io;
use std::net::SocketAddr;

use socket2::{Domain, Protocol, SockRef, Socket, Type};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

// `man 2 listen`: a backlog of 1024 is the conventional ceiling honored by both
// Linux (capped to `net.core.somaxconn`, usually 4096) and Windows. RMR
// accepts as fast as it can; the backlog matters only during a brief startup
// burst where many GCSes connect simultaneously.
const TCP_LISTEN_BACKLOG: i32 = 1024;

/// Bind a UDP socket with the same options RMR uses for every UDP endpoint:
/// `SO_REUSEADDR` on, `IPV6_V6ONLY` off when binding `[::]` (dual-stack on
/// both Windows and Linux for parity — irrelevant on a specific v6
/// address, so the flip is gated on `is_unspecified`), non-blocking.
/// Returns a `tokio::net::UdpSocket` ready for async I/O.
pub fn bind_udp_dual_stack(addr: SocketAddr) -> io::Result<UdpSocket> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    if matches!(addr, SocketAddr::V6(v6) if v6.ip().is_unspecified()) {
        sock.set_only_v6(false)?;
    }

    #[cfg(windows)]
    disable_udp_conn_reset(&sock)?;

    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    UdpSocket::from_std(sock.into())
}

/// Clear `SIO_UDP_CONNRESET` so a `send_to` to a vanished peer no longer makes
/// the next `recv_from` fail with `WSAECONNRESET` — matching Linux.
#[cfg(windows)]
fn disable_udp_conn_reset(sock: &Socket) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;

    use windows_sys::Win32::Networking::WinSock::{SIO_UDP_CONNRESET, WSAIoctl};

    let disabled: u32 = 0;
    let mut bytes_returned: u32 = 0;
    let result = unsafe {
        WSAIoctl(
            sock.as_raw_socket() as usize,
            SIO_UDP_CONNRESET,
            (&raw const disabled).cast(),
            core::mem::size_of::<u32>() as u32,
            core::ptr::null_mut(),
            0,
            &raw mut bytes_returned,
            core::ptr::null_mut(),
            None,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Bind a TCP listener with the same dual-stack and reuse semantics as
/// [`bind_udp_dual_stack`]: `SO_REUSEADDR` on (rebind without TIME_WAIT
/// delay), `IPV6_V6ONLY` off only on `[::]`. `SO_REUSEPORT` intentionally
/// not set.
pub fn bind_tcp_dual_stack(addr: SocketAddr) -> io::Result<TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    if matches!(addr, SocketAddr::V6(v6) if v6.ip().is_unspecified()) {
        sock.set_only_v6(false)?;
    }
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(TCP_LISTEN_BACKLOG)?;
    TcpListener::from_std(sock.into())
}

/// Apply RMR's standard per-connection TCP options to a freshly-accepted or
/// freshly-dialed stream: `TCP_NODELAY` on (small MAVLink frames must not
/// wait on Nagle) and `SO_KEEPALIVE` on with OS-default idle/interval/probes.
/// `tokio::net::TcpStream` exposes `set_nodelay` directly; keepalive goes
/// through `socket2::SockRef`, which borrows the underlying fd/handle without
/// taking ownership.
pub fn configure_tcp_stream(stream: &TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    SockRef::from(stream).set_keepalive(true)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn bind_ipv4_loopback_any_port() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let sock = bind_udp_dual_stack(addr).expect("bind 127.0.0.1:0");
        let local = sock.local_addr().expect("local_addr");
        assert!(local.is_ipv4());
        assert_ne!(local.port(), 0);
    }

    #[tokio::test]
    async fn bind_tcp_ipv4_loopback_any_port() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = bind_tcp_dual_stack(addr).expect("bind 127.0.0.1:0");
        let local = listener.local_addr().expect("local_addr");
        assert!(local.is_ipv4());
        assert_ne!(local.port(), 0);
    }

    #[tokio::test]
    async fn bind_tcp_ipv6_unspecified_any_port() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let listener = bind_tcp_dual_stack(addr).expect("bind [::]:0");
        let local = listener.local_addr().expect("local_addr");
        assert!(local.is_ipv6());
        assert_ne!(local.port(), 0);
    }

    /// Coverage for specific v6 binds. Documents-by-example that the
    /// `IPV6_V6ONLY=0` flip is gated on `is_unspecified` — `setsockopt`
    /// of `IPV6_V6ONLY` is a no-op on an unflipped v6 socket on all three
    /// supported OSes, so this won't strictly catch a regression that
    /// re-broadens the gate, but it pins the end-to-end behaviour.
    #[tokio::test]
    async fn bind_tcp_ipv6_loopback_any_port() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
        let listener = bind_tcp_dual_stack(addr).expect("bind [::1]:0");
        let local = listener.local_addr().expect("local_addr");
        assert!(local.is_ipv6());
        assert_ne!(local.port(), 0);
    }

    /// UDP companion to the TCP `[::1]:0` test above. Same caveat about
    /// not being a strict regression guard.
    #[tokio::test]
    async fn bind_udp_ipv6_loopback_any_port() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
        let sock = bind_udp_dual_stack(addr).expect("bind [::1]:0");
        let local = sock.local_addr().expect("local_addr");
        assert!(local.is_ipv6());
        assert_ne!(local.port(), 0);
    }

    /// Verify the dual-stack TCP listener actually accepts an IPv4 connection.
    /// Without `IPV6_V6ONLY=0`, Windows would refuse this connect entirely;
    /// Linux's default also varies by sysctl. The explicit setting normalises
    /// both platforms.
    #[tokio::test]
    async fn tcp_v6_listener_accepts_v4_connection() {
        let listen_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let listener = bind_tcp_dual_stack(listen_addr).expect("bind [::]:0");
        let port = listener.local_addr().expect("local_addr").port();

        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let (connected, accepted) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(tokio::net::TcpStream::connect(target), listener.accept())
        })
        .await
        .expect("tcp v4-into-v6 accept timeout");
        let _client = connected.expect("connect");
        let (_server, _peer) = accepted.expect("accept");
    }

    #[tokio::test]
    async fn tcp_rebind_same_port_after_drop() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = bind_tcp_dual_stack(addr).expect("first bind");
        let port = listener.local_addr().expect("local_addr").port();
        drop(listener);

        let again = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let _ = bind_tcp_dual_stack(again).expect("rebind same port");
    }

    /// `configure_tcp_stream` is idempotent and must not error on a fresh
    /// loopback stream — exercises the SockRef-via-keepalive path.
    #[tokio::test]
    async fn configure_tcp_stream_succeeds_on_loopback() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = bind_tcp_dual_stack(addr).expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let (client, accepted) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(tokio::net::TcpStream::connect(target), listener.accept())
        })
        .await
        .expect("accept timeout");
        let client = client.expect("connect");
        let (server, _peer) = accepted.expect("accept");
        configure_tcp_stream(&client).expect("configure client");
        configure_tcp_stream(&server).expect("configure server");
        assert!(client.nodelay().expect("nodelay get"));
        assert!(server.nodelay().expect("nodelay get"));
    }

    #[tokio::test]
    async fn bind_ipv6_unspecified_any_port() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let sock = bind_udp_dual_stack(addr).expect("bind [::]:0");
        let local = sock.local_addr().expect("local_addr");
        assert!(local.is_ipv6());
        assert_ne!(local.port(), 0);
    }

    /// Verify the dual-stack listener actually receives an IPv4 packet. This
    /// is the whole point of forcing `IPV6_V6ONLY=0` — without it Windows would
    /// quietly reject the datagram.
    #[tokio::test]
    async fn v6_listener_accepts_v4_packet() {
        let listen_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0);
        let listener = bind_udp_dual_stack(listen_addr).expect("bind [::]:0");
        let listener_port = listener.local_addr().expect("local_addr").port();

        let sender_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let sender = bind_udp_dual_stack(sender_addr).expect("bind 127.0.0.1:0");

        let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), listener_port);
        sender.send_to(b"hello", target).await.expect("send_to");

        let mut buf = [0u8; 64];
        let (n, src) = tokio::time::timeout(Duration::from_secs(1), listener.recv_from(&mut buf))
            .await
            .expect("recv timed out")
            .expect("recv_from");
        assert_eq!(&buf[..n], b"hello");
        // Source arrives as a v4-mapped v6 address on dual-stack sockets on
        // some platforms; either form is acceptable.
        let src_v4 = match src.ip() {
            IpAddr::V4(ip) => Some(ip),
            IpAddr::V6(ip) => ip.to_ipv4_mapped(),
        };
        assert_eq!(src_v4, Some(Ipv4Addr::LOCALHOST));
    }

    #[tokio::test]
    async fn rebind_same_port_after_drop() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let sock = bind_udp_dual_stack(addr).expect("first bind");
        let port = sock.local_addr().expect("local_addr").port();
        drop(sock);

        let again = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let _ = bind_udp_dual_stack(again).expect("rebind same port");
    }

    /// Regression guard for the `SIO_UDP_CONNRESET` clear: after a `send_to` to
    /// a closed port, `recv_from` must stay pending rather than complete with
    /// the stale `WSAECONNRESET`. Windows-only.
    #[cfg(windows)]
    #[tokio::test]
    async fn udp_recv_ignores_stale_conn_reset_from_unreachable_peer() {
        let sock = bind_udp_dual_stack(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .expect("bind sender");

        // A closed loopback port: claim one by binding, then drop it.
        let dead = bind_udp_dual_stack(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .expect("bind dead");
        let dead_addr = dead.local_addr().expect("dead local_addr");
        drop(dead);

        sock.send_to(b"x", dead_addr).await.expect("send_to closed");

        let mut buf = [0u8; 64];
        let result =
            tokio::time::timeout(Duration::from_millis(200), sock.recv_from(&mut buf)).await;
        assert!(
            result.is_err(),
            "recv_from must stay pending, not return a stale WSAECONNRESET: {result:?}"
        );
    }
}
