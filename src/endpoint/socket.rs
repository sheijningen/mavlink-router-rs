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
/// both Windows and Linux for parity), non-blocking. Returns a
/// `tokio::net::UdpSocket` ready for async I/O.
pub fn bind_udp_dual_stack(addr: SocketAddr) -> io::Result<UdpSocket> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    if matches!(addr, SocketAddr::V6(_)) {
        sock.set_only_v6(false)?;
    }
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    UdpSocket::from_std(sock.into())
}

/// Bind a TCP listener with the same dual-stack and reuse semantics as
/// [`bind_udp_dual_stack`]: `SO_REUSEADDR` on (so a fresh RMR can rebind
/// without TIME_WAIT delay), `IPV6_V6ONLY` off when binding `[::]`,
/// non-blocking, ready for async accept. `SO_REUSEPORT` is intentionally not
/// set — see CLAUDE.md ("Port reuse on `tcps:` and `udps:` binds").
pub fn bind_tcp_dual_stack(addr: SocketAddr) -> io::Result<TcpListener> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    if matches!(addr, SocketAddr::V6(_)) {
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
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

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
}
