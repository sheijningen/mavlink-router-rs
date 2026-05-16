use std::io;
use std::net::SocketAddr;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

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
