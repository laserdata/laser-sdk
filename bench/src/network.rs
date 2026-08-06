use std::collections::BTreeMap;
use std::io;
use std::mem::size_of;

use serde::{Deserialize, Serialize};
use strum::{Display, IntoStaticStr};

const SOCK_DIAG_BY_FAMILY: u16 = 20;
const INET_DIAG_INFO: u16 = 2;
const TCP_ESTABLISHED: u8 = 1;
const NLM_F_REQUEST: u16 = 1;
const NLM_F_ROOT: u16 = 0x100;
const NLM_F_MATCH: u16 = 0x200;
const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const NO_COOKIE: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum NetworkAccountingMethod {
    LinuxSockDiagTcpInfo,
}

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum NetworkAccountingScope {
    EstablishedServerSocketsOnDeclaredPort,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct NetworkByteMeasurement {
    pub method: NetworkAccountingMethod,
    pub scope: NetworkAccountingScope,
    pub server_port: u16,
    pub connections_before: usize,
    pub connections_after: usize,
    pub client_to_server_bytes: u64,
    pub server_to_client_bytes: u64,
    pub retransmitted_bytes: u64,
    pub total_tcp_payload_bytes: u64,
    pub complete: bool,
    pub incomplete_reason: Option<String>,
}

pub struct NetworkByteProbe {
    server_port: u16,
    before: Result<NetworkSnapshot, String>,
}

impl NetworkByteProbe {
    #[must_use]
    pub fn start(server_port: u16) -> Self {
        Self {
            server_port,
            before: NetworkSnapshot::capture(server_port).map_err(|error| error.to_string()),
        }
    }

    #[must_use]
    pub fn finish(self) -> NetworkByteMeasurement {
        let before = match self.before {
            Ok(before) => before,
            Err(reason) => return NetworkByteMeasurement::incomplete(self.server_port, reason),
        };
        let after = match NetworkSnapshot::capture(self.server_port) {
            Ok(after) => after,
            Err(error) => {
                return NetworkByteMeasurement::incomplete(self.server_port, error.to_string());
            }
        };
        before.measure(&after)
    }
}

impl NetworkByteMeasurement {
    fn incomplete(server_port: u16, reason: String) -> Self {
        Self {
            method: NetworkAccountingMethod::LinuxSockDiagTcpInfo,
            scope: NetworkAccountingScope::EstablishedServerSocketsOnDeclaredPort,
            server_port,
            connections_before: 0,
            connections_after: 0,
            client_to_server_bytes: 0,
            server_to_client_bytes: 0,
            retransmitted_bytes: 0,
            total_tcp_payload_bytes: 0,
            complete: false,
            incomplete_reason: Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SocketId {
    remote_address: [u32; 4],
    remote_port: u16,
    cookie: [u32; 2],
    inode: u32,
}

#[derive(Clone, Copy, Debug)]
struct TcpCounters {
    received: u64,
    sent: u64,
    retransmitted: u64,
}

struct NetworkSnapshot {
    server_port: u16,
    sockets: BTreeMap<SocketId, TcpCounters>,
}

impl NetworkSnapshot {
    #[cfg(target_os = "linux")]
    fn capture(server_port: u16) -> Result<Self, io::Error> {
        let sockets = linux::tcp_sockets(server_port)?;
        Ok(Self {
            server_port,
            sockets,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn capture(server_port: u16) -> Result<Self, io::Error> {
        let _ = server_port;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kernel TCP byte accounting requires Linux",
        ))
    }

    fn measure(self, after: &Self) -> NetworkByteMeasurement {
        let mut received = 0_u64;
        let mut sent = 0_u64;
        let mut retransmitted = 0_u64;
        let mut reason = None;
        for (id, current) in &after.sockets {
            let previous = self.sockets.get(id).copied().unwrap_or(TcpCounters {
                received: 0,
                sent: 0,
                retransmitted: 0,
            });
            let Some(received_delta) = current.received.checked_sub(previous.received) else {
                reason = Some("a TCP receive counter moved backwards".to_owned());
                break;
            };
            let Some(sent_delta) = current.sent.checked_sub(previous.sent) else {
                reason = Some("a TCP send counter moved backwards".to_owned());
                break;
            };
            let Some(retransmitted_delta) =
                current.retransmitted.checked_sub(previous.retransmitted)
            else {
                reason = Some("a TCP retransmission counter moved backwards".to_owned());
                break;
            };
            received = received.saturating_add(received_delta);
            sent = sent.saturating_add(sent_delta);
            retransmitted = retransmitted.saturating_add(retransmitted_delta);
        }
        if reason.is_none()
            && let Some(missing) = self
                .sockets
                .keys()
                .find(|socket| !after.sockets.contains_key(socket))
        {
            reason = Some(format!(
                "TCP socket {} closed before the final counter snapshot",
                missing.inode
            ));
        }
        if reason.is_none() && after.sockets.is_empty() {
            reason = Some("no established TCP socket remained at the final snapshot".to_owned());
        }
        NetworkByteMeasurement {
            method: NetworkAccountingMethod::LinuxSockDiagTcpInfo,
            scope: NetworkAccountingScope::EstablishedServerSocketsOnDeclaredPort,
            server_port: self.server_port,
            connections_before: self.sockets.len(),
            connections_after: after.sockets.len(),
            client_to_server_bytes: received,
            server_to_client_bytes: sent,
            retransmitted_bytes: retransmitted,
            total_tcp_payload_bytes: received.saturating_add(sent),
            complete: reason.is_none(),
            incomplete_reason: reason,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct InetDiagSockId {
    source_port: u16,
    destination_port: u16,
    source_address: [u32; 4],
    destination_address: [u32; 4],
    interface: u32,
    cookie: [u32; 2],
}

#[repr(C)]
struct InetDiagRequest {
    family: u8,
    protocol: u8,
    extensions: u8,
    padding: u8,
    states: u32,
    id: InetDiagSockId,
}

#[repr(C)]
struct NetlinkRequest {
    header: libc::nlmsghdr,
    body: InetDiagRequest,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct InetDiagMessage {
    family: u8,
    state: u8,
    timer: u8,
    retransmissions: u8,
    id: InetDiagSockId,
    expires: u32,
    receive_queue: u32,
    write_queue: u32,
    user_id: u32,
    inode: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RouteAttribute {
    length: u16,
    kind: u16,
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::BTreeMap;
    use std::io;
    use std::mem::{offset_of, size_of};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use super::{
        INET_DIAG_INFO, InetDiagMessage, InetDiagRequest, InetDiagSockId, NLM_F_MATCH,
        NLM_F_REQUEST, NLM_F_ROOT, NLMSG_DONE, NLMSG_ERROR, NO_COOKIE, NetlinkRequest,
        RouteAttribute, SOCK_DIAG_BY_FAMILY, SocketId, TCP_ESTABLISHED, TcpCounters, align4,
        invalid_data, read_copy,
    };

    pub(super) fn tcp_sockets(
        server_port: u16,
    ) -> Result<BTreeMap<SocketId, TcpCounters>, io::Error> {
        let socket = open_socket()?;
        send_request(&socket)?;
        receive_sockets(&socket, server_port)
    }

    fn open_socket() -> Result<OwnedFd, io::Error> {
        // SAFETY: `socket` returns a new descriptor, and `OwnedFd` assumes its sole ownership.
        let descriptor = unsafe {
            libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
                libc::NETLINK_SOCK_DIAG,
            )
        };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful `socket` call returned an owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
    }

    fn send_request(socket: &OwnedFd) -> Result<(), io::Error> {
        let request = NetlinkRequest {
            header: libc::nlmsghdr {
                nlmsg_len: u32::try_from(size_of::<NetlinkRequest>())
                    .expect("netlink request length fits u32"),
                nlmsg_type: SOCK_DIAG_BY_FAMILY,
                nlmsg_flags: NLM_F_REQUEST | NLM_F_ROOT | NLM_F_MATCH,
                nlmsg_seq: 1,
                nlmsg_pid: 0,
            },
            body: InetDiagRequest {
                family: u8::try_from(libc::AF_INET).expect("AF_INET fits u8"),
                protocol: u8::try_from(libc::IPPROTO_TCP).expect("IPPROTO_TCP fits u8"),
                extensions: 1 << (INET_DIAG_INFO - 1),
                padding: 0,
                states: 1 << TCP_ESTABLISHED,
                id: InetDiagSockId {
                    source_port: 0,
                    destination_port: 0,
                    source_address: [0; 4],
                    destination_address: [0; 4],
                    interface: 0,
                    cookie: [NO_COOKIE; 2],
                },
            },
        };
        // SAFETY: zero is a valid kernel netlink address and all public fields are initialized below.
        let mut kernel = unsafe { std::mem::zeroed::<libc::sockaddr_nl>() };
        kernel.nl_family = libc::sa_family_t::try_from(libc::AF_NETLINK)
            .map_err(|_| invalid_data("AF_NETLINK does not fit sa_family_t"))?;
        let sent = unsafe {
            // SAFETY: both pointers reference initialized values for their declared lengths.
            libc::sendto(
                socket.as_raw_fd(),
                std::ptr::from_ref(&request).cast(),
                size_of::<NetlinkRequest>(),
                0,
                std::ptr::from_ref(&kernel).cast(),
                u32::try_from(size_of::<libc::sockaddr_nl>())
                    .expect("netlink address length fits u32"),
            )
        };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn receive_sockets(
        socket: &OwnedFd,
        server_port: u16,
    ) -> Result<BTreeMap<SocketId, TcpCounters>, io::Error> {
        let mut sockets = BTreeMap::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            // SAFETY: the mutable buffer is valid for its full declared length.
            let received = unsafe {
                libc::recv(
                    socket.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    0,
                )
            };
            if received < 0 {
                return Err(io::Error::last_os_error());
            }
            let received = usize::try_from(received).expect("non-negative recv length fits usize");
            if parse_messages(&buffer[..received], server_port, &mut sockets)? {
                return Ok(sockets);
            }
        }
    }

    fn parse_messages(
        bytes: &[u8],
        server_port: u16,
        sockets: &mut BTreeMap<SocketId, TcpCounters>,
    ) -> Result<bool, io::Error> {
        let mut offset = 0_usize;
        while offset.saturating_add(size_of::<libc::nlmsghdr>()) <= bytes.len() {
            let header = read_copy::<libc::nlmsghdr>(bytes, offset)?;
            let length = usize::try_from(header.nlmsg_len).expect("netlink length fits usize");
            if length < size_of::<libc::nlmsghdr>() || offset.saturating_add(length) > bytes.len() {
                return Err(invalid_data("malformed netlink message length"));
            }
            let payload = &bytes[offset + size_of::<libc::nlmsghdr>()..offset + length];
            match header.nlmsg_type {
                NLMSG_DONE => return Ok(true),
                NLMSG_ERROR => parse_netlink_error(payload)?,
                SOCK_DIAG_BY_FAMILY => parse_socket(payload, server_port, sockets)?,
                _ => {}
            }
            offset = offset.saturating_add(align4(length));
        }
        Ok(false)
    }

    fn parse_netlink_error(payload: &[u8]) -> Result<(), io::Error> {
        let code = read_copy::<i32>(payload, 0)?;
        if code == 0 {
            return Ok(());
        }
        Err(io::Error::from_raw_os_error(code.saturating_neg()))
    }

    fn parse_socket(
        payload: &[u8],
        server_port: u16,
        sockets: &mut BTreeMap<SocketId, TcpCounters>,
    ) -> Result<(), io::Error> {
        let message = read_copy::<InetDiagMessage>(payload, 0)?;
        if message.state != TCP_ESTABLISHED || u16::from_be(message.id.source_port) != server_port {
            return Ok(());
        }
        let mut offset = align4(size_of::<InetDiagMessage>());
        while offset.saturating_add(size_of::<RouteAttribute>()) <= payload.len() {
            let attribute = read_copy::<RouteAttribute>(payload, offset)?;
            let length = usize::from(attribute.length);
            if length < size_of::<RouteAttribute>() || offset.saturating_add(length) > payload.len()
            {
                return Err(invalid_data("malformed socket diagnostic attribute"));
            }
            if attribute.kind == INET_DIAG_INFO {
                let information = &payload[offset + size_of::<RouteAttribute>()..offset + length];
                let counters = tcp_counters(information)?;
                sockets.insert(
                    SocketId {
                        remote_address: message.id.destination_address,
                        remote_port: u16::from_be(message.id.destination_port),
                        cookie: message.id.cookie,
                        inode: message.inode,
                    },
                    counters,
                );
            }
            offset = offset.saturating_add(align4(length));
        }
        Ok(())
    }

    fn tcp_counters(bytes: &[u8]) -> Result<TcpCounters, io::Error> {
        let required =
            offset_of!(libc::tcp_info, tcpi_bytes_retrans).saturating_add(size_of::<u64>());
        if bytes.len() < required {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kernel TCP_INFO does not expose byte counters",
            ));
        }
        // SAFETY: zero is a valid initialization for this kernel data structure.
        let mut information = unsafe { std::mem::zeroed::<libc::tcp_info>() };
        let copied = bytes.len().min(size_of::<libc::tcp_info>());
        // SAFETY: both buffers are valid for `copied` bytes and do not overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                std::ptr::from_mut(&mut information).cast(),
                copied,
            );
        }
        Ok(TcpCounters {
            received: information.tcpi_bytes_received,
            sent: information.tcpi_bytes_sent,
            retransmitted: information.tcpi_bytes_retrans,
        })
    }
}

fn read_copy<T: Copy>(bytes: &[u8], offset: usize) -> Result<T, io::Error> {
    if offset.saturating_add(size_of::<T>()) > bytes.len() {
        return Err(invalid_data("truncated kernel network response"));
    }
    // SAFETY: the bounds check above proves the source contains a full `T`, and unaligned reads are permitted here.
    Ok(unsafe { bytes.as_ptr().add(offset).cast::<T>().read_unaligned() })
}

const fn align4(length: usize) -> usize {
    length.saturating_add(3) & !3
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Barrier;

    use super::*;

    #[tokio::test]
    async fn given_loopback_exchange_when_measured_then_should_report_kernel_bytes() {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("loopback listener should bind");
        let port = listener
            .local_addr()
            .expect("listener address should exist")
            .port();
        let barrier = Arc::new(Barrier::new(2));
        let client_barrier = Arc::clone(&barrier);
        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
                .await
                .expect("loopback client should connect");
            stream
                .write_all(b"request")
                .await
                .expect("request should write");
            let mut response = [0_u8; 8];
            stream
                .read_exact(&mut response)
                .await
                .expect("response should read");
            assert_eq!(&response, b"response");
            client_barrier.wait().await;
        });
        let (mut server, _) = listener.accept().await.expect("server should accept");
        let probe = NetworkByteProbe::start(port);
        let mut request = [0_u8; 7];
        server
            .read_exact(&mut request)
            .await
            .expect("request should read");
        server
            .write_all(b"response")
            .await
            .expect("response should write");
        let measurement = probe.finish();
        barrier.wait().await;
        client.await.expect("client task should finish");

        assert!(measurement.complete, "{measurement:?}");
        assert_eq!(measurement.client_to_server_bytes, 7);
        assert_eq!(measurement.server_to_client_bytes, 8);
    }
}
