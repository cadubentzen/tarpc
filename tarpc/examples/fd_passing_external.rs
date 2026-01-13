// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Example demonstrating file descriptor passing with tarpc using a fully external
//! transport implementation.
//!
//! This example does NOT modify tarpc at all - it shows how to implement FD passing
//! as an external crate by creating a custom transport that implements `Stream + Sink`.
//!
//! # Architecture
//!
//! The key components are:
//! - `PassedFd`: A wrapper for `OwnedFd` with custom serde that uses indices
//! - `FdContext`: Thread-local storage for tracking FDs during serialization
//! - `FdUnixStream`: Async wrapper over Unix sockets with `sendmsg`/`recvmsg`
//! - `FdPassingTransport`: Implements `Stream + Sink` for tarpc compatibility
//!
//! # How FD Passing Works
//!
//! 1. During serialization, `PassedFd` pushes its FD to a thread-local vec and
//!    serializes only an index
//! 2. The transport collects these FDs and sends them via `SCM_RIGHTS`
//! 3. During deserialization, `PassedFd` reads the index and pulls the FD from
//!    the thread-local vec (populated by the transport from received ancillary data)
//!
//! Run with: cargo run --example fd_passing_external --features "serde-transport,unix"

use futures::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Read, Seek, Write};
use std::mem;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tarpc::{client, context, server::Channel};
use tokio::io::unix::AsyncFd;

// ============================================================================
// Constants
// ============================================================================

/// Maximum FDs per message (Linux SCM_MAX_FD limit)
const MAX_FDS_PER_MESSAGE: usize = 253;


// ============================================================================
// Thread-local FD context for serde
// ============================================================================

thread_local! {
    static FD_CONTEXT: RefCell<FdContext> = RefCell::new(FdContext::default());
}

/// Context for tracking file descriptors during serialization/deserialization.
#[derive(Default)]
struct FdContext {
    /// FDs collected during serialization (to be sent)
    outgoing: Vec<OwnedFd>,
    /// FDs available during deserialization (received from socket)
    incoming: VecDeque<OwnedFd>,
}

impl FdContext {
    /// Push an FD during serialization, returns its index
    fn push_outgoing(&mut self, fd: OwnedFd) -> u32 {
        let idx = self.outgoing.len() as u32;
        self.outgoing.push(fd);
        idx
    }

    /// Take all outgoing FDs after serialization
    fn take_outgoing(&mut self) -> Vec<OwnedFd> {
        mem::take(&mut self.outgoing)
    }

    /// Set incoming FDs before deserialization
    fn set_incoming(&mut self, fds: Vec<OwnedFd>) {
        self.incoming = fds.into();
    }

    /// Pop an FD during deserialization by index
    fn pop_incoming(&mut self, _index: u32) -> Option<OwnedFd> {
        // We pop in order since indices are assigned sequentially
        self.incoming.pop_front()
    }

    /// Clear any remaining state
    fn clear(&mut self) {
        self.outgoing.clear();
        self.incoming.clear();
    }
}

// ============================================================================
// PassedFd - File descriptor wrapper with custom serde
// ============================================================================

/// A file descriptor that can be passed between processes via RPC.
///
/// `PassedFd` wraps an `OwnedFd` and uses custom serialization to pass the FD
/// out-of-band via Unix socket ancillary data (`SCM_RIGHTS`).
///
/// During serialization, the FD is pushed to a thread-local context and only
/// an index is serialized. The transport layer handles sending the actual FD.
#[derive(Debug)]
pub struct PassedFd {
    fd: Option<OwnedFd>,
}

impl PassedFd {
    /// Creates a new `PassedFd` from an owned file descriptor.
    pub fn new(fd: OwnedFd) -> Self {
        Self { fd: Some(fd) }
    }

    /// Consumes the `PassedFd` and returns the underlying `OwnedFd`.
    ///
    /// # Panics
    ///
    /// Panics if the FD has already been taken or wasn't properly received.
    pub fn into_fd(mut self) -> OwnedFd {
        self.fd
            .take()
            .expect("PassedFd: FD was already taken or not properly received")
    }

    /// Returns a reference to the underlying FD if present.
    pub fn as_fd(&self) -> Option<BorrowedFd<'_>> {
        self.fd.as_ref().map(|fd| fd.as_fd())
    }
}

impl Serialize for PassedFd {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // During serialization, we need to clone the FD and push it to context
        // The original stays with us until the message is fully serialized
        let fd = self
            .fd
            .as_ref()
            .ok_or_else(|| serde::ser::Error::custom("PassedFd: no FD to serialize"))?;

        // Duplicate the FD for sending
        let fd_copy = fd.try_clone().map_err(serde::ser::Error::custom)?;

        let index = FD_CONTEXT.with(|ctx| ctx.borrow_mut().push_outgoing(fd_copy));

        index.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PassedFd {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let index = u32::deserialize(deserializer)?;

        let fd = FD_CONTEXT.with(|ctx| ctx.borrow_mut().pop_incoming(index)).ok_or_else(|| {
            serde::de::Error::custom(format!("PassedFd: no FD available for index {}", index))
        })?;

        Ok(PassedFd { fd: Some(fd) })
    }
}

// ============================================================================
// Low-level sendmsg/recvmsg with SCM_RIGHTS
// ============================================================================

/// Send data with file descriptors using sendmsg.
fn send_with_fds(socket: &StdUnixStream, data: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
    use libc::{
        CMSG_DATA, CMSG_FIRSTHDR, CMSG_LEN, CMSG_SPACE, SCM_RIGHTS, SOL_SOCKET, c_void, cmsghdr,
        iovec, msghdr, sendmsg,
    };

    if fds.len() > MAX_FDS_PER_MESSAGE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Too many FDs: {} > {}", fds.len(), MAX_FDS_PER_MESSAGE),
        ));
    }

    let iov = iovec {
        iov_base: data.as_ptr() as *mut c_void,
        iov_len: data.len(),
    };

    let fd_count = fds.len();
    let cmsg_size = if fd_count > 0 {
        unsafe { CMSG_SPACE((fd_count * mem::size_of::<RawFd>()) as u32) as usize }
    } else {
        0
    };

    let mut cmsg_buf = vec![0u8; cmsg_size];

    let mut msg: msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &iov as *const iovec as *mut iovec;
    msg.msg_iovlen = 1;

    if fd_count > 0 {
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut c_void;
        msg.msg_controllen = cmsg_size;

        unsafe {
            let cmsg: *mut cmsghdr = CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = SOL_SOCKET;
            (*cmsg).cmsg_type = SCM_RIGHTS;
            (*cmsg).cmsg_len = CMSG_LEN((fd_count * mem::size_of::<RawFd>()) as u32) as usize;

            let fd_ptr = CMSG_DATA(cmsg) as *mut RawFd;
            for (i, fd) in fds.iter().enumerate() {
                *fd_ptr.add(i) = fd.as_raw_fd();
            }
        }
    }

    let result = unsafe { sendmsg(socket.as_raw_fd(), &msg, 0) };

    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

/// Receive data with file descriptors using recvmsg.
fn recv_with_fds(socket: &StdUnixStream, buf: &mut [u8]) -> io::Result<(usize, Vec<OwnedFd>)> {
    use libc::{
        CMSG_DATA, CMSG_FIRSTHDR, CMSG_NXTHDR, CMSG_SPACE, SCM_RIGHTS, SOL_SOCKET, c_void,
        cmsghdr, iovec, msghdr, recvmsg,
    };

    let mut iov = iovec {
        iov_base: buf.as_mut_ptr() as *mut c_void,
        iov_len: buf.len(),
    };

    let cmsg_size =
        unsafe { CMSG_SPACE((MAX_FDS_PER_MESSAGE * mem::size_of::<RawFd>()) as u32) as usize };
    let mut cmsg_buf = vec![0u8; cmsg_size];

    let mut msg: msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut c_void;
    msg.msg_controllen = cmsg_size;

    let result = unsafe { recvmsg(socket.as_raw_fd(), &mut msg, 0) };

    if result < 0 {
        return Err(io::Error::last_os_error());
    }

    let bytes_read = result as usize;
    let mut fds = Vec::new();

    unsafe {
        let mut cmsg = CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == SOL_SOCKET && (*cmsg).cmsg_type == SCM_RIGHTS {
                let fd_bytes = (*cmsg).cmsg_len - mem::size_of::<cmsghdr>();
                let fd_count = fd_bytes / mem::size_of::<RawFd>();

                let fd_ptr = CMSG_DATA(cmsg) as *const RawFd;
                for i in 0..fd_count {
                    let raw_fd = *fd_ptr.add(i);
                    fds.push(OwnedFd::from_raw_fd(raw_fd));
                }
            }
            cmsg = CMSG_NXTHDR(&msg, cmsg);
        }
    }

    Ok((bytes_read, fds))
}

// ============================================================================
// FdUnixStream - Async Unix stream with FD passing
// ============================================================================

/// An async Unix stream that supports file descriptor passing.
pub struct FdUnixStream {
    inner: AsyncFd<StdUnixStream>,
}

impl FdUnixStream {
    /// Creates a new FdUnixStream from a standard UnixStream.
    pub fn new(stream: StdUnixStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            inner: AsyncFd::new(stream)?,
        })
    }

    /// Creates a connected pair of FdUnixStreams.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (a, b) = StdUnixStream::pair()?;
        Ok((Self::new(a)?, Self::new(b)?))
    }

    /// Sends data with optional file descriptors.
    pub async fn send_with_fds(&self, data: &[u8], fds: &[BorrowedFd<'_>]) -> io::Result<usize> {
        loop {
            let mut guard = self.inner.writable().await?;
            match guard.try_io(|inner| send_with_fds(inner.get_ref(), data, fds)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Receives data with optional file descriptors.
    pub async fn recv_with_fds(&self, buf: &mut [u8]) -> io::Result<(usize, Vec<OwnedFd>)> {
        loop {
            let mut guard = self.inner.readable().await?;
            match guard.try_io(|inner| recv_with_fds(inner.get_ref(), buf)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

// ============================================================================
// FdPassingTransport - Stream + Sink implementation for tarpc
// ============================================================================

/// A transport that supports file descriptor passing.
///
/// This implements `Stream` and `Sink` which is all tarpc needs for a transport.
/// Messages are serialized with bincode, and file descriptors are sent via SCM_RIGHTS.
///
/// Type parameters follow tarpc's convention:
/// - `SinkItem`: The type we send (write to the socket)
/// - `Item`: The type we receive (read from the socket)
///
/// Wire format:
/// - 4 bytes: message length (u32, little-endian)
/// - 4 bytes: FD count (u32, little-endian)
/// - N bytes: bincode-serialized message
/// - FDs are sent as ancillary data with the first chunk
pub struct FdPassingTransport<SinkItem, Item> {
    /// The underlying socket for write operations
    socket: StdUnixStream,
    /// Async handle for read operations (uses same socket via dup)
    read_async: AsyncFd<StdUnixStream>,
    /// Read buffer
    read_buf: Vec<u8>,
    /// FDs collected while reading current message
    read_fds: Vec<OwnedFd>,
    _phantom: std::marker::PhantomData<(SinkItem, Item)>,
}

const HEADER_SIZE: usize = 8; // 4 bytes message len + 4 bytes fd count

// FdPassingTransport doesn't contain any self-referential data, so it's safe to unpin
impl<SinkItem, Item> Unpin for FdPassingTransport<SinkItem, Item> {}

impl<SinkItem, Item> FdPassingTransport<SinkItem, Item> {
    /// Creates a new transport wrapping a standard UnixStream.
    pub fn new(stream: FdUnixStream) -> Self {
        // Extract the std stream
        let socket = stream.inner.into_inner();
        socket.set_nonblocking(true).expect("Failed to set nonblocking");

        // Duplicate the socket for the async read handle
        let read_socket = socket.try_clone().expect("Failed to clone socket");

        Self {
            read_async: AsyncFd::new(read_socket).expect("Failed to create AsyncFd"),
            socket,
            read_buf: Vec::new(),
            read_fds: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }
}

/// Creates a pair of connected transports for client/server communication.
///
/// Returns `(client_transport, server_transport)` where:
/// - Client sends `ClientReq` and receives `ServerResp`
/// - Server sends `ServerResp` and receives `ClientReq`
pub fn fd_transport_pair<ClientReq, ServerResp>() -> io::Result<(
    FdPassingTransport<ClientReq, ServerResp>,
    FdPassingTransport<ServerResp, ClientReq>,
)> {
    let (a, b) = FdUnixStream::pair()?;
    Ok((FdPassingTransport::new(a), FdPassingTransport::new(b)))
}

impl<SinkItem, Item> Stream for FdPassingTransport<SinkItem, Item>
where
    Item: for<'de> Deserialize<'de>,
{
    type Item = io::Result<Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Check if we have a complete message in the buffer
            if this.read_buf.len() >= HEADER_SIZE {
                let msg_len =
                    u32::from_le_bytes(this.read_buf[0..4].try_into().unwrap()) as usize;
                let fd_count =
                    u32::from_le_bytes(this.read_buf[4..8].try_into().unwrap()) as usize;
                let total_len = HEADER_SIZE + msg_len;

                if this.read_buf.len() >= total_len {
                    // We have a complete message!
                    // Extract the message bytes
                    let msg_bytes: Vec<u8> =
                        this.read_buf[HEADER_SIZE..total_len].to_vec();

                    // Remove processed data from buffer
                    this.read_buf.drain(..total_len);

                    // Verify FD count
                    if this.read_fds.len() < fd_count {
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Not enough FDs: expected {}, got {}",
                                fd_count,
                                this.read_fds.len()
                            ),
                        ))));
                    }

                    // Take the FDs for this message
                    let fds: Vec<OwnedFd> = this.read_fds.drain(..fd_count).collect();

                    // Set up FD context for deserialization
                    FD_CONTEXT.with(|ctx| {
                        let mut ctx = ctx.borrow_mut();
                        ctx.clear();
                        ctx.set_incoming(fds);
                    });

                    // Deserialize
                    let result: Result<Item, _> =
                        bincode::serde::decode_from_slice(&msg_bytes, bincode::config::standard())
                            .map(|(item, _)| item)
                            .map_err(|e| {
                                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                            });

                    // Clean up FD context
                    FD_CONTEXT.with(|ctx| ctx.borrow_mut().clear());

                    return Poll::Ready(Some(result));
                }
            }

            // Need to read more data
            let mut guard = match this.read_async.poll_read_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(guard)) => guard,
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
            };

            // Try to read
            let mut temp_buf = [0u8; 8192];
            match recv_with_fds(this.read_async.get_ref(), &mut temp_buf) {
                Ok((0, _)) => return Poll::Ready(None), // EOF
                Ok((n, fds)) => {
                    this.read_buf.extend_from_slice(&temp_buf[..n]);
                    this.read_fds.extend(fds);
                    // Continue loop to check if we have a complete message
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    guard.clear_ready();
                    return Poll::Pending;
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}

impl<SinkItem, Item> Sink<SinkItem> for FdPassingTransport<SinkItem, Item>
where
    SinkItem: Serialize,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready to accept items (we send synchronously)
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: SinkItem) -> Result<(), Self::Error> {
        let this = self.get_mut();

        // Clear FD context
        FD_CONTEXT.with(|ctx| ctx.borrow_mut().clear());

        // Serialize the item (this will push FDs to context)
        let msg_bytes = bincode::serde::encode_to_vec(&item, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        // Collect FDs from context
        let fds = FD_CONTEXT.with(|ctx| ctx.borrow_mut().take_outgoing());

        // Build the message with header
        let mut write_buf = Vec::with_capacity(HEADER_SIZE + msg_bytes.len());
        write_buf.extend_from_slice(&(msg_bytes.len() as u32).to_le_bytes());
        write_buf.extend_from_slice(&(fds.len() as u32).to_le_bytes());
        write_buf.extend_from_slice(&msg_bytes);

        // Convert to borrowed FDs for sending
        let borrowed_fds: Vec<BorrowedFd<'_>> = fds.iter().map(|fd| fd.as_fd()).collect();

        // Send synchronously (blocking if needed - acceptable for this example)
        let mut sent = 0;
        let mut fds_sent = false;

        while sent < write_buf.len() {
            let fds_to_send = if !fds_sent {
                fds_sent = true;
                &borrowed_fds[..]
            } else {
                &[]
            };

            match send_with_fds(&this.socket, &write_buf[sent..], fds_to_send) {
                Ok(n) => sent += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

// ============================================================================
// Service Definition
// ============================================================================

/// A simple file service that demonstrates FD passing.
#[tarpc::service]
pub trait FileService {
    /// Reads content from a passed file descriptor.
    async fn read_file(fd: PassedFd, max_bytes: usize) -> Vec<u8>;

    /// Server creates a memfd with some data and returns it to the client.
    async fn create_buffer(name: String, data: Vec<u8>) -> PassedFd;

    /// Echo back an FD (tests bidirectional passing).
    async fn echo_fd(fd: PassedFd) -> PassedFd;

    /// Simple method without FDs for comparison.
    async fn ping() -> String;
}

// ============================================================================
// Server Implementation
// ============================================================================

#[derive(Clone)]
struct FileServer;

impl FileService for FileServer {
    async fn read_file(self, _: context::Context, fd: PassedFd, max_bytes: usize) -> Vec<u8> {
        let owned_fd = fd.into_fd();
        let mut file = unsafe { std::fs::File::from_raw_fd(owned_fd.into_raw_fd()) };

        file.seek(std::io::SeekFrom::Start(0)).ok();
        let mut buf = vec![0u8; max_bytes];
        let n = file.read(&mut buf).unwrap_or(0);
        buf.truncate(n);
        buf
    }

    async fn create_buffer(self, _: context::Context, name: String, data: Vec<u8>) -> PassedFd {
        let memfd = create_memfd(&name).expect("Failed to create memfd");
        let mut file = unsafe { std::fs::File::from_raw_fd(memfd.into_raw_fd()) };
        file.write_all(&data).expect("Failed to write to memfd");
        file.flush().expect("Failed to flush memfd");

        PassedFd::new(file.into())
    }

    async fn echo_fd(self, _: context::Context, fd: PassedFd) -> PassedFd {
        // Just pass it right back
        fd
    }

    async fn ping(self, _: context::Context) -> String {
        "pong".to_string()
    }
}

/// Creates a memfd using the memfd_create syscall.
fn create_memfd(name: &str) -> io::Result<OwnedFd> {
    use std::ffi::CString;

    let name_cstr =
        CString::new(name).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid name"))?;

    // MFD_CLOEXEC = 0x0001
    let fd = unsafe { libc::memfd_create(name_cstr.as_ptr(), 0x0001) };

    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== tarpc FD Passing Example (External Transport) ===\n");
    println!("This example demonstrates file descriptor passing using a");
    println!("custom transport implemented entirely outside of tarpc.\n");

    // Create a connected pair of transports
    // Type annotations help the compiler understand the message types
    let (client_transport, server_transport) = fd_transport_pair::<
        tarpc::ClientMessage<FileServiceRequest>,
        tarpc::Response<FileServiceResponse>,
    >()?;

    // Spawn the server
    let server = tarpc::server::BaseChannel::with_defaults(server_transport);
    tokio::spawn(async move {
        server
            .execute(FileServer.serve())
            .for_each(|response| async move {
                tokio::spawn(response);
            })
            .await;
    });

    // Create the client
    let client = FileServiceClient::new(client::Config::default(), client_transport).spawn();

    // Test 1: Ping (no FDs)
    println!("--- Test 1: Ping (no FDs) ---");
    let response = client.ping(context::current()).await?;
    println!("Server responded: {}\n", response);

    // Test 2: Client sends FD to server
    println!("--- Test 2: Client sends FD to server ---");
    let mut tmp = tempfile::tempfile()?;
    tmp.write_all(b"Hello from the client! This data was written to a temp file.")?;
    tmp.flush()?;

    let fd: OwnedFd = tmp.into();
    let passed_fd = PassedFd::new(fd);

    let content = client
        .read_file(context::current(), passed_fd, 1024)
        .await?;
    println!(
        "Server read {} bytes from client's FD: {:?}\n",
        content.len(),
        String::from_utf8_lossy(&content)
    );

    // Test 3: Server creates FD and returns it to client
    println!("--- Test 3: Server creates FD and returns to client ---");
    let server_data = b"This buffer was created by the server!".to_vec();
    let returned_fd = client
        .create_buffer(
            context::current(),
            "server_buffer".to_string(),
            server_data.clone(),
        )
        .await?;

    // Read from the FD the server created
    let owned_fd = returned_fd.into_fd();
    let mut file = unsafe { std::fs::File::from_raw_fd(owned_fd.into_raw_fd()) };
    file.seek(std::io::SeekFrom::Start(0))?;
    let mut received = Vec::new();
    file.read_to_end(&mut received)?;

    println!(
        "Client received FD from server with data: {:?}",
        String::from_utf8_lossy(&received)
    );
    assert_eq!(received, server_data);
    println!("Data matches!\n");

    // Test 4: Bidirectional - client sends FD, server echoes it back
    println!("--- Test 4: Bidirectional FD passing (echo) ---");
    let mut echo_file = tempfile::tempfile()?;
    echo_file.write_all(b"Echo test data - round trip!")?;
    echo_file.flush()?;

    let echo_fd: OwnedFd = echo_file.into();
    let echoed = client
        .echo_fd(context::current(), PassedFd::new(echo_fd))
        .await?;

    let mut echoed_file = unsafe { std::fs::File::from_raw_fd(echoed.into_fd().into_raw_fd()) };
    echoed_file.seek(std::io::SeekFrom::Start(0))?;
    let mut echo_content = String::new();
    echoed_file.read_to_string(&mut echo_content)?;

    println!("Echoed FD contains: {:?}", echo_content);
    assert_eq!(echo_content, "Echo test data - round trip!");
    println!("Round-trip successful!\n");

    println!("=== All tests passed! ===");
    println!("\nThis example demonstrated:");
    println!("  - Custom FdPassingTransport implementing Stream + Sink");
    println!("  - PassedFd with custom serde using thread-local FD tracking");
    println!("  - sendmsg/recvmsg with SCM_RIGHTS for FD passing");
    println!("  - Client -> Server FD passing");
    println!("  - Server -> Client FD passing");
    println!("  - Bidirectional FD passing (echo)");
    println!("\nAll of this without modifying tarpc!");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transport_pair() {
        // For symmetric message types, we can create transports directly
        let (a_stream, b_stream) = FdUnixStream::pair().unwrap();
        let mut a: FdPassingTransport<String, String> = FdPassingTransport::new(a_stream);
        let mut b: FdPassingTransport<String, String> = FdPassingTransport::new(b_stream);

        // Send from a to b
        a.send("Hello".to_string()).await.unwrap();

        let received: String = b.next().await.unwrap().unwrap();
        assert_eq!(received, "Hello");
    }

    #[tokio::test]
    async fn test_fd_passing() {
        let (a_stream, b_stream) = FdUnixStream::pair().unwrap();
        let mut sender: FdPassingTransport<PassedFd, PassedFd> = FdPassingTransport::new(a_stream);
        let mut receiver: FdPassingTransport<PassedFd, PassedFd> = FdPassingTransport::new(b_stream);

        // Create a temp file
        let mut tmp = tempfile::tempfile().unwrap();
        tmp.write_all(b"Test FD content").unwrap();
        tmp.flush().unwrap();

        let fd: OwnedFd = tmp.into();
        sender.send(PassedFd::new(fd)).await.unwrap();

        let received_fd = receiver.next().await.unwrap().unwrap();

        // Verify content
        let mut file = unsafe { std::fs::File::from_raw_fd(received_fd.into_fd().into_raw_fd()) };
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();

        assert_eq!(content, "Test FD content");
    }
}
