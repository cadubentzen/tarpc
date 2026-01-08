# File Descriptor Passing Support for tarpc

## Overview

This document outlines the design for adding Linux file descriptor (FD) passing support to tarpc, enabling zero-copy transfer of DMABUFs and other kernel objects between processes.

## Background

### Unix Domain Socket FD Passing

Linux allows passing file descriptors between processes via Unix domain sockets using the `SCM_RIGHTS` mechanism:

1. **Sender**: Uses `sendmsg()` with control message containing FDs
2. **Kernel**: Duplicates FDs into receiver's file descriptor table
3. **Receiver**: Uses `recvmsg()` to receive data + control message with new FD numbers

Key properties:
- FDs are NOT serialized - they're transferred via kernel mechanism
- Received FDs are NEW numbers (kernel allocates in receiver's FD table)
- Only works over Unix domain sockets (not TCP)
- Maximum ~253 FDs per message (SCM_MAX_FD)

### Use Case: DMABUF Sharing

For zero-copy video encoding with DMABUFs:
1. Capture process creates DMABUF (gets FD)
2. Passes FD to encoder process via tarpc RPC
3. Encoder imports DMABUF via received FD
4. Zero-copy achieved - no data copying between processes

## Design Goals

1. **Type Safety**: Compile-time guarantees for FD handling
2. **Ergonomic API**: Natural integration with tarpc's service macro
3. **Zero-Copy**: FDs passed without serialization overhead
4. **Resource Safety**: Proper FD lifecycle management (ownership, closing)
5. **Backward Compatibility**: Optional feature, doesn't break existing code
6. **Platform Specificity**: Linux-only, graceful degradation elsewhere

## Architecture

### Layer 1: Low-Level FD Transport (`fd_transport.rs`)

A wrapper around `UnixStream` that handles `sendmsg`/`recvmsg` with ancillary data.

```rust
// New module: tarpc/src/fd_transport.rs

use std::os::unix::io::{OwnedFd, BorrowedFd, AsRawFd, FromRawFd};

/// A message with optional file descriptors
pub struct FdMessage {
    /// The serialized data payload
    pub data: Bytes,
    /// File descriptors passed with this message
    pub fds: Vec<OwnedFd>,
}

/// Unix stream wrapper supporting FD passing
pub struct FdUnixStream {
    inner: UnixStream,
}

impl FdUnixStream {
    /// Send data with optional file descriptors
    pub async fn send_with_fds(
        &self,
        data: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> io::Result<()>;

    /// Receive data and any passed file descriptors
    pub async fn recv_with_fds(&self) -> io::Result<FdMessage>;
}
```

### Layer 2: FD-Aware Codec (`fd_codec.rs`)

Extends the framing layer to synchronize FD passing with length-delimited messages.

```rust
// New module: tarpc/src/fd_codec.rs

/// Codec that handles length-delimited messages with FD passing
pub struct FdLengthDelimitedCodec {
    inner: LengthDelimitedCodec,
}

/// A framed message that may contain file descriptors
pub struct FramedFdMessage<T> {
    pub message: T,
    pub fds: Vec<OwnedFd>,
}
```

### Layer 3: FD-Enabled Serde Transport (`serde_transport.rs` extension)

Extends the existing serde transport to optionally carry FDs.

```rust
// Extension to existing serde_transport.rs

pub mod unix_fd {
    /// Transport supporting FD passing
    pub struct FdTransport<Item, SinkItem, Codec> {
        // Wraps FdUnixStream instead of regular UnixStream
        inner: FdFramed<FdUnixStream, Codec>,
        _phantom: PhantomData<(Item, SinkItem)>,
    }

    pub async fn connect<A, C, Req, Resp>(
        addr: A,
        codec: C,
    ) -> io::Result<FdTransport<...>>;

    pub async fn listen<A, C>(
        addr: A,
        codec: C,
    ) -> io::Result<Incoming<...>>;
}
```

### Layer 4: FD Wrapper Types (`fd.rs`)

User-facing types for including FDs in RPC messages.

```rust
// New module: tarpc/src/fd.rs

/// Wrapper for file descriptors in RPC messages
///
/// When serialized, only a placeholder is written.
/// The actual FD is passed via SCM_RIGHTS.
#[derive(Debug)]
pub struct PassedFd {
    fd: OwnedFd,
    // Index in the FD array for this message
    index: u32,
}

impl PassedFd {
    /// Create from an owned file descriptor
    pub fn new(fd: OwnedFd) -> Self;

    /// Take ownership of the file descriptor
    pub fn into_fd(self) -> OwnedFd;

    /// Borrow the file descriptor
    pub fn as_fd(&self) -> BorrowedFd<'_>;
}

// Custom serialization that only writes the index
impl Serialize for PassedFd { ... }
impl<'de> Deserialize<'de> for PassedFd { ... }

/// Marker trait for types containing PassedFd
pub trait ContainsFds {
    /// Extract all FDs for sending
    fn extract_fds(&mut self) -> Vec<OwnedFd>;

    /// Inject received FDs
    fn inject_fds(&mut self, fds: Vec<OwnedFd>);
}
```

### Layer 5: Service Macro Integration

Extend the `#[tarpc::service]` macro to handle FD-containing types.

```rust
// Usage example

#[tarpc::service]
trait DmaBufService {
    /// Import a DMABUF from another process
    async fn import_dmabuf(fd: PassedFd, metadata: DmaBufMetadata) -> ImportResult;

    /// Export a DMABUF to another process
    async fn export_dmabuf(name: String) -> (PassedFd, DmaBufMetadata);
}
```

## Implementation Details

### FD Passing Wire Protocol

Since FDs are passed out-of-band via SCM_RIGHTS, we need to synchronize:

```
Message Frame:
┌─────────────────────────────────────────────────────┐
│ 4 bytes: total length                               │
├─────────────────────────────────────────────────────┤
│ 4 bytes: FD count                                   │
├─────────────────────────────────────────────────────┤
│ N bytes: serialized message (with FD index markers) │
└─────────────────────────────────────────────────────┘

Ancillary Data (via SCM_RIGHTS):
┌─────────────────────────────────────────────────────┐
│ FD₀, FD₁, ..., FDₙ                                 │
└─────────────────────────────────────────────────────┘
```

### Serialization Strategy

`PassedFd` serializes to just an index (u32):

```rust
impl Serialize for PassedFd {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Only serialize the index, not the actual FD
        self.index.serialize(serializer)
    }
}
```

Before sending, the transport:
1. Collects all `PassedFd` instances from the message
2. Assigns sequential indices (0, 1, 2, ...)
3. Serializes the message (FDs become indices)
4. Sends data + FDs via `sendmsg`

On receive:
1. Receives data + FDs via `recvmsg`
2. Deserializes message (gets indices)
3. Replaces indices with received FDs

### Async sendmsg/recvmsg

Using `tokio::io::unix::AsyncFd` for async operations:

```rust
impl FdUnixStream {
    pub async fn send_with_fds(
        &self,
        data: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> io::Result<()> {
        loop {
            let guard = self.async_fd.writable().await?;
            match guard.try_io(|inner| {
                send_with_fds_sync(inner.get_ref(), data, fds)
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

fn send_with_fds_sync(
    socket: &std::os::unix::net::UnixStream,
    data: &[u8],
    fds: &[BorrowedFd<'_>],
) -> io::Result<()> {
    use std::io::IoSlice;
    use libc::{msghdr, cmsghdr, CMSG_SPACE, CMSG_DATA, SCM_RIGHTS, SOL_SOCKET};

    let iov = [IoSlice::new(data)];

    // Build control message for FDs
    let fd_count = fds.len();
    let cmsg_space = unsafe { CMSG_SPACE((fd_count * size_of::<RawFd>()) as u32) };
    let mut cmsg_buf = vec![0u8; cmsg_space as usize];

    // ... setup msghdr and sendmsg
}
```

### FD Lifecycle Management

```rust
/// FD passes through these states:
///
/// Sender side:
/// 1. User creates OwnedFd
/// 2. Wraps in PassedFd (takes ownership)
/// 3. Transport extracts FDs before sending
/// 4. sendmsg passes FDs to kernel
/// 5. Original FDs closed after send (kernel duplicated them)
///
/// Receiver side:
/// 1. recvmsg receives new FD numbers
/// 2. Transport creates OwnedFd wrappers
/// 3. Injects into deserialized message
/// 4. User receives PassedFd with valid FD
/// 5. User takes ownership via into_fd()
```

### Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum FdError {
    #[error("FD passing not supported on this transport")]
    NotSupported,

    #[error("Too many file descriptors ({count}, max {max})")]
    TooManyFds { count: usize, max: usize },

    #[error("FD count mismatch: expected {expected}, got {actual}")]
    FdCountMismatch { expected: usize, actual: usize },

    #[error("IO error during FD passing: {0}")]
    Io(#[from] io::Error),
}
```

## File Structure

```
tarpc/src/
├── lib.rs                    # Add fd module export
├── fd.rs                     # NEW: PassedFd, ContainsFds trait
├── fd_transport.rs           # NEW: FdUnixStream, low-level FD passing
├── fd_codec.rs               # NEW: FD-aware framing codec
├── serde_transport.rs        # MODIFY: Add unix_fd submodule
│   └── (unix_fd module)      # NEW: FD-enabled transport
└── ...
```

## Feature Flags

```toml
[features]
# Existing
unix = []

# New feature for FD passing
fd-passing = ["unix"]  # Implies unix feature

# Full feature includes fd-passing on Linux
full = ["...", "fd-passing"]
```

## API Examples

### Basic Usage

```rust
use tarpc::fd::PassedFd;
use std::os::unix::io::OwnedFd;

#[tarpc::service]
trait VideoEncoder {
    async fn encode_dmabuf(
        dmabuf: PassedFd,
        width: u32,
        height: u32,
        format: u32,
    ) -> Result<PassedFd, EncodeError>;
}

// Server implementation
#[derive(Clone)]
struct EncoderServer;

impl VideoEncoder for EncoderServer {
    async fn encode_dmabuf(
        self,
        _ctx: tarpc::context::Context,
        dmabuf: PassedFd,
        width: u32,
        height: u32,
        format: u32,
    ) -> Result<PassedFd, EncodeError> {
        // Import the DMABUF
        let input_fd = dmabuf.into_fd();

        // ... do encoding ...

        // Return encoded buffer
        Ok(PassedFd::new(output_fd))
    }
}

// Client usage
async fn encode_frame(client: &VideoEncoderClient, dmabuf: OwnedFd) {
    let result = client.encode_dmabuf(
        tarpc::context::current(),
        PassedFd::new(dmabuf),
        1920, 1080, DRM_FORMAT_NV12,
    ).await?;

    let encoded_fd = result.into_fd();
}
```

### Server Setup

```rust
use tarpc::serde_transport::unix_fd;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = unix_fd::listen("/tmp/encoder.sock", Bincode::default).await?;

    listener
        .filter_map(|r| async { r.ok() })
        .map(|transport| {
            let server = EncoderServer;
            BaseChannel::with_defaults(transport)
                .execute(server.serve())
        })
        .buffer_unordered(10)
        .for_each(|_| async {})
        .await;

    Ok(())
}
```

### Client Setup

```rust
async fn connect_to_encoder() -> anyhow::Result<VideoEncoderClient> {
    let transport = unix_fd::connect("/tmp/encoder.sock", Bincode::default).await?;

    let client = VideoEncoderClient::new(Default::default(), transport).spawn();
    Ok(client)
}
```

## Testing Strategy

1. **Unit Tests**: FD extraction/injection, serialization
2. **Integration Tests**: Full RPC with FD passing
3. **Stress Tests**: Many concurrent FD-passing RPCs
4. **Resource Tests**: Verify FDs are properly closed
5. **Error Tests**: Handle FD exhaustion, invalid FDs

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] `fd.rs`: `PassedFd` type with serialization
- [ ] `fd_transport.rs`: `FdUnixStream` with sendmsg/recvmsg

### Phase 2: Framing Layer
- [ ] `fd_codec.rs`: FD-aware length-delimited codec
- [ ] Wire protocol implementation

### Phase 3: Transport Integration
- [ ] `serde_transport.rs`: `unix_fd` module
- [ ] FD-enabled client/server transports

### Phase 4: Macro Integration
- [ ] Update `#[tarpc::service]` for FD detection
- [ ] Code generation for FD extraction/injection

### Phase 5: Testing & Documentation
- [ ] Comprehensive test suite
- [ ] Example application
- [ ] Documentation

## Open Questions

1. **Multiple FDs per field**: Support `Vec<PassedFd>`?
2. **Nested structs**: How to handle `struct Foo { inner: Bar }` where `Bar` contains `PassedFd`?
3. **Derive macro**: Should we add `#[derive(ContainsFds)]`?
4. **Fallback**: Error or panic when using `PassedFd` over non-FD transport?

## Alternatives Considered

### Alternative A: Separate FD Channel
Have a parallel channel just for FDs. Rejected: adds complexity, synchronization issues.

### Alternative B: FD as Opaque Bytes
Serialize FD as bytes, reconstruct on other side. Rejected: doesn't work (FD is process-local).

### Alternative C: Shared Memory Instead
Use shared memory for data transfer. Rejected: doesn't address DMABUF use case.

## Dependencies

- `libc`: For `sendmsg`/`recvmsg` syscalls
- `tokio`: Async runtime (already a dependency)
- No new external dependencies required

## Platform Support

| Platform | Support |
|----------|---------|
| Linux    | Full    |
| macOS    | Possible (uses different SCM_RIGHTS details) |
| FreeBSD  | Possible |
| Windows  | Not supported (no Unix sockets) |

Initial implementation targets Linux only.
