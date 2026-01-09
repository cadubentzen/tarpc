// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Example demonstrating file descriptor passing with the high-level tarpc service system.
//!
//! This example shows how to use `#[tarpc::service]` with fd-passing by using the
//! `derive_contains_fds = true` option. The generated request and response types
//! automatically implement `ContainsFds`, allowing `PassedFd` parameters to be
//! passed between client and server.
//!
//! Key features demonstrated:
//! - Nested structs containing `PassedFd` fields
//! - Bidirectional FD passing (client sends FD, server returns FD)
//! - Multiple FDs in nested structures
//! - Zero-copy memory sharing via memfd
//!
//! Run with: cargo run --example fd_passing_service --features "fd-passing,serde1,tokio1"

use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, Write};
use std::os::unix::io::{FromRawFd, IntoRawFd, OwnedFd};
use tarpc::{
    client, context,
    fd::PassedFd,
    server::{self, Channel},
    ContainsFds,
};
use tokio_serde::formats::Bincode;

// ============================================================================
// Nested structures demonstrating ContainsFds with complex data
// ============================================================================

/// A request to read from a file, wrapped in a struct.
#[derive(Debug, Serialize, Deserialize, ContainsFds)]
pub struct ReadRequest {
    /// The file descriptor to read from
    pub fd: PassedFd,
    /// Maximum bytes to read
    pub max_bytes: usize,
    /// Optional offset to seek to before reading
    pub offset: Option<u64>,
}

/// A request to copy data between files, with nested FD references.
#[derive(Debug, Serialize, Deserialize, ContainsFds)]
pub struct CopyRequest {
    /// Source file info
    pub source: FileHandle,
    /// Destination file info
    pub destination: FileHandle,
}

/// A file handle containing an FD and metadata.
#[derive(Debug, Serialize, Deserialize, ContainsFds)]
pub struct FileHandle {
    /// The file descriptor
    pub fd: PassedFd,
    /// Human-readable name for logging
    pub name: String,
}

/// Result of a file operation.
#[derive(Debug, Serialize, Deserialize, ContainsFds)]
pub struct FileResult {
    /// Number of bytes processed
    pub bytes_processed: usize,
    /// Status message
    pub message: String,
}

/// A transform request: read from input FD, transform data, write to new FD returned by server.
#[derive(Debug, Serialize, Deserialize, ContainsFds)]
pub struct TransformRequest {
    /// Input file to read from
    pub input: FileHandle,
    /// Transformation to apply
    pub transform: TransformType,
}

/// Types of transformations the server can apply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformType {
    /// Convert text to uppercase
    Uppercase,
    /// Convert text to lowercase
    Lowercase,
    /// Reverse the bytes
    Reverse,
}

// TransformType contains no FDs, so it implements NoFds
impl tarpc::fd::NoFds for TransformType {}

/// Result of a transform operation - server creates output FD and returns it.
#[derive(Debug, Serialize, Deserialize, ContainsFds)]
pub struct TransformResult {
    /// The output file descriptor created by the server containing transformed data
    pub output: FileHandle,
    /// Number of bytes in the output
    pub output_size: usize,
}

// ============================================================================
// Service definition
// ============================================================================

/// Define a service that can handle file descriptor passing with nested structs.
///
/// The `derive_contains_fds = true` option causes the generated `FileServiceRequest`
/// and `FileServiceResponse` enums to implement `ContainsFds`, enabling fd-passing.
#[tarpc::service(derive_contains_fds = true)]
pub trait FileService {
    /// Reads content from a file using a nested request struct.
    async fn read_file(request: ReadRequest) -> Vec<u8>;

    /// Copies content between files using nested structs with multiple FDs.
    async fn copy_files(request: CopyRequest) -> FileResult;

    /// Transforms data: takes an input FD from client, returns output FD from server.
    ///
    /// This demonstrates bidirectional FD passing - the client sends an FD,
    /// and the server creates a new FD with transformed data and returns it.
    async fn transform(request: TransformRequest) -> TransformResult;

    /// Creates a memfd on the server and returns it to the client.
    ///
    /// This demonstrates the server creating an FD and passing it to the client.
    async fn create_shared_buffer(name: String, initial_data: Vec<u8>) -> FileHandle;

    /// A simple method without file descriptors, for comparison.
    async fn ping() -> String;
}

// ============================================================================
// Server implementation
// ============================================================================

/// Server implementation
#[derive(Clone)]
struct FileServer;

impl FileService for FileServer {
    async fn read_file(self, _: context::Context, request: ReadRequest) -> Vec<u8> {
        let owned_fd: OwnedFd = request.fd.into_fd();
        let mut file = unsafe { std::fs::File::from_raw_fd(owned_fd.into_raw_fd()) };

        // Seek if offset specified
        if let Some(offset) = request.offset {
            file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        } else {
            file.seek(std::io::SeekFrom::Start(0)).unwrap();
        }

        let mut buf = vec![0u8; request.max_bytes];
        let n = file.read(&mut buf).unwrap_or(0);
        buf.truncate(n);
        buf
    }

    async fn copy_files(self, _: context::Context, request: CopyRequest) -> FileResult {
        let src_fd: OwnedFd = request.source.fd.into_fd();
        let dst_fd: OwnedFd = request.destination.fd.into_fd();

        let mut src_file = unsafe { std::fs::File::from_raw_fd(src_fd.into_raw_fd()) };
        let mut dst_file = unsafe { std::fs::File::from_raw_fd(dst_fd.into_raw_fd()) };

        src_file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf = Vec::new();
        src_file.read_to_end(&mut buf).unwrap_or(0);
        let bytes_written = dst_file.write(&buf).unwrap_or(0);

        FileResult {
            bytes_processed: bytes_written,
            message: format!(
                "Copied {} bytes from '{}' to '{}'",
                bytes_written, request.source.name, request.destination.name
            ),
        }
    }

    async fn transform(self, _: context::Context, request: TransformRequest) -> TransformResult {
        // Read from input FD
        let input_fd: OwnedFd = request.input.fd.into_fd();
        let mut input_file = unsafe { std::fs::File::from_raw_fd(input_fd.into_raw_fd()) };

        input_file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut input_data = Vec::new();
        input_file.read_to_end(&mut input_data).unwrap_or(0);

        // Apply transformation
        let output_data = match request.transform {
            TransformType::Uppercase => {
                String::from_utf8_lossy(&input_data).to_uppercase().into_bytes()
            }
            TransformType::Lowercase => {
                String::from_utf8_lossy(&input_data).to_lowercase().into_bytes()
            }
            TransformType::Reverse => {
                let mut reversed = input_data.clone();
                reversed.reverse();
                reversed
            }
        };

        // Create output memfd on the server
        let output_memfd = create_memfd("transform_output").unwrap();
        let mut output_file =
            unsafe { std::fs::File::from_raw_fd(output_memfd.into_raw_fd()) };
        output_file.write_all(&output_data).unwrap();
        output_file.flush().unwrap();

        let output_size = output_data.len();
        let output_fd: OwnedFd = output_file.into();

        TransformResult {
            output: FileHandle {
                fd: PassedFd::new(output_fd),
                name: format!("transformed_{}", request.input.name),
            },
            output_size,
        }
    }

    async fn create_shared_buffer(
        self,
        _: context::Context,
        name: String,
        initial_data: Vec<u8>,
    ) -> FileHandle {
        // Server creates a memfd and populates it
        let memfd = create_memfd(&name).unwrap();
        let mut file = unsafe { std::fs::File::from_raw_fd(memfd.into_raw_fd()) };
        file.write_all(&initial_data).unwrap();
        file.flush().unwrap();

        let fd: OwnedFd = file.into();
        FileHandle {
            fd: PassedFd::new(fd),
            name,
        }
    }

    async fn ping(self, _: context::Context) -> String {
        "pong".to_string()
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Creates a memfd (memory file descriptor) using the memfd_create syscall.
fn create_memfd(name: &str) -> std::io::Result<OwnedFd> {
    use std::ffi::CString;

    let name_cstr = CString::new(name).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid memfd name")
    })?;

    // MFD_CLOEXEC = 0x0001 - close on exec
    let flags = 0x0001u32;

    let fd = unsafe { libc::memfd_create(name_cstr.as_ptr(), flags) };

    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

// ============================================================================
// Main - Client tests
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tarpc::fd_transport::FdUnixStream;
    use tarpc::serde_transport::unix_fd;

    // Create a connected socketpair
    let (client_stream, server_stream) = FdUnixStream::pair()?;
    println!("Created socketpair for client/server communication");

    // Create server transport
    let server_transport = unix_fd::FdChannelTransport::new(server_stream, Bincode::default());
    let server = server::BaseChannel::with_defaults(server_transport);

    // Create client transport
    let client_transport = unix_fd::FdChannelTransport::new(client_stream, Bincode::default());
    let client = FileServiceClient::new(client::Config::default(), client_transport).spawn();

    // Run server as a task that processes requests
    let server_task = async {
        server
            .execute(FileServer.serve())
            .for_each(|response| async move {
                response.await;
            })
            .await;
    };

    // Run client tests
    let client_task = async {
        println!("\n--- Test 1: Ping/Pong (no FDs) ---");
        let response = client.ping(context::current()).await?;
        println!("Server responded: {}", response);

        // Test 2: Read file using nested ReadRequest struct
        println!("\n--- Test 2: Read file via nested struct ---");
        let mut tmp_file = tempfile::tempfile()?;
        tmp_file.write_all(b"Hello from nested struct! This demonstrates ContainsFds with structs.")?;
        tmp_file.flush()?;

        let fd: OwnedFd = tmp_file.into();
        let request = ReadRequest {
            fd: PassedFd::new(fd),
            max_bytes: 1024,
            offset: None,
        };
        let content = client.read_file(context::current(), request).await?;
        println!(
            "Server read {} bytes: {:?}",
            content.len(),
            String::from_utf8_lossy(&content)
        );

        // Test 3: Copy between files using deeply nested structs
        println!("\n--- Test 3: Copy files via nested CopyRequest ---");
        let mut src_file = tempfile::tempfile()?;
        src_file.write_all(b"Source data in nested FileHandle struct")?;
        src_file.flush()?;

        let dst_file = tempfile::tempfile()?;

        let src_fd: OwnedFd = src_file.into();
        let dst_fd: OwnedFd = dst_file.into();

        let copy_request = CopyRequest {
            source: FileHandle {
                fd: PassedFd::new(src_fd),
                name: "source.txt".to_string(),
            },
            destination: FileHandle {
                fd: PassedFd::new(dst_fd),
                name: "destination.txt".to_string(),
            },
        };

        let result = client.copy_files(context::current(), copy_request).await?;
        println!("Copy result: {} bytes - {}", result.bytes_processed, result.message);

        // Test 4: Bidirectional FD passing - client sends FD, server returns FD
        println!("\n--- Test 4: Bidirectional FD passing (transform) ---");
        let mut input_file = tempfile::tempfile()?;
        input_file.write_all(b"Hello World - Transform Me!")?;
        input_file.flush()?;

        let input_fd: OwnedFd = input_file.into();
        let transform_request = TransformRequest {
            input: FileHandle {
                fd: PassedFd::new(input_fd),
                name: "input.txt".to_string(),
            },
            transform: TransformType::Uppercase,
        };

        let transform_result = client.transform(context::current(), transform_request).await?;
        println!(
            "Transform created output '{}' with {} bytes",
            transform_result.output.name, transform_result.output_size
        );

        // Read the FD that the server returned to us
        let output_fd: OwnedFd = transform_result.output.fd.into_fd();
        let mut output_file = unsafe { std::fs::File::from_raw_fd(output_fd.into_raw_fd()) };
        output_file.seek(std::io::SeekFrom::Start(0))?;
        let mut transformed_content = String::new();
        output_file.read_to_string(&mut transformed_content)?;
        println!("Server returned FD with transformed data: {:?}", transformed_content);
        println!("✓ Bidirectional FD passing successful!");

        // Test 5: Server creates and returns FD (no input FD)
        println!("\n--- Test 5: Server creates shared buffer (returns FD) ---");
        let server_data = b"This buffer was created by the server!".to_vec();
        let handle = client
            .create_shared_buffer(
                context::current(),
                "server_created_buffer".to_string(),
                server_data.clone(),
            )
            .await?;

        println!("Server created buffer named: {}", handle.name);

        // Read from the FD that the server created
        let buffer_fd: OwnedFd = handle.fd.into_fd();
        let mut buffer_file = unsafe { std::fs::File::from_raw_fd(buffer_fd.into_raw_fd()) };
        buffer_file.seek(std::io::SeekFrom::Start(0))?;
        let mut received_data = Vec::new();
        buffer_file.read_to_end(&mut received_data)?;

        println!(
            "Client read from server-created FD: {:?}",
            String::from_utf8_lossy(&received_data)
        );

        if received_data == server_data {
            println!("✓ Server-created FD data matches!");
        }

        // Test 6: Transform with reverse (another bidirectional test)
        println!("\n--- Test 6: Reverse transform (bidirectional) ---");
        let mut rev_input = tempfile::tempfile()?;
        rev_input.write_all(b"ABCDEFG")?;
        rev_input.flush()?;

        let rev_fd: OwnedFd = rev_input.into();
        let rev_result = client
            .transform(
                context::current(),
                TransformRequest {
                    input: FileHandle {
                        fd: PassedFd::new(rev_fd),
                        name: "reverse_input.txt".to_string(),
                    },
                    transform: TransformType::Reverse,
                },
            )
            .await?;

        let rev_output_fd: OwnedFd = rev_result.output.fd.into_fd();
        let mut rev_output = unsafe { std::fs::File::from_raw_fd(rev_output_fd.into_raw_fd()) };
        rev_output.seek(std::io::SeekFrom::Start(0))?;
        let mut reversed = String::new();
        rev_output.read_to_string(&mut reversed)?;
        println!("Input: 'ABCDEFG' -> Reversed: '{}'", reversed);

        if reversed == "GFEDCBA" {
            println!("✓ Reverse transform correct!");
        }

        println!("\n=== Example completed successfully! ===");
        println!("\nThis example demonstrated:");
        println!("  - Nested structs with PassedFd fields (ReadRequest, CopyRequest)");
        println!("  - Deeply nested FDs (CopyRequest -> FileHandle -> PassedFd)");
        println!("  - Bidirectional FD passing (client FD in, server FD out)");
        println!("  - Server-created FDs returned to client");
        println!("  - ContainsFds derive macro on custom structs");
        println!("  - Integration with #[tarpc::service(derive_contains_fds = true)]");

        anyhow::Ok(())
    };

    // Run both tasks concurrently - client finishes first, then we're done
    tokio::select! {
        _ = server_task => {
            println!("Server finished unexpectedly");
        }
        result = client_task => {
            result?;
        }
    }

    Ok(())
}
