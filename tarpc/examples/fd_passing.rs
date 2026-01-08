// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Example demonstrating file descriptor passing over Unix domain sockets.
//!
//! This example shows how to pass file descriptors between processes using tarpc's
//! fd-passing feature. The client creates a temporary file, writes data to it,
//! and sends the file descriptor to the server. The server then reads the data
//! from the received file descriptor.
//!
//! Run with: cargo run --example fd_passing --features fd-passing

use std::io::{Read, Seek, Write};
use std::os::unix::io::{FromRawFd, IntoRawFd, OwnedFd};
// Import the derive macro from the crate root
use tarpc::ContainsFds;
// Import the trait and PassedFd type from the fd module
use tarpc::fd::PassedFd;
use tokio_serde::formats::Bincode;

/// A request containing a file descriptor.
#[derive(Debug, serde::Serialize, serde::Deserialize, ContainsFds)]
struct ReadFileRequest {
    /// The file descriptor to read from.
    fd: PassedFd,
    /// Maximum number of bytes to read.
    max_bytes: usize,
}

/// A response containing the data read from the file.
#[derive(Debug, serde::Serialize, serde::Deserialize, ContainsFds)]
struct ReadFileResponse {
    /// The data read from the file.
    data: Vec<u8>,
    /// Number of bytes read.
    bytes_read: usize,
}

/// Example showing multiple file descriptors in one message.
#[derive(Debug, serde::Serialize, serde::Deserialize, ContainsFds)]
struct CopyFileRequest {
    /// Source file descriptor to read from.
    src: PassedFd,
    /// Destination file descriptor to write to.
    dst: PassedFd,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ContainsFds)]
struct CopyFileResponse {
    bytes_copied: usize,
}

/// Message enum demonstrating fd-passing with enums.
#[derive(Debug, serde::Serialize, serde::Deserialize, ContainsFds)]
enum Request {
    ReadFile(ReadFileRequest),
    CopyFile(CopyFileRequest),
    Ping,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ContainsFds)]
enum Response {
    ReadFile(ReadFileResponse),
    CopyFile(CopyFileResponse),
    Pong,
}

/// Server handler that processes requests.
fn handle_request(request: Request) -> Response {
    match request {
        Request::ReadFile(req) => {
            // Convert PassedFd to a File
            let fd: OwnedFd = req.fd.into_fd();
            let mut file = unsafe { std::fs::File::from_raw_fd(fd.into_raw_fd()) };

            // Seek to beginning and read
            file.seek(std::io::SeekFrom::Start(0)).unwrap();
            let mut buf = vec![0u8; req.max_bytes];
            let bytes_read = file.read(&mut buf).unwrap();
            buf.truncate(bytes_read);

            Response::ReadFile(ReadFileResponse {
                data: buf,
                bytes_read,
            })
        }
        Request::CopyFile(req) => {
            let src_fd: OwnedFd = req.src.into_fd();
            let dst_fd: OwnedFd = req.dst.into_fd();

            let mut src = unsafe { std::fs::File::from_raw_fd(src_fd.into_raw_fd()) };
            let mut dst = unsafe { std::fs::File::from_raw_fd(dst_fd.into_raw_fd()) };

            src.seek(std::io::SeekFrom::Start(0)).unwrap();
            let mut buf = Vec::new();
            src.read_to_end(&mut buf).unwrap();
            let bytes_copied = dst.write(&buf).unwrap();

            Response::CopyFile(CopyFileResponse { bytes_copied })
        }
        Request::Ping => Response::Pong,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tarpc::serde_transport::unix_fd;
    use tarpc::fd_transport::FdUnixStream;

    // Create a socketpair for testing (simulates client/server communication)
    use std::os::unix::net::UnixStream;
    let (client_sock, server_sock) = UnixStream::pair()?;

    // Create FdUnixStream from std UnixStreams
    let client_stream = FdUnixStream::new(client_sock)?;
    let server_stream = FdUnixStream::new(server_sock)?;

    // Create transports
    let mut client: unix_fd::FdTransport<Response, Request, _> =
        unix_fd::FdTransport::new(client_stream, Bincode::default());
    let mut server: unix_fd::FdTransport<Request, Response, _> =
        unix_fd::FdTransport::new(server_stream, Bincode::default());

    println!("Connected via socketpair\n");

    // Test 1: Send a ping
    println!("--- Test 1: Ping/Pong ---");
    client.send(Request::Ping).await?;
    let request = server.recv().await?.expect("expected request");
    println!("Server received: {:?}", request);

    let response = handle_request(request);
    server.send(response).await?;

    let response = client.recv().await?.expect("expected response");
    println!("Client received: {:?}", response);

    // Test 2: Create a file, write data, send FD for server to read
    println!("\n--- Test 2: Read file via FD ---");
    let mut tmp_file = tempfile::tempfile()?;
    tmp_file.write_all(b"Hello from the client! This data was written before sending the FD.")?;
    tmp_file.flush()?;

    // Convert to OwnedFd and wrap in PassedFd
    let fd: OwnedFd = tmp_file.into();
    let request = Request::ReadFile(ReadFileRequest {
        fd: PassedFd::new(fd),
        max_bytes: 1024,
    });
    println!("Client sending ReadFile request with FD...");
    client.send(request).await?;

    let request = server.recv().await?.expect("expected request");
    println!("Server received ReadFile request");

    let response = handle_request(request);
    if let Response::ReadFile(ref resp) = response {
        println!(
            "Server read {} bytes: {:?}",
            resp.bytes_read,
            String::from_utf8_lossy(&resp.data)
        );
    }
    server.send(response).await?;

    let response = client.recv().await?.expect("expected response");
    println!("Client received response: {:?}", response);

    // Test 3: Copy between two file descriptors
    println!("\n--- Test 3: Copy file via two FDs ---");
    let mut src_file = tempfile::tempfile()?;
    src_file.write_all(b"Source file content to be copied across processes")?;
    src_file.flush()?;
    let dst_file = tempfile::tempfile()?;

    let src_fd: OwnedFd = src_file.into();
    let dst_fd: OwnedFd = dst_file.into();

    let request = Request::CopyFile(CopyFileRequest {
        src: PassedFd::new(src_fd),
        dst: PassedFd::new(dst_fd),
    });
    println!("Client sending CopyFile request with 2 FDs...");
    client.send(request).await?;

    let request = server.recv().await?.expect("expected request");
    println!("Server received CopyFile request");

    let response = handle_request(request);
    if let Response::CopyFile(ref resp) = response {
        println!("Server copied {} bytes between FDs", resp.bytes_copied);
    }
    server.send(response).await?;

    let response = client.recv().await?.expect("expected response");
    println!("Client received response: {:?}", response);

    println!("\n=== Example completed successfully! ===");
    println!("\nThis example demonstrated:");
    println!("  - Passing a single file descriptor (ReadFile)");
    println!("  - Passing multiple file descriptors (CopyFile)");
    println!("  - Using the ContainsFds derive macro with structs and enums");
    println!("  - File descriptors being passed out-of-band via SCM_RIGHTS");

    Ok(())
}
