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
//! Run with: cargo run --example fd_passing_service --features "fd-passing,serde1,tokio1"

use futures::prelude::*;
use std::io::{Read, Seek, Write};
use std::os::unix::io::{FromRawFd, IntoRawFd, OwnedFd};
use tarpc::{
    client, context,
    fd::PassedFd,
    server::{self, Channel},
};
use tokio_serde::formats::Bincode;

/// Define a service that can handle file descriptor passing.
///
/// The `derive_contains_fds = true` option causes the generated `FileServiceRequest`
/// and `FileServiceResponse` enums to implement `ContainsFds`, enabling fd-passing.
#[tarpc::service(derive_contains_fds = true)]
pub trait FileService {
    /// Reads content from a file descriptor passed by the client.
    ///
    /// The client sends an open file descriptor, and the server reads from it
    /// and returns the content. This demonstrates passing an FD from client to server.
    async fn read_file(fd: PassedFd, max_bytes: usize) -> Vec<u8>;

    /// Writes content to a file descriptor passed by the client.
    ///
    /// The client sends an open file descriptor and data, and the server writes
    /// the data to the FD. Returns the number of bytes written.
    async fn write_file(fd: PassedFd, data: Vec<u8>) -> usize;

    /// Copies content between two file descriptors.
    ///
    /// This demonstrates passing multiple FDs in a single request.
    async fn copy_file(src: PassedFd, dst: PassedFd) -> usize;

    /// A simple method without file descriptors, for comparison.
    async fn ping() -> String;
}

/// Server implementation
#[derive(Clone)]
struct FileServer;

impl FileService for FileServer {
    async fn read_file(self, _: context::Context, fd: PassedFd, max_bytes: usize) -> Vec<u8> {
        let owned_fd: OwnedFd = fd.into_fd();
        let mut file = unsafe { std::fs::File::from_raw_fd(owned_fd.into_raw_fd()) };

        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0u8; max_bytes];
        let n = file.read(&mut buf).unwrap_or(0);
        buf.truncate(n);
        buf
    }

    async fn write_file(self, _: context::Context, fd: PassedFd, data: Vec<u8>) -> usize {
        let owned_fd: OwnedFd = fd.into_fd();
        let mut file = unsafe { std::fs::File::from_raw_fd(owned_fd.into_raw_fd()) };

        file.write(&data).unwrap_or(0)
    }

    async fn copy_file(self, _: context::Context, src: PassedFd, dst: PassedFd) -> usize {
        let src_fd: OwnedFd = src.into_fd();
        let dst_fd: OwnedFd = dst.into_fd();

        let mut src_file = unsafe { std::fs::File::from_raw_fd(src_fd.into_raw_fd()) };
        let mut dst_file = unsafe { std::fs::File::from_raw_fd(dst_fd.into_raw_fd()) };

        src_file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf = Vec::new();
        src_file.read_to_end(&mut buf).unwrap_or(0);
        dst_file.write(&buf).unwrap_or(0)
    }

    async fn ping(self, _: context::Context) -> String {
        "pong".to_string()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tarpc::serde_transport::unix_fd;
    use tarpc::fd_transport::FdUnixStream;

    // For this example, we'll use a socketpair to avoid threading issues
    // with PassedFd's Cell-based interior mutability.
    //
    // In production, you could:
    // 1. Use separate processes (fork) where each process has its own FD namespace
    // 2. Use a custom transport wrapper that handles the Send/Sync requirements
    // 3. Keep the server and client on the same task (as shown here)

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
    // We use tokio::select! to run the server and client concurrently
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

        println!("\n--- Test 2: Read file via FD ---");
        // Create a temp file with some content
        let mut tmp_file = tempfile::tempfile()?;
        tmp_file.write_all(b"Hello from the client! This was written to a temp file.")?;
        tmp_file.flush()?;

        // Send the FD to server for reading
        let fd: OwnedFd = tmp_file.into();
        let content = client
            .read_file(context::current(), PassedFd::new(fd), 1024)
            .await?;
        println!("Server read {} bytes: {:?}", content.len(), String::from_utf8_lossy(&content));

        println!("\n--- Test 3: Write file via FD ---");
        // Create a temp file for writing
        let write_file = tempfile::tempfile()?;
        let write_fd: OwnedFd = write_file.into();
        let bytes_written = client
            .write_file(
                context::current(),
                PassedFd::new(write_fd),
                b"Data written by the server!".to_vec(),
            )
            .await?;
        println!("Server wrote {} bytes", bytes_written);

        println!("\n--- Test 4: Copy between two FDs ---");
        // Create source file with content
        let mut src_file = tempfile::tempfile()?;
        src_file.write_all(b"Source content to be copied")?;
        src_file.flush()?;

        // Create destination file
        let dst_file = tempfile::tempfile()?;

        let src_fd: OwnedFd = src_file.into();
        let dst_fd: OwnedFd = dst_file.into();

        let bytes_copied = client
            .copy_file(
                context::current(),
                PassedFd::new(src_fd),
                PassedFd::new(dst_fd),
            )
            .await?;
        println!("Server copied {} bytes between FDs", bytes_copied);

        println!("\n=== Example completed successfully! ===");
        println!("\nThis example demonstrated:");
        println!("  - Using #[tarpc::service(derive_contains_fds = true)]");
        println!("  - Passing single file descriptors (read_file, write_file)");
        println!("  - Passing multiple file descriptors (copy_file)");
        println!("  - Integration with tarpc's high-level client/server infrastructure");
        println!("  - Using FdChannelTransport with BaseChannel and Client");

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
