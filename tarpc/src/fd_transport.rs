// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Low-level file descriptor passing over Unix domain sockets.
//!
//! This module provides the foundational types for sending and receiving
//! file descriptors using `sendmsg`/`recvmsg` with `SCM_RIGHTS` control messages.

use std::io;
use std::mem;
use std::os::unix::io::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream as StdUnixStream;
use std::path::Path;

use tokio::io::unix::AsyncFd;

use crate::fd::{FdError, MAX_FDS_PER_MESSAGE, SupportsFdPassing};

/// A message containing data and optional file descriptors.
#[derive(Debug)]
pub struct FdMessage {
    /// The data payload.
    pub data: Vec<u8>,
    /// File descriptors received with this message.
    pub fds: Vec<OwnedFd>,
}

impl FdMessage {
    /// Creates a new FdMessage with data only.
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data,
            fds: Vec::new(),
        }
    }

    /// Creates a new FdMessage with data and file descriptors.
    pub fn with_fds(data: Vec<u8>, fds: Vec<OwnedFd>) -> Self {
        Self { data, fds }
    }
}

/// A Unix stream that supports file descriptor passing.
///
/// This wraps a standard Unix stream and provides async methods for
/// sending and receiving data with file descriptors using `sendmsg`/`recvmsg`.
pub struct FdUnixStream {
    inner: AsyncFd<StdUnixStream>,
}

impl FdUnixStream {
    /// Creates a new FdUnixStream from a standard UnixStream.
    ///
    /// The stream is set to non-blocking mode.
    pub fn new(stream: StdUnixStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            inner: AsyncFd::new(stream)?,
        })
    }

    /// Connects to a Unix socket at the given path.
    pub async fn connect<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        // Use tokio's UnixStream for connection, then convert
        let stream = tokio::net::UnixStream::connect(path).await?;
        let std_stream = stream.into_std()?;
        Self::new(std_stream)
    }

    /// Creates a pair of connected FdUnixStreams.
    pub fn pair() -> io::Result<(Self, Self)> {
        let (a, b) = StdUnixStream::pair()?;
        Ok((Self::new(a)?, Self::new(b)?))
    }

    /// Returns a reference to the underlying standard UnixStream.
    pub fn get_ref(&self) -> &StdUnixStream {
        self.inner.get_ref()
    }

    /// Sends data with optional file descriptors.
    ///
    /// This is the core FD-passing send operation using `sendmsg` with `SCM_RIGHTS`.
    pub async fn send_with_fds(
        &self,
        data: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> io::Result<usize> {
        if fds.len() > MAX_FDS_PER_MESSAGE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                FdError::TooManyFds {
                    count: fds.len(),
                    max: MAX_FDS_PER_MESSAGE,
                },
            ));
        }

        loop {
            let mut guard = self.inner.writable().await?;
            match guard.try_io(|inner| send_with_fds_sync(inner.get_ref(), data, fds)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Receives data with optional file descriptors.
    ///
    /// This is the core FD-passing receive operation using `recvmsg` with `SCM_RIGHTS`.
    ///
    /// Returns the number of bytes read and any file descriptors received.
    pub async fn recv_with_fds(
        &self,
        buf: &mut [u8],
    ) -> io::Result<FdMessage> {
        loop {
            let mut guard = self.inner.readable().await?;
            match guard.try_io(|inner| recv_with_fds_sync(inner.get_ref(), buf)) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    /// Sends all data with file descriptors, handling partial writes.
    ///
    /// Note: FDs are sent with the first chunk of data only.
    pub async fn send_all_with_fds(
        &self,
        mut data: &[u8],
        fds: &[BorrowedFd<'_>],
    ) -> io::Result<()> {
        let mut fds_sent = false;

        while !data.is_empty() {
            let sent = if !fds_sent {
                fds_sent = true;
                self.send_with_fds(data, fds).await?
            } else {
                self.send_with_fds(data, &[]).await?
            };

            if sent == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write any data",
                ));
            }

            data = &data[sent..];
        }

        Ok(())
    }
}

impl SupportsFdPassing for FdUnixStream {}

impl AsRawFd for FdUnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

/// Synchronously send data with file descriptors using sendmsg.
fn send_with_fds_sync(
    socket: &StdUnixStream,
    data: &[u8],
    fds: &[BorrowedFd<'_>],
) -> io::Result<usize> {
    use libc::{
        c_void, cmsghdr, iovec, msghdr, sendmsg, CMSG_DATA, CMSG_FIRSTHDR, CMSG_LEN, CMSG_SPACE,
        SCM_RIGHTS, SOL_SOCKET,
    };

    let iov = iovec {
        iov_base: data.as_ptr() as *mut c_void,
        iov_len: data.len(),
    };

    // Calculate control message buffer size
    let fd_count = fds.len();
    let cmsg_size = if fd_count > 0 {
        unsafe { CMSG_SPACE((fd_count * mem::size_of::<RawFd>()) as u32) as usize }
    } else {
        0
    };

    // Allocate control message buffer
    let mut cmsg_buf = vec![0u8; cmsg_size];

    let mut msg: msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &iov as *const iovec as *mut iovec;
    msg.msg_iovlen = 1;

    if fd_count > 0 {
        msg.msg_control = cmsg_buf.as_mut_ptr() as *mut c_void;
        msg.msg_controllen = cmsg_size;

        // Fill in the control message
        unsafe {
            let cmsg: *mut cmsghdr = CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = SOL_SOCKET;
            (*cmsg).cmsg_type = SCM_RIGHTS;
            (*cmsg).cmsg_len = CMSG_LEN((fd_count * mem::size_of::<RawFd>()) as u32) as usize;

            // Copy file descriptors into the control message
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

/// Synchronously receive data with file descriptors using recvmsg.
fn recv_with_fds_sync(socket: &StdUnixStream, buf: &mut [u8]) -> io::Result<FdMessage> {
    use libc::{
        c_void, cmsghdr, iovec, msghdr, recvmsg, CMSG_DATA, CMSG_FIRSTHDR, CMSG_NXTHDR,
        CMSG_SPACE, SCM_RIGHTS, SOL_SOCKET,
    };

    let mut iov = iovec {
        iov_base: buf.as_mut_ptr() as *mut c_void,
        iov_len: buf.len(),
    };

    // Allocate buffer for control messages (enough for max FDs)
    let cmsg_size = unsafe { CMSG_SPACE((MAX_FDS_PER_MESSAGE * mem::size_of::<RawFd>()) as u32) as usize };
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

    // Extract file descriptors from control messages
    let mut fds = Vec::new();

    unsafe {
        let mut cmsg = CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == SOL_SOCKET && (*cmsg).cmsg_type == SCM_RIGHTS {
                // Calculate number of FDs in this control message
                let fd_bytes = (*cmsg).cmsg_len - mem::size_of::<cmsghdr>();
                let fd_count = fd_bytes / mem::size_of::<RawFd>();

                let fd_ptr = CMSG_DATA(cmsg) as *const RawFd;
                for i in 0..fd_count {
                    let raw_fd = *fd_ptr.add(i);
                    // Wrap in OwnedFd - this takes ownership of the FD
                    fds.push(OwnedFd::from_raw_fd(raw_fd));
                }
            }
            cmsg = CMSG_NXTHDR(&msg, cmsg);
        }
    }

    Ok(FdMessage {
        data: buf[..bytes_read].to_vec(),
        fds,
    })
}

/// A listener for Unix domain sockets that produces FdUnixStream connections.
pub struct FdUnixListener {
    inner: tokio::net::UnixListener,
}

impl FdUnixListener {
    /// Creates a new listener bound to the given path.
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let listener = tokio::net::UnixListener::bind(path)?;
        Ok(Self { inner: listener })
    }

    /// Accepts a new connection.
    pub async fn accept(&self) -> io::Result<FdUnixStream> {
        let (stream, _addr) = self.inner.accept().await?;
        let std_stream = stream.into_std()?;
        FdUnixStream::new(std_stream)
    }

    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<std::os::unix::net::SocketAddr> {
        let tokio_addr = self.inner.local_addr()?;
        // Convert tokio SocketAddr to std SocketAddr via pathname
        if let Some(path) = tokio_addr.as_pathname() {
            std::os::unix::net::SocketAddr::from_pathname(path)
        } else {
            // Abstract or unnamed socket - return an error as we can't convert these easily
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Cannot get local address of abstract or unnamed socket",
            ))
        }
    }
}

impl SupportsFdPassing for FdUnixListener {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{Read, Seek, Write};
    use std::os::unix::io::AsFd;

    #[tokio::test]
    async fn test_send_recv_data_only() {
        let (a, b) = FdUnixStream::pair().unwrap();

        let data = b"Hello, world!";
        a.send_with_fds(data, &[]).await.unwrap();

        let mut buf = [0u8; 64];
        let msg = b.recv_with_fds(&mut buf).await.unwrap();

        assert_eq!(&msg.data, data);
        assert!(msg.fds.is_empty());
    }

    #[tokio::test]
    async fn test_send_recv_with_fd() {
        let (a, b) = FdUnixStream::pair().unwrap();

        // Create a temp file and write some data
        let mut tmp = tempfile::tempfile().unwrap();
        tmp.write_all(b"Test file content").unwrap();
        tmp.flush().unwrap();

        // Send the FD
        let data = b"Here's a file!";
        let fd: OwnedFd = tmp.into();
        a.send_with_fds(data, &[fd.as_fd()]).await.unwrap();

        // Receive the FD
        let mut buf = [0u8; 64];
        let msg = b.recv_with_fds(&mut buf).await.unwrap();

        assert_eq!(&msg.data, data);
        assert_eq!(msg.fds.len(), 1);

        // Read from the received FD
        let mut received_file: File = msg.fds.into_iter().next().unwrap().into();
        received_file.rewind().unwrap();
        let mut content = String::new();
        received_file.read_to_string(&mut content).unwrap();
        assert_eq!(content, "Test file content");
    }

    #[tokio::test]
    async fn test_send_recv_multiple_fds() {
        let (a, b) = FdUnixStream::pair().unwrap();

        // Create multiple temp files
        let mut files: Vec<File> = Vec::new();
        for i in 0..5 {
            let mut f = tempfile::tempfile().unwrap();
            write!(f, "File {}", i).unwrap();
            files.push(f);
        }

        // Convert to OwnedFd
        let fds: Vec<OwnedFd> = files.into_iter().map(Into::into).collect();
        let borrowed: Vec<BorrowedFd> = fds.iter().map(|f| f.as_fd()).collect();

        // Send all FDs
        let data = b"Multiple files!";
        a.send_with_fds(data, &borrowed).await.unwrap();

        // Receive
        let mut buf = [0u8; 64];
        let msg = b.recv_with_fds(&mut buf).await.unwrap();

        assert_eq!(&msg.data, data);
        assert_eq!(msg.fds.len(), 5);

        // Verify each file
        for (i, fd) in msg.fds.into_iter().enumerate() {
            let mut f: File = fd.into();
            f.rewind().unwrap();
            let mut content = String::new();
            f.read_to_string(&mut content).unwrap();
            assert_eq!(content, format!("File {}", i));
        }
    }

    #[tokio::test]
    async fn test_too_many_fds() {
        let (a, _b) = FdUnixStream::pair().unwrap();

        // Try to send too many FDs
        let files: Vec<File> = (0..300)
            .map(|_| tempfile::tempfile().unwrap())
            .collect();
        let fds: Vec<OwnedFd> = files.into_iter().map(Into::into).collect();
        let borrowed: Vec<BorrowedFd> = fds.iter().map(|f| f.as_fd()).collect();

        let result = a.send_with_fds(b"test", &borrowed).await;
        assert!(result.is_err());
    }
}
