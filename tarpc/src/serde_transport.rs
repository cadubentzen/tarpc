// Copyright 2019 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! A generic Serde-based `Transport` that can serialize anything supported by `tokio-serde` via any medium that implements `AsyncRead` and `AsyncWrite`.

#![deny(missing_docs)]

use futures::{prelude::*, task::*};
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{error::Error, io, pin::Pin};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_serde::{Framed as SerdeFramed, *};
use tokio_util::codec::{Framed, length_delimited::LengthDelimitedCodec};

/// A transport that serializes to, and deserializes from, a byte stream.
#[pin_project]
pub struct Transport<S, Item, SinkItem, Codec> {
    #[pin]
    inner: SerdeFramed<Framed<S, LengthDelimitedCodec>, Item, SinkItem, Codec>,
}

impl<S, Item, SinkItem, Codec> Transport<S, Item, SinkItem, Codec> {
    /// Returns the inner transport over which messages are sent and received.
    pub fn get_ref(&self) -> &S {
        self.inner.get_ref().get_ref()
    }
}

impl<S, Item, SinkItem, Codec, CodecError> Stream for Transport<S, Item, SinkItem, Codec>
where
    S: AsyncWrite + AsyncRead,
    Item: for<'a> Deserialize<'a>,
    Codec: Deserializer<Item>,
    CodecError: Into<Box<dyn std::error::Error + Send + Sync>>,
    SerdeFramed<Framed<S, LengthDelimitedCodec>, Item, SinkItem, Codec>:
        Stream<Item = Result<Item, CodecError>>,
{
    type Item = io::Result<Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<io::Result<Item>>> {
        self.project().inner.poll_next(cx).map_err(io::Error::other)
    }
}

impl<S, Item, SinkItem, Codec, CodecError> Sink<SinkItem> for Transport<S, Item, SinkItem, Codec>
where
    S: AsyncWrite,
    SinkItem: Serialize,
    Codec: Serializer<SinkItem>,
    CodecError: Into<Box<dyn Error + Send + Sync>>,
    SerdeFramed<Framed<S, LengthDelimitedCodec>, Item, SinkItem, Codec>:
        Sink<SinkItem, Error = CodecError>,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project()
            .inner
            .poll_ready(cx)
            .map_err(io::Error::other)
    }

    fn start_send(self: Pin<&mut Self>, item: SinkItem) -> io::Result<()> {
        self.project()
            .inner
            .start_send(item)
            .map_err(io::Error::other)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project()
            .inner
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project()
            .inner
            .poll_close(cx)
            .map_err(io::Error::other)
    }
}

/// Constructs a new transport from a framed transport and a serialization codec.
pub fn new<S, Item, SinkItem, Codec>(
    framed_io: Framed<S, LengthDelimitedCodec>,
    codec: Codec,
) -> Transport<S, Item, SinkItem, Codec>
where
    S: AsyncWrite + AsyncRead,
    Item: for<'de> Deserialize<'de>,
    SinkItem: Serialize,
    Codec: Serializer<SinkItem> + Deserializer<Item>,
{
    Transport {
        inner: SerdeFramed::new(framed_io, codec),
    }
}

impl<S, Item, SinkItem, Codec> From<(S, Codec)> for Transport<S, Item, SinkItem, Codec>
where
    S: AsyncWrite + AsyncRead,
    Item: for<'de> Deserialize<'de>,
    SinkItem: Serialize,
    Codec: Serializer<SinkItem> + Deserializer<Item>,
{
    fn from((io, codec): (S, Codec)) -> Self {
        new(Framed::new(io, LengthDelimitedCodec::new()), codec)
    }
}

#[cfg(feature = "tcp")]
#[cfg_attr(docsrs, doc(cfg(feature = "tcp")))]
/// TCP support for generic transport using Tokio.
pub mod tcp {
    use {
        super::*,
        futures::ready,
        std::{marker::PhantomData, net::SocketAddr},
        tokio::net::{TcpListener, TcpStream, ToSocketAddrs},
        tokio_util::codec::length_delimited,
    };

    impl<Item, SinkItem, Codec> Transport<TcpStream, Item, SinkItem, Codec> {
        /// Returns the peer address of the underlying TcpStream.
        pub fn peer_addr(&self) -> io::Result<SocketAddr> {
            self.inner.get_ref().get_ref().peer_addr()
        }
        /// Returns the local address of the underlying TcpStream.
        pub fn local_addr(&self) -> io::Result<SocketAddr> {
            self.inner.get_ref().get_ref().local_addr()
        }
    }

    /// A connection Future that also exposes the length-delimited framing config.
    #[must_use]
    #[pin_project]
    pub struct TcpConnect<T, Item, SinkItem, CodecFn> {
        #[pin]
        inner: T,
        codec_fn: CodecFn,
        config: length_delimited::Builder,
        ghost: PhantomData<(fn(SinkItem), fn() -> Item)>,
    }

    impl<T, Item, SinkItem, Codec, CodecFn> Future for TcpConnect<T, Item, SinkItem, CodecFn>
    where
        T: Future<Output = io::Result<TcpStream>>,
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        type Output = io::Result<Transport<TcpStream, Item, SinkItem, Codec>>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let io = ready!(self.as_mut().project().inner.poll(cx))?;
            Poll::Ready(Ok(new(self.config.new_framed(io), (self.codec_fn)())))
        }
    }

    impl<T, Item, SinkItem, CodecFn> TcpConnect<T, Item, SinkItem, CodecFn> {
        /// Returns an immutable reference to the length-delimited codec's config.
        pub fn config(&self) -> &length_delimited::Builder {
            &self.config
        }

        /// Returns a mutable reference to the length-delimited codec's config.
        pub fn config_mut(&mut self) -> &mut length_delimited::Builder {
            &mut self.config
        }
    }

    /// Connects to `addr`, wrapping the connection in a TCP transport.
    pub fn connect<A, Item, SinkItem, Codec, CodecFn>(
        addr: A,
        codec_fn: CodecFn,
    ) -> TcpConnect<impl Future<Output = io::Result<TcpStream>>, Item, SinkItem, CodecFn>
    where
        A: ToSocketAddrs,
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        TcpConnect {
            inner: TcpStream::connect(addr),
            codec_fn,
            config: LengthDelimitedCodec::builder(),
            ghost: PhantomData,
        }
    }

    /// Listens on `addr`, wrapping accepted connections in TCP transports.
    pub async fn listen<A, Item, SinkItem, Codec, CodecFn>(
        addr: A,
        codec_fn: CodecFn,
    ) -> io::Result<Incoming<Item, SinkItem, Codec, CodecFn>>
    where
        A: ToSocketAddrs,
        Item: for<'de> Deserialize<'de>,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        listen_on(TcpListener::bind(addr).await?, codec_fn).await
    }

    /// Wrap accepted connections from `listener` in TCP transports.
    pub async fn listen_on<Item, SinkItem, Codec, CodecFn>(
        listener: TcpListener,
        codec_fn: CodecFn,
    ) -> io::Result<Incoming<Item, SinkItem, Codec, CodecFn>>
    where
        Item: for<'de> Deserialize<'de>,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        let local_addr = listener.local_addr()?;
        Ok(Incoming {
            listener,
            codec_fn,
            local_addr,
            config: LengthDelimitedCodec::builder(),
            ghost: PhantomData,
        })
    }

    /// A [`TcpListener`] that wraps connections in [transports](Transport).
    #[pin_project]
    #[derive(Debug)]
    pub struct Incoming<Item, SinkItem, Codec, CodecFn> {
        listener: TcpListener,
        local_addr: SocketAddr,
        codec_fn: CodecFn,
        config: length_delimited::Builder,
        ghost: PhantomData<(fn() -> Item, fn(SinkItem), Codec)>,
    }

    impl<Item, SinkItem, Codec, CodecFn> Incoming<Item, SinkItem, Codec, CodecFn> {
        /// Returns the address being listened on.
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        /// Returns an immutable reference to the length-delimited codec's config.
        pub fn config(&self) -> &length_delimited::Builder {
            &self.config
        }

        /// Returns a mutable reference to the length-delimited codec's config.
        pub fn config_mut(&mut self) -> &mut length_delimited::Builder {
            &mut self.config
        }
    }

    impl<Item, SinkItem, Codec, CodecFn> Stream for Incoming<Item, SinkItem, Codec, CodecFn>
    where
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        type Item = io::Result<Transport<TcpStream, Item, SinkItem, Codec>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let conn: TcpStream =
                ready!(Pin::new(&mut self.as_mut().project().listener).poll_accept(cx)?).0;
            Poll::Ready(Some(Ok(new(
                self.config.new_framed(conn),
                (self.codec_fn)(),
            ))))
        }
    }
}

#[cfg(all(unix, feature = "unix"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "unix"))))]
/// Unix Domain Socket support for generic transport using Tokio.
pub mod unix {
    use {
        super::*,
        futures::ready,
        std::{marker::PhantomData, path::Path},
        tokio::net::{UnixListener, UnixStream, unix::SocketAddr},
        tokio_util::codec::length_delimited,
    };

    impl<Item, SinkItem, Codec> Transport<UnixStream, Item, SinkItem, Codec> {
        /// Returns the socket address of the remote half of the underlying [`UnixStream`].
        pub fn peer_addr(&self) -> io::Result<SocketAddr> {
            self.inner.get_ref().get_ref().peer_addr()
        }
        /// Returns the socket address of the local half of the underlying [`UnixStream`].
        pub fn local_addr(&self) -> io::Result<SocketAddr> {
            self.inner.get_ref().get_ref().local_addr()
        }
    }

    /// A connection Future that also exposes the length-delimited framing config.
    #[must_use]
    #[pin_project]
    pub struct UnixConnect<T, Item, SinkItem, CodecFn> {
        #[pin]
        inner: T,
        codec_fn: CodecFn,
        config: length_delimited::Builder,
        ghost: PhantomData<(fn(SinkItem), fn() -> Item)>,
    }

    impl<T, Item, SinkItem, Codec, CodecFn> Future for UnixConnect<T, Item, SinkItem, CodecFn>
    where
        T: Future<Output = io::Result<UnixStream>>,
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        type Output = io::Result<Transport<UnixStream, Item, SinkItem, Codec>>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let io = ready!(self.as_mut().project().inner.poll(cx))?;
            Poll::Ready(Ok(new(self.config.new_framed(io), (self.codec_fn)())))
        }
    }

    impl<T, Item, SinkItem, CodecFn> UnixConnect<T, Item, SinkItem, CodecFn> {
        /// Returns an immutable reference to the length-delimited codec's config.
        pub fn config(&self) -> &length_delimited::Builder {
            &self.config
        }

        /// Returns a mutable reference to the length-delimited codec's config.
        pub fn config_mut(&mut self) -> &mut length_delimited::Builder {
            &mut self.config
        }
    }

    /// Connects to socket named by `path`, wrapping the connection in a Unix Domain Socket
    /// transport.
    pub fn connect<P, Item, SinkItem, Codec, CodecFn>(
        path: P,
        codec_fn: CodecFn,
    ) -> UnixConnect<impl Future<Output = io::Result<UnixStream>>, Item, SinkItem, CodecFn>
    where
        P: AsRef<Path>,
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        UnixConnect {
            inner: UnixStream::connect(path),
            codec_fn,
            config: LengthDelimitedCodec::builder(),
            ghost: PhantomData,
        }
    }

    /// Listens on the socket named by `path`, wrapping accepted connections in Unix Domain Socket
    /// transports.
    pub async fn listen<P, Item, SinkItem, Codec, CodecFn>(
        path: P,
        codec_fn: CodecFn,
    ) -> io::Result<Incoming<Item, SinkItem, Codec, CodecFn>>
    where
        P: AsRef<Path>,
        Item: for<'de> Deserialize<'de>,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        listen_on(UnixListener::bind(path)?, codec_fn).await
    }

    /// Wrap accepted connections from `listener` in Unix Domain Socket transports.
    pub async fn listen_on<Item, SinkItem, Codec, CodecFn>(
        listener: UnixListener,
        codec_fn: CodecFn,
    ) -> io::Result<Incoming<Item, SinkItem, Codec, CodecFn>>
    where
        Item: for<'de> Deserialize<'de>,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        let local_addr = listener.local_addr()?;
        Ok(Incoming {
            listener,
            codec_fn,
            local_addr,
            config: LengthDelimitedCodec::builder(),
            ghost: PhantomData,
        })
    }

    /// A [`UnixListener`] that wraps connections in [transports](Transport).
    #[pin_project]
    #[derive(Debug)]
    pub struct Incoming<Item, SinkItem, Codec, CodecFn> {
        listener: UnixListener,
        local_addr: SocketAddr,
        codec_fn: CodecFn,
        config: length_delimited::Builder,
        ghost: PhantomData<(fn() -> Item, fn(SinkItem), Codec)>,
    }

    impl<Item, SinkItem, Codec, CodecFn> Incoming<Item, SinkItem, Codec, CodecFn> {
        /// Returns the the socket address being listened on.
        pub fn local_addr(&self) -> &SocketAddr {
            &self.local_addr
        }

        /// Returns an immutable reference to the length-delimited codec's config.
        pub fn config(&self) -> &length_delimited::Builder {
            &self.config
        }

        /// Returns a mutable reference to the length-delimited codec's config.
        pub fn config_mut(&mut self) -> &mut length_delimited::Builder {
            &mut self.config
        }
    }

    impl<Item, SinkItem, Codec, CodecFn> Stream for Incoming<Item, SinkItem, Codec, CodecFn>
    where
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        type Item = io::Result<Transport<UnixStream, Item, SinkItem, Codec>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let conn: UnixStream = ready!(self.as_mut().project().listener.poll_accept(cx)?).0;
            Poll::Ready(Some(Ok(new(
                self.config.new_framed(conn),
                (self.codec_fn)(),
            ))))
        }
    }

    /// A temporary `PathBuf` that lives in `std::env::temp_dir` and is removed on drop.
    pub struct TempPathBuf(std::path::PathBuf);

    impl TempPathBuf {
        /// A named socket that results in `<tempdir>/<name>`
        pub fn new<S: AsRef<str>>(name: S) -> Self {
            let mut sock = std::env::temp_dir();
            sock.push(name.as_ref());
            Self(sock)
        }

        /// Appends a random hex string to the socket name resulting in
        /// `<tempdir>/<name>_<xxxxx>`
        pub fn with_random<S: AsRef<str>>(name: S) -> Self {
            Self::new(format!("{}_{:016x}", name.as_ref(), rand::random::<u64>()))
        }
    }

    impl AsRef<std::path::Path> for TempPathBuf {
        fn as_ref(&self) -> &std::path::Path {
            self.0.as_path()
        }
    }

    impl Drop for TempPathBuf {
        fn drop(&mut self) {
            // This will remove the file pointed to by this PathBuf if it exists, however Err's can
            // be returned such as attempting to remove a non-existing file, or one which we don't
            // have permission to remove. In these cases the Err is swallowed
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use tokio_serde::formats::SymmetricalJson;

        #[test]
        fn temp_path_buf_non_random() {
            let sock = TempPathBuf::new("test");
            let mut good = std::env::temp_dir();
            good.push("test");
            assert_eq!(sock.as_ref(), good);
            assert_eq!(sock.as_ref().file_name().unwrap(), "test");
        }

        #[test]
        fn temp_path_buf_random() {
            let sock = TempPathBuf::with_random("test");
            let good = std::env::temp_dir();
            assert!(sock.as_ref().starts_with(good));
            // Since there are 16 random characters we just assert the file_name has the right name
            // and starts with the correct string 'test_'
            // file name: test_xxxxxxxxxxxxxxxx
            // test  = 4
            // _     = 1
            // <hex> = 16
            // total = 21
            let fname = sock.as_ref().file_name().unwrap().to_string_lossy();
            assert!(fname.starts_with("test_"));
            assert_eq!(fname.len(), 21);
        }

        #[test]
        fn temp_path_buf_non_existing() {
            let sock = TempPathBuf::with_random("test");
            let sock_path = std::path::PathBuf::from(sock.as_ref());

            // No actual file has been created yet
            assert!(!sock_path.exists());
            // Should not panic
            std::mem::drop(sock);
            assert!(!sock_path.exists());
        }

        #[test]
        fn temp_path_buf_existing_file() {
            let sock = TempPathBuf::with_random("test");
            let sock_path = std::path::PathBuf::from(sock.as_ref());
            let _file = std::fs::File::create(&sock).unwrap();
            assert!(sock_path.exists());
            std::mem::drop(sock);
            assert!(!sock_path.exists());
        }

        #[test]
        fn temp_path_buf_preexisting_file() {
            let mut pre_existing = std::env::temp_dir();
            pre_existing.push("test");
            let _file = std::fs::File::create(&pre_existing).unwrap();
            let sock = TempPathBuf::new("test");
            let sock_path = std::path::PathBuf::from(sock.as_ref());
            assert!(sock_path.exists());
            std::mem::drop(sock);
            assert!(!sock_path.exists());
        }

        #[tokio::test]
        async fn temp_path_buf_for_socket() {
            let sock = TempPathBuf::with_random("test");
            // Save path for testing after drop
            let sock_path = std::path::PathBuf::from(sock.as_ref());
            // create the actual socket
            let _ = listen(&sock, SymmetricalJson::<String>::default).await;
            assert!(sock_path.exists());
            std::mem::drop(sock);
            assert!(!sock_path.exists());
        }
    }
}

#[cfg(all(unix, feature = "unix", feature = "fd-passing"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "unix", feature = "fd-passing"))))]
/// Unix Domain Socket transport with file descriptor passing support.
///
/// This module provides a transport that can pass file descriptors between
/// processes using `SCM_RIGHTS` control messages over Unix domain sockets.
///
/// # Example
///
/// ## Low-level usage (async send/recv)
///
/// ```ignore
/// use tarpc::serde_transport::unix_fd;
/// use tarpc::fd::PassedFd;
/// use tokio_serde::formats::Bincode;
///
/// // Server
/// let listener = unix_fd::listen("/tmp/my.sock", Bincode::default)?;
/// // Use listener.accept() to get connections
///
/// // Client
/// let transport = unix_fd::connect("/tmp/my.sock", Bincode::default).await?;
/// ```
///
/// ## High-level usage (with tarpc client/server)
///
/// ```ignore
/// use tarpc::{client, context, server::Channel};
/// use tarpc::serde_transport::unix_fd;
/// use tokio_serde::formats::Bincode;
///
/// // Define service with ContainsFds derive
/// #[tarpc::service(derive = [tarpc::ContainsFds])]
/// trait FileService {
///     async fn read_file(fd: PassedFd) -> Vec<u8>;
/// }
///
/// // Connect using channel transport
/// let transport = unix_fd::connect_channel("/tmp/my.sock", Bincode::default).await?;
/// let client = FileServiceClient::new(client::Config::default(), transport).spawn();
/// ```
pub mod unix_fd {
    use {
        super::*,
        crate::fd::{ContainsFds, SupportsFdPassing},
        crate::fd_transport::{FdUnixStream, FdUnixListener},
        std::{
            marker::PhantomData,
            os::unix::io::OwnedFd,
            path::Path,
        },
    };

    /// A transport that supports file descriptor passing over Unix domain sockets.
    ///
    /// This transport wraps an [`FdUnixStream`] and provides async methods for
    /// sending and receiving messages with file descriptors.
    ///
    /// For integration with tarpc's high-level client/server infrastructure,
    /// use [`FdChannelTransport`] instead, which implements `Stream + Sink`.
    pub struct FdTransport<Item, SinkItem, Codec> {
        stream: FdUnixStream,
        codec: Codec,
        ghost: PhantomData<(fn() -> Item, fn(SinkItem))>,
    }

    impl<Item, SinkItem, Codec> FdTransport<Item, SinkItem, Codec> {
        /// Creates a new FD-passing transport.
        pub fn new(stream: FdUnixStream, codec: Codec) -> Self {
            Self {
                stream,
                codec,
                ghost: PhantomData,
            }
        }

        /// Returns a reference to the underlying stream.
        pub fn get_ref(&self) -> &FdUnixStream {
            &self.stream
        }

        /// Returns a mutable reference to the underlying stream.
        pub fn get_mut(&mut self) -> &mut FdUnixStream {
            &mut self.stream
        }
    }

    impl<Item, SinkItem, Codec> SupportsFdPassing for FdTransport<Item, SinkItem, Codec> {}

    impl<Item, SinkItem, Codec> FdTransport<Item, SinkItem, Codec>
    where
        Item: for<'de> Deserialize<'de> + ContainsFds,
        SinkItem: Serialize + ContainsFds,
        Codec: Serializer<SinkItem> + Deserializer<Item> + Unpin,
    {
        /// Receives a message with any associated file descriptors.
        ///
        /// This method reads a framed message from the transport and injects
        /// any received file descriptors into the deserialized message.
        pub async fn recv(&mut self) -> io::Result<Option<Item>>
        where
            Codec: tokio_serde::Deserializer<Item>,
            <Codec as tokio_serde::Deserializer<Item>>::Error: std::fmt::Debug,
        {
            use bytes::BytesMut;
            use crate::fd::MAX_FDS_PER_MESSAGE;
            use crate::fd_codec::{HEADER_SIZE, MAX_FRAME_SIZE};

            // Read header
            let mut header_buf = [0u8; 8];
            let msg = self.stream.recv_with_fds(&mut header_buf).await?;
            if msg.data.is_empty() {
                return Ok(None);
            }
            if msg.data.len() < HEADER_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "incomplete header",
                ));
            }

            let frame_length = u32::from_be_bytes([
                msg.data[0], msg.data[1], msg.data[2], msg.data[3],
            ]) as usize;
            let fd_count = u32::from_be_bytes([
                msg.data[4], msg.data[5], msg.data[6], msg.data[7],
            ]) as usize;

            if frame_length > MAX_FRAME_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("frame too large: {}", frame_length),
                ));
            }

            if fd_count > MAX_FDS_PER_MESSAGE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("too many FDs: {}", fd_count),
                ));
            }

            // Collect FDs from header read
            let mut fds = msg.fds;

            // Read body
            let mut body_buf = vec![0u8; frame_length];
            let mut total_read = msg.data.len() - HEADER_SIZE;

            // Copy any extra data from header read
            if total_read > 0 {
                let extra = &msg.data[HEADER_SIZE..];
                body_buf[..extra.len()].copy_from_slice(extra);
            }

            // Read remaining body
            while total_read < frame_length || fds.len() < fd_count {
                let body_msg = self.stream.recv_with_fds(&mut body_buf[total_read..]).await?;
                if body_msg.data.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected EOF while reading frame body",
                    ));
                }
                total_read += body_msg.data.len();
                fds.extend(body_msg.fds);
            }

            // Deserialize using tokio_serde::Deserializer trait
            let frame_bytes = BytesMut::from(&body_buf[..]);
            let message: Item = Deserializer::deserialize(Pin::new(&mut self.codec), &frame_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e)))?;

            // Inject FDs
            let fd_vec: Vec<_> = fds.into_iter().take(fd_count).collect();
            message.inject_fds(fd_vec);

            Ok(Some(message))
        }

        /// Sends a message with any associated file descriptors.
        ///
        /// This method extracts file descriptors from the message, serializes it,
        /// and sends both the data and FDs over the transport.
        pub async fn send(&mut self, item: SinkItem) -> io::Result<()>
        where
            Codec: tokio_serde::Serializer<SinkItem>,
            <Codec as tokio_serde::Serializer<SinkItem>>::Error: std::fmt::Debug,
        {
            use bytes::BufMut;
            use std::os::unix::io::{AsRawFd, BorrowedFd};
            use crate::fd::MAX_FDS_PER_MESSAGE;

            // Extract FDs before serialization
            let fds = item.extract_fds();
            let fd_count = fds.len();

            if fd_count > MAX_FDS_PER_MESSAGE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("too many FDs: {}", fd_count),
                ));
            }

            // Serialize using tokio_serde::Serializer trait
            let encoded = Serializer::serialize(Pin::new(&mut self.codec), &item)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e)))?;

            // Build frame
            let mut frame = Vec::with_capacity(8 + encoded.len());
            frame.put_u32(encoded.len() as u32);
            frame.put_u32(fd_count as u32);
            frame.extend_from_slice(&encoded);

            // Borrow FDs
            let borrowed_fds: Vec<BorrowedFd<'_>> = fds.iter()
                .map(|fd| unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
                .collect();

            // Send
            self.stream.send_all_with_fds(&frame, &borrowed_fds).await?;

            Ok(())
        }
    }

    /// Internal state for the read operation
    enum ReadState {
        /// Ready to read a new message
        Idle,
        /// Reading the header
        ReadingHeader {
            buf: [u8; 8],
            fds: Vec<OwnedFd>,
        },
        /// Reading the message body
        ReadingBody {
            frame_length: usize,
            fd_count: usize,
            buf: Vec<u8>,
            pos: usize,
            fds: Vec<OwnedFd>,
        },
    }

    /// Internal state for the write operation
    enum WriteState {
        /// Ready to accept a new message
        Idle,
        /// Sending data
        Sending {
            data: Vec<u8>,
            pos: usize,
            fds: Vec<OwnedFd>,
            fds_sent: bool,
        },
    }

    /// A transport that implements `Stream + Sink` for integration with tarpc's
    /// high-level client/server infrastructure.
    ///
    /// This wraps [`FdUnixStream`] and provides poll-based methods compatible
    /// with the tarpc `Transport` trait.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tarpc::{client, context, server::Channel};
    /// use tarpc::serde_transport::unix_fd;
    /// use tokio_serde::formats::Bincode;
    ///
    /// // Connect and create a channel transport
    /// let transport = unix_fd::connect_channel("/tmp/my.sock", Bincode::default).await?;
    ///
    /// // Use with tarpc client
    /// let client = MyServiceClient::new(client::Config::default(), transport).spawn();
    /// ```
    pub struct FdChannelTransport<Item, SinkItem, Codec> {
        stream: FdUnixStream,
        codec: Codec,
        read_state: ReadState,
        write_state: WriteState,
        ghost: PhantomData<(fn() -> Item, fn(SinkItem))>,
    }

    impl<Item, SinkItem, Codec> FdChannelTransport<Item, SinkItem, Codec> {
        /// Creates a new channel transport from an FdUnixStream.
        pub fn new(stream: FdUnixStream, codec: Codec) -> Self {
            Self {
                stream,
                codec,
                read_state: ReadState::Idle,
                write_state: WriteState::Idle,
                ghost: PhantomData,
            }
        }

        /// Returns a reference to the underlying stream.
        pub fn get_ref(&self) -> &FdUnixStream {
            &self.stream
        }
    }

    impl<Item, SinkItem, Codec> SupportsFdPassing for FdChannelTransport<Item, SinkItem, Codec> {}

    // Implement Stream for FdChannelTransport
    impl<Item, SinkItem, Codec> Stream for FdChannelTransport<Item, SinkItem, Codec>
    where
        Item: for<'de> Deserialize<'de> + ContainsFds + Unpin,
        SinkItem: Serialize + ContainsFds,
        Codec: Serializer<SinkItem> + Deserializer<Item> + Unpin,
        Codec: tokio_serde::Deserializer<Item>,
        <Codec as tokio_serde::Deserializer<Item>>::Error: std::fmt::Debug,
    {
        type Item = io::Result<Item>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            use bytes::BytesMut;
            use crate::fd::MAX_FDS_PER_MESSAGE;
            use crate::fd_codec::{HEADER_SIZE, MAX_FRAME_SIZE};

            let this = self.get_mut();

            loop {
                match &mut this.read_state {
                    ReadState::Idle => {
                        // Start reading header
                        this.read_state = ReadState::ReadingHeader {
                            buf: [0u8; 8],
                            fds: Vec::new(),
                        };
                    }
                    ReadState::ReadingHeader { buf, fds } => {
                        // Try to read header
                        let mut guard = match this.stream.inner().poll_read_ready(cx) {
                            Poll::Ready(Ok(guard)) => guard,
                            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                            Poll::Pending => return Poll::Pending,
                        };

                        match guard.try_io(|inner| recv_with_fds_sync(inner.get_ref(), buf)) {
                            Ok(Ok(msg)) => {
                                if msg.data.is_empty() {
                                    this.read_state = ReadState::Idle;
                                    return Poll::Ready(None);
                                }

                                fds.extend(msg.fds);

                                if msg.data.len() < HEADER_SIZE {
                                    // Need more header data - continue reading
                                    continue;
                                }

                                let frame_length = u32::from_be_bytes([
                                    msg.data[0], msg.data[1], msg.data[2], msg.data[3],
                                ]) as usize;
                                let fd_count = u32::from_be_bytes([
                                    msg.data[4], msg.data[5], msg.data[6], msg.data[7],
                                ]) as usize;

                                if frame_length > MAX_FRAME_SIZE {
                                    this.read_state = ReadState::Idle;
                                    return Poll::Ready(Some(Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("frame too large: {}", frame_length),
                                    ))));
                                }

                                if fd_count > MAX_FDS_PER_MESSAGE {
                                    this.read_state = ReadState::Idle;
                                    return Poll::Ready(Some(Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("too many FDs: {}", fd_count),
                                    ))));
                                }

                                // Transition to reading body
                                let mut body_buf = vec![0u8; frame_length];
                                let extra_len = msg.data.len() - HEADER_SIZE;
                                if extra_len > 0 {
                                    let extra = &msg.data[HEADER_SIZE..];
                                    body_buf[..extra_len].copy_from_slice(extra);
                                }

                                let collected_fds = std::mem::take(fds);
                                this.read_state = ReadState::ReadingBody {
                                    frame_length,
                                    fd_count,
                                    buf: body_buf,
                                    pos: extra_len,
                                    fds: collected_fds,
                                };
                            }
                            Ok(Err(e)) => {
                                this.read_state = ReadState::Idle;
                                return Poll::Ready(Some(Err(e)));
                            }
                            Err(_would_block) => {
                                // Continue loop to re-poll
                                continue;
                            }
                        }
                    }
                    ReadState::ReadingBody { frame_length, fd_count, buf, pos, fds } => {
                        // Check if we have all data
                        if *pos >= *frame_length && fds.len() >= *fd_count {
                            // Deserialize
                            let frame_bytes = BytesMut::from(&buf[..*frame_length]);
                            let message: Item = match Deserializer::deserialize(
                                Pin::new(&mut this.codec),
                                &frame_bytes,
                            ) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    this.read_state = ReadState::Idle;
                                    return Poll::Ready(Some(Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("{:?}", e),
                                    ))));
                                }
                            };

                            // Inject FDs
                            let fd_vec: Vec<_> = std::mem::take(fds)
                                .into_iter()
                                .take(*fd_count)
                                .collect();
                            message.inject_fds(fd_vec);

                            this.read_state = ReadState::Idle;
                            return Poll::Ready(Some(Ok(message)));
                        }

                        // Need more data
                        let mut guard = match this.stream.inner().poll_read_ready(cx) {
                            Poll::Ready(Ok(guard)) => guard,
                            Poll::Ready(Err(e)) => {
                                this.read_state = ReadState::Idle;
                                return Poll::Ready(Some(Err(e)));
                            }
                            Poll::Pending => return Poll::Pending,
                        };

                        match guard.try_io(|inner| recv_with_fds_sync(inner.get_ref(), &mut buf[*pos..])) {
                            Ok(Ok(msg)) => {
                                if msg.data.is_empty() {
                                    this.read_state = ReadState::Idle;
                                    return Poll::Ready(Some(Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "unexpected EOF while reading frame body",
                                    ))));
                                }
                                *pos += msg.data.len();
                                fds.extend(msg.fds);
                            }
                            Ok(Err(e)) => {
                                this.read_state = ReadState::Idle;
                                return Poll::Ready(Some(Err(e)));
                            }
                            Err(_would_block) => {
                                continue;
                            }
                        }
                    }
                }
            }
        }
    }

    // Helper function to access recv_with_fds_sync from fd_transport
    fn recv_with_fds_sync(socket: &std::os::unix::net::UnixStream, buf: &mut [u8]) -> io::Result<crate::fd_transport::FdMessage> {
        crate::fd_transport::recv_with_fds_sync(socket, buf)
    }

    fn send_with_fds_sync(
        socket: &std::os::unix::net::UnixStream,
        data: &[u8],
        fds: &[std::os::unix::io::BorrowedFd<'_>],
    ) -> io::Result<usize> {
        crate::fd_transport::send_with_fds_sync(socket, data, fds)
    }

    // Implement Sink for FdChannelTransport
    impl<Item, SinkItem, Codec> Sink<SinkItem> for FdChannelTransport<Item, SinkItem, Codec>
    where
        Item: for<'de> Deserialize<'de> + ContainsFds,
        SinkItem: Serialize + ContainsFds + Unpin,
        Codec: Serializer<SinkItem> + Deserializer<Item> + Unpin,
        Codec: tokio_serde::Serializer<SinkItem>,
        <Codec as tokio_serde::Serializer<SinkItem>>::Error: std::fmt::Debug,
    {
        type Error = io::Error;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            match &this.write_state {
                WriteState::Idle => Poll::Ready(Ok(())),
                WriteState::Sending { .. } => {
                    // Need to flush before accepting new data
                    Pin::new(this).poll_flush(cx)
                }
            }
        }

        fn start_send(self: Pin<&mut Self>, item: SinkItem) -> Result<(), Self::Error> {
            use bytes::BufMut;
            use crate::fd::MAX_FDS_PER_MESSAGE;

            let this = self.get_mut();

            // Extract FDs before serialization
            let fds = item.extract_fds();
            let fd_count = fds.len();

            if fd_count > MAX_FDS_PER_MESSAGE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("too many FDs: {}", fd_count),
                ));
            }

            // Serialize using tokio_serde::Serializer trait
            let encoded = Serializer::serialize(Pin::new(&mut this.codec), &item)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e)))?;

            // Build frame
            let mut frame = Vec::with_capacity(8 + encoded.len());
            frame.put_u32(encoded.len() as u32);
            frame.put_u32(fd_count as u32);
            frame.extend_from_slice(&encoded);

            this.write_state = WriteState::Sending {
                data: frame,
                pos: 0,
                fds,
                fds_sent: false,
            };

            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            use std::os::unix::io::{AsRawFd, BorrowedFd};

            let this = self.get_mut();

            loop {
                match &mut this.write_state {
                    WriteState::Idle => return Poll::Ready(Ok(())),
                    WriteState::Sending { data, pos, fds, fds_sent } => {
                        if *pos >= data.len() {
                            this.write_state = WriteState::Idle;
                            return Poll::Ready(Ok(()));
                        }

                        let mut guard = match this.stream.inner().poll_write_ready(cx) {
                            Poll::Ready(Ok(guard)) => guard,
                            Poll::Ready(Err(e)) => {
                                this.write_state = WriteState::Idle;
                                return Poll::Ready(Err(e));
                            }
                            Poll::Pending => return Poll::Pending,
                        };

                        let fds_to_send: Vec<BorrowedFd<'_>> = if !*fds_sent {
                            fds.iter()
                                .map(|fd| unsafe { BorrowedFd::borrow_raw(fd.as_raw_fd()) })
                                .collect()
                        } else {
                            Vec::new()
                        };

                        match guard.try_io(|inner| {
                            send_with_fds_sync(inner.get_ref(), &data[*pos..], &fds_to_send)
                        }) {
                            Ok(Ok(n)) => {
                                if n == 0 {
                                    this.write_state = WriteState::Idle;
                                    return Poll::Ready(Err(io::Error::new(
                                        io::ErrorKind::WriteZero,
                                        "failed to write any data",
                                    )));
                                }
                                *pos += n;
                                *fds_sent = true;
                            }
                            Ok(Err(e)) => {
                                this.write_state = WriteState::Idle;
                                return Poll::Ready(Err(e));
                            }
                            Err(_would_block) => {
                                continue;
                            }
                        }
                    }
                }
            }
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            // First flush any pending data
            futures::ready!(self.poll_flush(cx))?;
            Poll::Ready(Ok(()))
        }
    }

    /// Connects to a Unix socket with FD-passing support.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tarpc::serde_transport::unix_fd;
    /// use tokio_serde::formats::Bincode;
    ///
    /// let mut transport = unix_fd::connect("/tmp/my.sock", Bincode::default).await?;
    /// transport.send(my_message).await?;
    /// let response = transport.recv().await?;
    /// ```
    pub async fn connect<P, Item, SinkItem, Codec, CodecFn>(
        path: P,
        codec_fn: CodecFn,
    ) -> io::Result<FdTransport<Item, SinkItem, Codec>>
    where
        P: AsRef<Path>,
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: FnOnce() -> Codec,
    {
        let stream = FdUnixStream::connect(path).await?;
        Ok(FdTransport::new(stream, codec_fn()))
    }

    /// A listener that accepts FD-passing connections.
    pub struct Incoming<Item, SinkItem, Codec, CodecFn> {
        listener: FdUnixListener,
        local_addr: std::os::unix::net::SocketAddr,
        codec_fn: CodecFn,
        ghost: PhantomData<(fn() -> Item, fn(SinkItem), Codec)>,
    }

    /// Listens on a Unix socket with FD-passing support.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tarpc::serde_transport::unix_fd;
    /// use tokio_serde::formats::Bincode;
    ///
    /// let listener = unix_fd::listen("/tmp/my.sock", Bincode::default)?;
    /// loop {
    ///     let mut transport = listener.accept().await?;
    ///     // Handle connection
    /// }
    /// ```
    pub fn listen<P, Item, SinkItem, Codec, CodecFn>(
        path: P,
        codec_fn: CodecFn,
    ) -> io::Result<Incoming<Item, SinkItem, Codec, CodecFn>>
    where
        P: AsRef<Path>,
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: Fn() -> Codec,
    {
        let listener = FdUnixListener::bind(path)?;
        let local_addr = listener.local_addr()?;
        Ok(Incoming {
            listener,
            codec_fn,
            local_addr,
            ghost: PhantomData,
        })
    }

    impl<Item, SinkItem, Codec, CodecFn> Incoming<Item, SinkItem, Codec, CodecFn> {
        /// Returns the local address this listener is bound to.
        pub fn local_addr(&self) -> &std::os::unix::net::SocketAddr {
            &self.local_addr
        }

        /// Accepts a new connection, returning a low-level `FdTransport`.
        ///
        /// Use this for custom send/recv handling.
        pub async fn accept(&self) -> io::Result<FdTransport<Item, SinkItem, Codec>>
        where
            CodecFn: Fn() -> Codec,
        {
            let stream = self.listener.accept().await?;
            Ok(FdTransport::new(stream, (self.codec_fn)()))
        }

        /// Accepts a new connection, returning an `FdChannelTransport`.
        ///
        /// This transport implements `Stream + Sink` for integration with
        /// tarpc's high-level client/server infrastructure.
        pub async fn accept_channel(&self) -> io::Result<FdChannelTransport<Item, SinkItem, Codec>>
        where
            CodecFn: Fn() -> Codec,
        {
            let stream = self.listener.accept().await?;
            Ok(FdChannelTransport::new(stream, (self.codec_fn)()))
        }
    }

    impl<Item, SinkItem, Codec, CodecFn> SupportsFdPassing
        for Incoming<Item, SinkItem, Codec, CodecFn>
    {
    }

    /// Connects to a Unix socket with FD-passing support, returning a channel transport.
    ///
    /// This returns an `FdChannelTransport` that implements `Stream + Sink`,
    /// which can be used directly with tarpc's high-level client/server infrastructure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tarpc::{client, context};
    /// use tarpc::serde_transport::unix_fd;
    /// use tokio_serde::formats::Bincode;
    ///
    /// let transport = unix_fd::connect_channel("/tmp/my.sock", Bincode::default).await?;
    /// let client = MyServiceClient::new(client::Config::default(), transport).spawn();
    /// let result = client.my_method(context::current(), arg).await?;
    /// ```
    pub async fn connect_channel<P, Item, SinkItem, Codec, CodecFn>(
        path: P,
        codec_fn: CodecFn,
    ) -> io::Result<FdChannelTransport<Item, SinkItem, Codec>>
    where
        P: AsRef<Path>,
        Item: for<'de> Deserialize<'de>,
        SinkItem: Serialize,
        Codec: Serializer<SinkItem> + Deserializer<Item>,
        CodecFn: FnOnce() -> Codec,
    {
        let stream = FdUnixStream::connect(path).await?;
        Ok(FdChannelTransport::new(stream, codec_fn()))
    }

    /// Re-export TempPathBuf for convenience
    pub use super::unix::TempPathBuf;
}

#[cfg(test)]
mod tests {
    use super::Transport;
    use assert_matches::assert_matches;
    use futures::{Sink, Stream, task::*};
    #[cfg(any(feature = "tcp", all(unix, feature = "unix")))]
    use futures::{SinkExt, StreamExt};
    use pin_utils::pin_mut;
    use std::{
        io::{self, Cursor},
        pin::Pin,
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio_serde::formats::SymmetricalJson;

    fn ctx() -> Context<'static> {
        Context::from_waker(noop_waker_ref())
    }

    struct TestIo(Cursor<Vec<u8>>);

    impl AsyncRead for TestIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            AsyncRead::poll_read(Pin::new(&mut self.0), cx, buf)
        }
    }

    impl AsyncWrite for TestIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            AsyncWrite::poll_write(Pin::new(&mut self.0), cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            AsyncWrite::poll_flush(Pin::new(&mut self.0), cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            AsyncWrite::poll_shutdown(Pin::new(&mut self.0), cx)
        }
    }

    #[test]
    fn close() {
        let (tx, _rx) = crate::transport::channel::bounded::<(), ()>(0);
        pin_mut!(tx);
        assert_matches!(tx.as_mut().poll_close(&mut ctx()), Poll::Ready(Ok(())));
        assert_matches!(tx.as_mut().start_send(()), Err(_));
    }

    #[test]
    fn test_stream() {
        let data: &[u8] = b"\x00\x00\x00\x18\"Test one, check check.\"";
        let transport = Transport::from((
            TestIo(Cursor::new(Vec::from(data))),
            SymmetricalJson::<String>::default(),
        ));
        pin_mut!(transport);

        assert_matches!(
            transport.as_mut().poll_next(&mut ctx()),
            Poll::Ready(Some(Ok(ref s))) if s == "Test one, check check.");
        assert_matches!(transport.as_mut().poll_next(&mut ctx()), Poll::Ready(None));
    }

    #[test]
    fn test_sink() {
        let writer = Cursor::new(vec![]);
        let mut transport = Box::pin(Transport::from((
            TestIo(writer),
            SymmetricalJson::<String>::default(),
        )));

        assert_matches!(
            transport.as_mut().poll_ready(&mut ctx()),
            Poll::Ready(Ok(()))
        );
        assert_matches!(
            transport
                .as_mut()
                .start_send("Test one, check check.".into()),
            Ok(())
        );
        assert_matches!(
            transport.as_mut().poll_flush(&mut ctx()),
            Poll::Ready(Ok(()))
        );
        assert_eq!(
            transport.get_ref().0.get_ref(),
            b"\x00\x00\x00\x18\"Test one, check check.\""
        );
    }

    #[cfg(feature = "tcp")]
    #[tokio::test]
    async fn tcp() -> io::Result<()> {
        use super::tcp;

        let mut listener = tcp::listen("0.0.0.0:0", SymmetricalJson::<String>::default).await?;
        let addr = listener.local_addr();
        tokio::spawn(async move {
            let mut transport = listener.next().await.unwrap().unwrap();
            let message = transport.next().await.unwrap().unwrap();
            transport.send(message).await.unwrap();
        });
        let mut transport = tcp::connect(addr, SymmetricalJson::<String>::default).await?;
        transport.send(String::from("test")).await?;
        assert_matches!(transport.next().await, Some(Ok(s)) if s == "test");
        assert_matches!(transport.next().await, None);
        Ok(())
    }

    #[cfg(feature = "tcp")]
    #[tokio::test]
    async fn tcp_on_existing_transport() -> io::Result<()> {
        use super::tcp;

        let transport = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
        let mut listener = tcp::listen_on(transport, SymmetricalJson::<String>::default).await?;
        let addr = listener.local_addr();
        tokio::spawn(async move {
            let mut transport = listener.next().await.unwrap().unwrap();
            let message = transport.next().await.unwrap().unwrap();
            transport.send(message).await.unwrap();
        });
        let mut transport = tcp::connect(addr, SymmetricalJson::<String>::default).await?;
        transport.send(String::from("test")).await?;
        assert_matches!(transport.next().await, Some(Ok(s)) if s == "test");
        assert_matches!(transport.next().await, None);
        Ok(())
    }

    #[cfg(all(unix, feature = "unix"))]
    #[tokio::test]
    async fn uds() -> io::Result<()> {
        use super::unix;

        let sock = unix::TempPathBuf::with_random("uds");
        let mut listener = unix::listen(&sock, SymmetricalJson::<String>::default).await?;
        tokio::spawn(async move {
            let mut transport = listener.next().await.unwrap().unwrap();
            let message = transport.next().await.unwrap().unwrap();
            transport.send(message).await.unwrap();
        });
        let mut transport = unix::connect(&sock, SymmetricalJson::<String>::default).await?;
        transport.send(String::from("test")).await?;
        assert_matches!(transport.next().await, Some(Ok(s)) if s == "test");
        assert_matches!(transport.next().await, None);
        Ok(())
    }

    #[cfg(all(unix, feature = "unix"))]
    #[tokio::test]
    async fn uds_on_existing_transport() -> io::Result<()> {
        use super::unix;

        let sock = unix::TempPathBuf::with_random("uds");
        let transport = tokio::net::UnixListener::bind(&sock)?;
        let mut listener = unix::listen_on(transport, SymmetricalJson::<String>::default).await?;
        tokio::spawn(async move {
            let mut transport = listener.next().await.unwrap().unwrap();
            let message = transport.next().await.unwrap().unwrap();
            transport.send(message).await.unwrap();
        });
        let mut transport = unix::connect(&sock, SymmetricalJson::<String>::default).await?;
        transport.send(String::from("test")).await?;
        assert_matches!(transport.next().await, Some(Ok(s)) if s == "test");
        assert_matches!(transport.next().await, None);
        Ok(())
    }
}
