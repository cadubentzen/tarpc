// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! File descriptor passing support for Unix domain sockets.
//!
//! This module provides types and traits for passing file descriptors between
//! processes using Unix domain sockets with `SCM_RIGHTS` ancillary messages.
//!
//! # Overview
//!
//! File descriptors (FDs) are process-local handles to kernel resources like files,
//! sockets, and DMABUFs. On Linux, FDs can be passed between processes via Unix
//! domain sockets using the `SCM_RIGHTS` mechanism.
//!
//! This module provides:
//! - [`PassedFd`]: A wrapper type for FDs that can be included in RPC messages
//! - [`ContainsFds`]: A trait for types that may contain FDs to be passed
//!
//! # Usage
//!
//! ```ignore
//! use tarpc::fd::PassedFd;
//! use std::os::unix::io::OwnedFd;
//!
//! #[tarpc::service]
//! trait DmaBufService {
//!     async fn import_buffer(fd: PassedFd, size: usize) -> Result<(), String>;
//! }
//! ```
//!
//! # How It Works
//!
//! When a message containing `PassedFd` is sent:
//! 1. The FDs are extracted from the message before serialization
//! 2. The message is serialized with placeholder indices for FDs
//! 3. The serialized data and FDs are sent together via `sendmsg` with `SCM_RIGHTS`
//!
//! When receiving:
//! 1. Data and FDs are received together via `recvmsg`
//! 2. The message is deserialized with placeholder indices
//! 3. FDs are injected back into the message, replacing the placeholders
//!
//! # Important Notes
//!
//! - FD passing only works over Unix domain sockets, not TCP or other transports
//! - Using `PassedFd` with a non-FD-passing transport will result in a compile error
//! - FDs are duplicated by the kernel; the sender's FD remains valid after sending
//! - The receiver gets a new FD number pointing to the same kernel resource

use std::cell::Cell;
use std::fmt;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

#[cfg(feature = "serde1")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum number of file descriptors that can be passed in a single message.
///
/// This limit is imposed by the Linux kernel (`SCM_MAX_FD`).
pub const MAX_FDS_PER_MESSAGE: usize = 253;

/// A file descriptor that can be passed between processes via RPC.
///
/// `PassedFd` wraps an [`OwnedFd`] and provides custom serialization that allows
/// the FD to be passed out-of-band via `SCM_RIGHTS` control messages.
///
/// # Ownership
///
/// `PassedFd` takes ownership of the file descriptor. When the `PassedFd` is dropped,
/// the underlying FD will be closed (unless it has been extracted via [`into_fd`](Self::into_fd)).
///
/// # Serialization
///
/// When serialized, `PassedFd` writes only a placeholder index. The actual FD
/// is passed separately via the socket's ancillary data. This is handled
/// automatically by the FD-passing transport.
///
/// # Example
///
/// ```ignore
/// use std::os::unix::io::OwnedFd;
/// use std::fs::File;
/// use tarpc::fd::PassedFd;
///
/// // Create an FD from a file
/// let file = File::open("/dev/null").unwrap();
/// let fd: OwnedFd = file.into();
///
/// // Wrap it for RPC passing
/// let passed = PassedFd::new(fd);
///
/// // Later, extract the FD
/// let fd: OwnedFd = passed.into_fd();
/// ```
pub struct PassedFd {
    /// The underlying owned file descriptor.
    /// None after the FD has been extracted for sending.
    fd: Cell<Option<OwnedFd>>,
    /// Index assigned during FD extraction, used for serialization.
    /// Set to u32::MAX initially (invalid), assigned during extraction.
    index: Cell<u32>,
}

impl PassedFd {
    /// Invalid index value, indicating the FD hasn't been extracted yet.
    const INVALID_INDEX: u32 = u32::MAX;

    /// Creates a new `PassedFd` from an owned file descriptor.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::os::unix::io::OwnedFd;
    /// use tarpc::fd::PassedFd;
    ///
    /// let fd: OwnedFd = /* ... */;
    /// let passed = PassedFd::new(fd);
    /// ```
    pub fn new(fd: OwnedFd) -> Self {
        Self {
            fd: Cell::new(Some(fd)),
            index: Cell::new(Self::INVALID_INDEX),
        }
    }

    /// Creates a `PassedFd` from a raw file descriptor.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `fd` is a valid file descriptor and that
    /// ownership is being transferred to this `PassedFd`.
    pub unsafe fn from_raw(fd: RawFd) -> Self {
        Self::new(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    /// Creates a `PassedFd` with a pre-assigned index.
    ///
    /// This is used during deserialization when the FD will be injected later.
    pub(crate) fn with_index(index: u32) -> Self {
        Self {
            fd: Cell::new(None),
            index: Cell::new(index),
        }
    }

    /// Returns the index assigned to this FD.
    ///
    /// Returns `None` if no index has been assigned yet.
    pub(crate) fn index(&self) -> Option<u32> {
        let idx = self.index.get();
        if idx == Self::INVALID_INDEX {
            None
        } else {
            Some(idx)
        }
    }

    /// Sets the index for this FD.
    ///
    /// Called during FD extraction before serialization.
    pub(crate) fn set_index(&self, index: u32) {
        self.index.set(index);
    }

    /// Takes the underlying FD out of this wrapper.
    ///
    /// Returns `None` if the FD has already been taken.
    pub(crate) fn take_fd(&self) -> Option<OwnedFd> {
        self.fd.take()
    }

    /// Injects an FD into this wrapper.
    ///
    /// Called during FD injection after deserialization.
    pub(crate) fn inject_fd(&self, fd: OwnedFd) {
        self.fd.set(Some(fd));
    }

    /// Checks if this `PassedFd` currently holds an FD.
    pub fn has_fd(&self) -> bool {
        // We need to peek without taking
        let fd = self.fd.take();
        let has = fd.is_some();
        self.fd.set(fd);
        has
    }

    /// Consumes the `PassedFd` and returns the underlying [`OwnedFd`].
    ///
    /// # Panics
    ///
    /// Panics if the FD has already been extracted or hasn't been injected yet.
    /// In practice, this shouldn't happen if `PassedFd` is used correctly through
    /// the RPC system.
    pub fn into_fd(self) -> OwnedFd {
        self.fd
            .take()
            .expect("PassedFd: FD not available (already taken or not yet injected)")
    }

    /// Tries to consume the `PassedFd` and return the underlying [`OwnedFd`].
    ///
    /// Returns `None` if the FD has already been extracted or hasn't been injected.
    pub fn try_into_fd(self) -> Option<OwnedFd> {
        self.fd.take()
    }

    /// Returns the raw file descriptor number.
    ///
    /// # Panics
    ///
    /// Panics if the FD is not currently available.
    pub fn as_raw_fd(&self) -> RawFd {
        let fd = self.fd.take();
        let raw = fd.as_ref().map(|f| f.as_raw_fd()).expect("FD not available");
        self.fd.set(fd);
        raw
    }
}

impl fmt::Debug for PassedFd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fd = self.fd.take();
        let raw_fd = fd.as_ref().map(|f| f.as_raw_fd());
        self.fd.set(fd);

        f.debug_struct("PassedFd")
            .field("fd", &raw_fd)
            .field("index", &self.index.get())
            .finish()
    }
}

impl From<OwnedFd> for PassedFd {
    fn from(fd: OwnedFd) -> Self {
        Self::new(fd)
    }
}

impl From<PassedFd> for OwnedFd {
    fn from(passed: PassedFd) -> Self {
        passed.into_fd()
    }
}

impl AsFd for PassedFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // SAFETY: The FD is valid as long as self is alive
        unsafe { BorrowedFd::borrow_raw(self.as_raw_fd()) }
    }
}

impl AsRawFd for PassedFd {
    fn as_raw_fd(&self) -> RawFd {
        PassedFd::as_raw_fd(self)
    }
}

impl IntoRawFd for PassedFd {
    fn into_raw_fd(self) -> RawFd {
        self.into_fd().into_raw_fd()
    }
}

#[cfg(feature = "serde1")]
impl Serialize for PassedFd {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize only the index. The actual FD is passed out-of-band.
        let index = self.index.get();
        if index == Self::INVALID_INDEX {
            return Err(serde::ser::Error::custom(
                "PassedFd: index not assigned. FDs must be extracted before serialization.",
            ));
        }
        index.serialize(serializer)
    }
}

#[cfg(feature = "serde1")]
impl<'de> Deserialize<'de> for PassedFd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize the index. The actual FD will be injected later.
        let index = u32::deserialize(deserializer)?;
        Ok(Self::with_index(index))
    }
}

/// A trait for types that may contain file descriptors to be passed.
///
/// This trait is used by the FD-passing transport to extract FDs before
/// serialization and inject them after deserialization.
///
/// # Deriving
///
/// For structs containing `PassedFd` fields (including nested structs),
/// you can derive this trait:
///
/// ```ignore
/// use tarpc::fd::{PassedFd, ContainsFds};
///
/// #[derive(ContainsFds)]
/// struct MyRequest {
///     name: String,
///     buffer: PassedFd,
/// }
/// ```
///
/// # Manual Implementation
///
/// For types with complex FD layouts, you can implement this trait manually:
///
/// ```ignore
/// use tarpc::fd::{PassedFd, ContainsFds};
/// use std::os::unix::io::OwnedFd;
///
/// struct CustomType {
///     fds: Vec<PassedFd>,
/// }
///
/// impl ContainsFds for CustomType {
///     fn extract_fds(&self) -> Vec<OwnedFd> {
///         self.fds.iter().filter_map(|pfd| pfd.take_fd()).collect()
///     }
///
///     fn inject_fds(&self, mut fds: Vec<OwnedFd>) {
///         for pfd in &self.fds {
///             if let Some(idx) = pfd.index() {
///                 if let Some(fd) = fds.get_mut(idx as usize).and_then(|slot| {
///                     // Take from vec by swapping with a dummy
///                     Some(std::mem::replace(slot, unsafe {
///                         OwnedFd::from_raw_fd(-1)
///                     }))
///                 }) {
///                     pfd.inject_fd(fd);
///                 }
///             }
///         }
///     }
///
///     fn fd_count(&self) -> usize {
///         self.fds.len()
///     }
/// }
/// ```
pub trait ContainsFds {
    /// Extracts all file descriptors from this value.
    ///
    /// This method assigns sequential indices to each FD and returns them
    /// in order. The indices are stored in the `PassedFd` wrappers for
    /// serialization.
    ///
    /// Returns the FDs in the order they should be passed via `SCM_RIGHTS`.
    fn extract_fds(&self) -> Vec<OwnedFd> {
        let mut next_index = 0u32;
        self.extract_fds_with_index(&mut next_index)
    }

    /// Extracts FDs starting from the given index.
    ///
    /// This method is used internally to assign sequential indices across
    /// multiple fields. The `next_index` is updated to the next available index.
    fn extract_fds_with_index(&self, next_index: &mut u32) -> Vec<OwnedFd>;

    /// Injects file descriptors into this value after deserialization.
    ///
    /// The FDs should be in the same order as they were extracted.
    /// Each `PassedFd` uses its stored index to find the correct FD.
    fn inject_fds(&self, fds: Vec<OwnedFd>) {
        let mut fds: Vec<Option<OwnedFd>> = fds.into_iter().map(Some).collect();
        self.inject_fds_from(&mut fds);
    }

    /// Injects file descriptors from a mutable slice of options.
    ///
    /// This method is called by `inject_fds` after converting the vector.
    /// Each `PassedFd` takes its FD from the slot at its stored index.
    fn inject_fds_from(&self, fds: &mut [Option<OwnedFd>]);

    /// Returns the number of file descriptors in this value.
    fn fd_count(&self) -> usize;
}

/// Blanket implementation for types that don't contain FDs.
impl<T> ContainsFds for T
where
    T: NoFds,
{
    fn extract_fds_with_index(&self, _next_index: &mut u32) -> Vec<OwnedFd> {
        Vec::new()
    }

    fn inject_fds_from(&self, _fds: &mut [Option<OwnedFd>]) {
        // No FDs to inject
    }

    fn fd_count(&self) -> usize {
        0
    }
}

/// Marker trait for types that definitely don't contain file descriptors.
///
/// This is auto-implemented for common types and can be implemented for
/// custom types that are known not to contain `PassedFd`.
///
/// Types implementing `NoFds` get a blanket implementation of `ContainsFds`
/// that does nothing.
pub trait NoFds {}

// Implement NoFds for primitive types
impl NoFds for () {}
impl NoFds for bool {}
impl NoFds for char {}
impl NoFds for i8 {}
impl NoFds for i16 {}
impl NoFds for i32 {}
impl NoFds for i64 {}
impl NoFds for i128 {}
impl NoFds for isize {}
impl NoFds for u8 {}
impl NoFds for u16 {}
impl NoFds for u32 {}
impl NoFds for u64 {}
impl NoFds for u128 {}
impl NoFds for usize {}
impl NoFds for f32 {}
impl NoFds for f64 {}
impl NoFds for String {}
impl NoFds for &str {}
impl<T: NoFds> NoFds for Vec<T> {}
impl<T: NoFds> NoFds for Option<T> {}
impl<T: NoFds, E: NoFds> NoFds for Result<T, E> {}
impl<T: NoFds> NoFds for Box<T> {}
impl<T: NoFds + ?Sized> NoFds for std::sync::Arc<T> {}
impl<T: NoFds + ?Sized> NoFds for std::rc::Rc<T> {}
impl<T: NoFds> NoFds for std::cell::RefCell<T> {}
impl<T: NoFds + Copy> NoFds for std::cell::Cell<T> {}

// Implement NoFds for tuples
impl<A: NoFds> NoFds for (A,) {}
impl<A: NoFds, B: NoFds> NoFds for (A, B) {}
impl<A: NoFds, B: NoFds, C: NoFds> NoFds for (A, B, C) {}
impl<A: NoFds, B: NoFds, C: NoFds, D: NoFds> NoFds for (A, B, C, D) {}
impl<A: NoFds, B: NoFds, C: NoFds, D: NoFds, E: NoFds> NoFds for (A, B, C, D, E) {}

// Implement NoFds for arrays
impl<T: NoFds, const N: usize> NoFds for [T; N] {}
impl<T: NoFds> NoFds for [T] {}

// Implement ContainsFds for PassedFd itself
impl ContainsFds for PassedFd {
    fn extract_fds_with_index(&self, next_index: &mut u32) -> Vec<OwnedFd> {
        self.set_index(*next_index);
        *next_index += 1;
        if let Some(fd) = self.take_fd() {
            vec![fd]
        } else {
            vec![]
        }
    }

    fn inject_fds_from(&self, fds: &mut [Option<OwnedFd>]) {
        if let Some(idx) = self.index() {
            let idx = idx as usize;
            if idx < fds.len() {
                if let Some(fd) = fds[idx].take() {
                    self.inject_fd(fd);
                }
            }
        }
    }

    fn fd_count(&self) -> usize {
        1
    }
}

// Implement ContainsFds for Vec<PassedFd>
impl ContainsFds for Vec<PassedFd> {
    fn extract_fds_with_index(&self, next_index: &mut u32) -> Vec<OwnedFd> {
        let mut fds = Vec::with_capacity(self.len());
        for pfd in self {
            pfd.set_index(*next_index);
            *next_index += 1;
            if let Some(fd) = pfd.take_fd() {
                fds.push(fd);
            }
        }
        fds
    }

    fn inject_fds_from(&self, fds: &mut [Option<OwnedFd>]) {
        for pfd in self {
            if let Some(idx) = pfd.index() {
                let idx = idx as usize;
                if idx < fds.len() {
                    if let Some(fd) = fds[idx].take() {
                        pfd.inject_fd(fd);
                    }
                }
            }
        }
    }

    fn fd_count(&self) -> usize {
        self.len()
    }
}

/// Marker trait for transports that support file descriptor passing.
///
/// This trait is used to enforce at compile time that `PassedFd` can only
/// be used with transports that actually support FD passing.
///
/// # Example
///
/// ```ignore
/// use tarpc::fd::SupportsFdPassing;
///
/// fn requires_fd_support<T: SupportsFdPassing>(transport: T) {
///     // This function can only be called with FD-passing transports
/// }
/// ```
pub trait SupportsFdPassing {}

/// Error type for FD passing operations.
#[derive(Debug, thiserror::Error)]
pub enum FdError {
    /// FD passing is not supported on this transport.
    #[error("FD passing not supported on this transport")]
    NotSupported,

    /// Too many file descriptors in a single message.
    #[error("too many file descriptors ({count}), maximum is {max}")]
    TooManyFds {
        /// The number of FDs attempted.
        count: usize,
        /// The maximum allowed.
        max: usize,
    },

    /// FD count mismatch between sent and received.
    #[error("FD count mismatch: expected {expected}, received {actual}")]
    CountMismatch {
        /// Expected number of FDs.
        expected: usize,
        /// Actual number received.
        actual: usize,
    },

    /// I/O error during FD passing.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// Implement ContainsFds for tarpc wrapper types
// These implementations delegate to the inner message type.

impl<T: ContainsFds> ContainsFds for crate::ClientMessage<T> {
    fn extract_fds_with_index(&self, next_index: &mut u32) -> Vec<OwnedFd> {
        match self {
            crate::ClientMessage::Request(req) => req.extract_fds_with_index(next_index),
            crate::ClientMessage::Cancel { .. } => Vec::new(),
        }
    }

    fn inject_fds_from(&self, fds: &mut [Option<OwnedFd>]) {
        match self {
            crate::ClientMessage::Request(req) => req.inject_fds_from(fds),
            crate::ClientMessage::Cancel { .. } => {}
        }
    }

    fn fd_count(&self) -> usize {
        match self {
            crate::ClientMessage::Request(req) => req.fd_count(),
            crate::ClientMessage::Cancel { .. } => 0,
        }
    }
}

impl<T: ContainsFds> ContainsFds for crate::Request<T> {
    fn extract_fds_with_index(&self, next_index: &mut u32) -> Vec<OwnedFd> {
        // context::Context doesn't contain FDs, only the message does
        self.message.extract_fds_with_index(next_index)
    }

    fn inject_fds_from(&self, fds: &mut [Option<OwnedFd>]) {
        self.message.inject_fds_from(fds)
    }

    fn fd_count(&self) -> usize {
        self.message.fd_count()
    }
}

impl<T: ContainsFds> ContainsFds for crate::Response<T> {
    fn extract_fds_with_index(&self, next_index: &mut u32) -> Vec<OwnedFd> {
        match &self.message {
            Ok(msg) => msg.extract_fds_with_index(next_index),
            Err(_) => Vec::new(),
        }
    }

    fn inject_fds_from(&self, fds: &mut [Option<OwnedFd>]) {
        match &self.message {
            Ok(msg) => msg.inject_fds_from(fds),
            Err(_) => {}
        }
    }

    fn fd_count(&self) -> usize {
        match &self.message {
            Ok(msg) => msg.fd_count(),
            Err(_) => 0,
        }
    }
}

// ServerError doesn't contain FDs
impl NoFds for crate::ServerError {}

// context::Context doesn't contain FDs
impl NoFds for crate::context::Context {}

// trace::Context doesn't contain FDs
impl NoFds for crate::trace::Context {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_passed_fd_creation() {
        let file = File::open("/dev/null").unwrap();
        let fd: OwnedFd = file.into();
        let raw = fd.as_raw_fd();

        let passed = PassedFd::new(fd);
        assert!(passed.has_fd());
        assert_eq!(passed.as_raw_fd(), raw);
    }

    #[test]
    fn test_passed_fd_into() {
        let file = File::open("/dev/null").unwrap();
        let fd: OwnedFd = file.into();

        let passed = PassedFd::new(fd);
        let _recovered: OwnedFd = passed.into_fd();
    }

    #[test]
    fn test_passed_fd_extract_inject() {
        let file = File::open("/dev/null").unwrap();
        let fd: OwnedFd = file.into();

        let passed = PassedFd::new(fd);

        // Extract
        let extracted = passed.extract_fds();
        assert_eq!(extracted.len(), 1);
        assert!(!passed.has_fd());
        assert_eq!(passed.index(), Some(0));

        // Inject
        passed.inject_fds(extracted);
        assert!(passed.has_fd());
        // Note: FD number might be different after inject in real scenario,
        // but here it's the same since we're in the same process
    }

    #[test]
    fn test_no_fds_types() {
        // Test that NoFds types work with ContainsFds
        let s = String::from("hello");
        assert_eq!(s.fd_count(), 0);
        assert!(s.extract_fds().is_empty());

        let v: Vec<i32> = vec![1, 2, 3];
        assert_eq!(v.fd_count(), 0);
    }

    #[test]
    fn test_vec_passed_fd() {
        let f1 = File::open("/dev/null").unwrap();
        let f2 = File::open("/dev/null").unwrap();

        let vec = vec![PassedFd::new(f1.into()), PassedFd::new(f2.into())];

        assert_eq!(vec.fd_count(), 2);

        let extracted = vec.extract_fds();
        assert_eq!(extracted.len(), 2);

        // Both should have indices assigned
        assert_eq!(vec[0].index(), Some(0));
        assert_eq!(vec[1].index(), Some(1));
    }

    #[cfg(feature = "serde1")]
    #[test]
    fn test_serialization_requires_index() {
        let file = File::open("/dev/null").unwrap();
        let passed = PassedFd::new(file.into());

        // Should fail because index isn't set
        let result = serde_json::to_string(&passed);
        assert!(result.is_err());

        // Set index and try again
        passed.set_index(0);
        let json = serde_json::to_string(&passed).unwrap();
        assert_eq!(json, "0");
    }

    #[cfg(feature = "serde1")]
    #[test]
    fn test_deserialization() {
        let json = "42";
        let passed: PassedFd = serde_json::from_str(json).unwrap();

        assert_eq!(passed.index(), Some(42));
        assert!(!passed.has_fd()); // FD not injected yet
    }
}
