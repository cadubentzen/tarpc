// Copyright 2018 Google LLC
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! FD-aware codec constants for framing messages with file descriptor passing.
//!
//! # Wire Format
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ 4 bytes: total frame length (big-endian)│
//! ├─────────────────────────────────────────┤
//! │ 4 bytes: FD count (big-endian)          │
//! ├─────────────────────────────────────────┤
//! │ N bytes: serialized message             │
//! └─────────────────────────────────────────┘
//!
//! Ancillary Data (via SCM_RIGHTS):
//! ┌─────────────────────────────────────────┐
//! │ FD₀, FD₁, ..., FDₙ                      │
//! └─────────────────────────────────────────┘
//! ```

/// Header size: 4 bytes frame length + 4 bytes FD count
pub const HEADER_SIZE: usize = 8;
/// Maximum frame size (16 MB)
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_size() {
        assert_eq!(HEADER_SIZE, 8);
    }
}
