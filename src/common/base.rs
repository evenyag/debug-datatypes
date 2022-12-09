// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub use bitvec::prelude;

// `Lsb0` provides the best codegen for bit manipulation,
// see https://github.com/bitvecto-rs/bitvec/blob/main/doc/order/Lsb0.md
pub type BitVec = prelude::BitVec<u8>;

use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Bytes buffer.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Bytes(bytes::Bytes);

impl From<bytes::Bytes> for Bytes {
    fn from(bytes: bytes::Bytes) -> Bytes {
        Bytes(bytes)
    }
}

impl From<&[u8]> for Bytes {
    fn from(bytes: &[u8]) -> Bytes {
        Bytes(bytes::Bytes::copy_from_slice(bytes))
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Bytes {
        Bytes(bytes::Bytes::from(bytes))
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl PartialEq<Vec<u8>> for Bytes {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.0 == other
    }
}

impl PartialEq<Bytes> for Vec<u8> {
    fn eq(&self, other: &Bytes) -> bool {
        *self == other.0
    }
}

impl PartialEq<[u8]> for Bytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.0 == other
    }
}

impl PartialEq<Bytes> for [u8] {
    fn eq(&self, other: &Bytes) -> bool {
        self == other.0
    }
}

/// String buffer that can hold arbitrary encoding string (only support UTF-8 now).
///
/// Now this buffer is restricted to only hold valid UTF-8 string (only allow constructing `StringBytes`
/// from String or str). We may support other encoding in the future.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StringBytes(bytes::Bytes);

impl StringBytes {
    /// View this string as UTF-8 string slice.
    ///
    /// # Safety
    /// We only allow constructing `StringBytes` from String/str, so the inner
    /// buffer must holds valid UTF-8.
    pub fn as_utf8(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl From<String> for StringBytes {
    fn from(string: String) -> StringBytes {
        StringBytes(bytes::Bytes::from(string))
    }
}

impl From<&str> for StringBytes {
    fn from(string: &str) -> StringBytes {
        StringBytes(bytes::Bytes::copy_from_slice(string.as_bytes()))
    }
}

impl PartialEq<String> for StringBytes {
    fn eq(&self, other: &String) -> bool {
        self.0 == other.as_bytes()
    }
}

impl PartialEq<StringBytes> for String {
    fn eq(&self, other: &StringBytes) -> bool {
        self.as_bytes() == other.0
    }
}

impl PartialEq<str> for StringBytes {
    fn eq(&self, other: &str) -> bool {
        self.0 == other.as_bytes()
    }
}

impl PartialEq<StringBytes> for str {
    fn eq(&self, other: &StringBytes) -> bool {
        self.as_bytes() == other.0
    }
}

impl Serialize for StringBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_utf8().serialize(serializer)
    }
}

// Custom Deserialize to ensure UTF-8 check is always done.
impl<'de> Deserialize<'de> for StringBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(StringBytes::from(s))
    }
}
