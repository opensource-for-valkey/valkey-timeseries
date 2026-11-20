use std::borrow::Borrow;
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;

const STACK_SIZE: usize = 64;

/// Enum to minimize allocations when searching the posting index.
///
/// Because of the way keys are constructed in the posting index, we have to construct
/// temporary values for lookups. We attempt to avoid allocations by creating these
/// search keys in a stack buffer when possible.
#[derive(Clone)]
pub(crate) enum KeyBuffer {
    Stack([u8; STACK_SIZE], usize),
    Heap(Box<[u8]>),
}

impl KeyBuffer {
    pub(super) fn for_prefix(data: &str) -> Self {
        let len = data.len();
        if len + 1 < STACK_SIZE {
            let mut buf = [0u8; STACK_SIZE];
            buf[..len].copy_from_slice(data.as_bytes());
            buf[len] = b'=';
            KeyBuffer::Stack(buf, len + 1)
        } else {
            let mut v = Vec::with_capacity(len + 1);
            v.extend_from_slice(data.as_bytes());
            v.push(b'=');
            KeyBuffer::Heap(v.into_boxed_slice())
        }
    }

    pub(super) fn for_label_value_prefix(label: &str, value_prefix: &str) -> Self {
        let label_len = label.len();
        let prefix_len = value_prefix.len();
        let len = label_len + prefix_len;
        if len + 1 < STACK_SIZE {
            let mut buf = [0u8; STACK_SIZE];
            buf[..label_len].copy_from_slice(label.as_bytes());
            buf[label_len] = b'=';
            let ofs = label_len + 1;
            // value_prefix should be copied after the '=' byte at index `label_len + 1`.
            buf[ofs..ofs + prefix_len].copy_from_slice(value_prefix.as_bytes());
            KeyBuffer::Stack(buf, len + 1)
        } else {
            let mut v = Vec::with_capacity(len + 1);
            v.extend_from_slice(label.as_bytes());
            v.push(b'=');
            v.extend_from_slice(value_prefix.as_bytes());
            KeyBuffer::Heap(v.into_boxed_slice())
        }
    }

    pub(super) fn for_label_value(label_name: &str, value: &str) -> Self {
        let label_len = label_name.len();
        let total_len = label_len + 1 + value.len();
        if total_len < STACK_SIZE {
            let mut buf = [0u8; STACK_SIZE];
            buf[..label_len].copy_from_slice(label_name.as_bytes());
            buf[label_len] = b'=';
            buf[label_len + 1..total_len].copy_from_slice(value.as_bytes());
            // Ensure the stack buffer includes the terminating NUL byte to match
            // the heap path (which pushes a trailing '\0').
            buf[total_len] = b'\0';
            KeyBuffer::Stack(buf, total_len + 1)
        } else {
            // `total_len` plus the trailing NUL.
            let mut v = Vec::with_capacity(total_len + 1);
            v.extend_from_slice(label_name.as_bytes());
            v.push(b'=');
            v.extend_from_slice(value.as_bytes());
            v.push(b'\0');
            KeyBuffer::Heap(v.into_boxed_slice())
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            KeyBuffer::Stack(_, len) => *len,
            KeyBuffer::Heap(boxed) => boxed.len(),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        let buf = self.as_bytes();
        let str_buf = &buf[..self.len()];
        std::str::from_utf8(str_buf).unwrap()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            KeyBuffer::Stack(buf, len) => &buf[..*len],
            KeyBuffer::Heap(boxed) => boxed,
        }
    }
}

impl Default for KeyBuffer {
    fn default() -> Self {
        KeyBuffer::Stack([0u8; STACK_SIZE], 0)
    }
}

impl Deref for KeyBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl From<&str> for KeyBuffer {
    fn from(s: &str) -> Self {
        KeyBuffer::for_prefix(s)
    }
}

impl FromStr for KeyBuffer {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(KeyBuffer::for_prefix(s))
    }
}

impl Borrow<[u8]> for KeyBuffer {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Display for KeyBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
