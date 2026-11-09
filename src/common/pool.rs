//! A small, bounded pool of byte buffers for fan-out message encoding.
//!
//! Buffers grow while a message is encoded and come back with whatever capacity
//! that message needed. The pool keeps at most [`MAX_POOLED_BUFFERS`] of them,
//! and only those whose capacity is at most [`MAX_POOLED_CAPACITY`], so the
//! memory it retains is bounded however large the messages it has seen.

use crate::common::sync::lock;
use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

/// Buffers kept for reuse. Enough for the lanes' workers plus the main thread.
const MAX_POOLED_BUFFERS: usize = 32;
/// Largest buffer kept for reuse; a bigger one is freed when dropped.
const MAX_POOLED_CAPACITY: usize = 64 * 1024;

static BYTE_POOL: BufferPool = BufferPool::new(MAX_POOLED_BUFFERS, MAX_POOLED_CAPACITY);

struct BufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
    max_buffers: usize,
    max_capacity: usize,
}

impl BufferPool {
    const fn new(max_buffers: usize, max_capacity: usize) -> Self {
        Self {
            buffers: Mutex::new(Vec::new()),
            max_buffers,
            max_capacity,
        }
    }

    fn get(&'static self, size: usize) -> PooledBuffer {
        let mut buf = lock(&self.buffers).pop().unwrap_or_default();
        buf.reserve(size);
        PooledBuffer { buf, pool: self }
    }

    fn put(&self, mut buf: Vec<u8>) {
        if buf.capacity() == 0 || buf.capacity() > self.max_capacity {
            return;
        }
        buf.clear();
        let mut buffers = lock(&self.buffers);
        if buffers.len() < self.max_buffers {
            buffers.push(buf);
        }
    }

    #[cfg(test)]
    fn retained(&self) -> (usize, usize) {
        let buffers = lock(&self.buffers);
        (buffers.len(), buffers.iter().map(Vec::capacity).sum())
    }
}

/// An empty buffer that returns to the pool when dropped.
pub struct PooledBuffer {
    buf: Vec<u8>,
    pool: &'static BufferPool,
}

impl Deref for PooledBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Vec<u8> {
        &self.buf
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        self.pool.put(std::mem::take(&mut self.buf));
    }
}

/// An empty buffer with room for at least `size` bytes.
pub fn get_pooled_buffer(size: usize) -> PooledBuffer {
    BYTE_POOL.get(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reuses_buffers_and_bounds_retention() {
        static POOL: BufferPool = BufferPool::new(4, 4096);

        // Sequential encode/drop cycles reuse one allocation.
        let first = {
            let mut buf = POOL.get(1024);
            buf.extend_from_slice(&[1; 1500]);
            buf.as_ptr()
        };
        for _ in 0..16 {
            let buf = POOL.get(1024);
            assert!(buf.is_empty());
            assert_eq!(buf.as_ptr(), first);
        }
        assert_eq!(POOL.retained().0, 1);

        // An oversized buffer is freed rather than kept.
        for _ in 0..16 {
            let mut buf = POOL.get(1024);
            buf.resize(1 << 20, 1);
        }
        assert_eq!(POOL.retained(), (0, 0));

        // At most `max_buffers` are kept, however many were live at once.
        let live: Vec<_> = (0..10).map(|_| POOL.get(512)).collect();
        drop(live);
        let (count, bytes) = POOL.retained();
        assert_eq!(count, 4);
        assert!(bytes <= 4 * 4096);
    }
}
