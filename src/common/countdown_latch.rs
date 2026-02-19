// MIT License
//
// Copyright (c) 2022 Eric Thill
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! A synchronization tool that allows one or more threads to wait until a set of operations being performed in other threads completes.
//!
//! A CountDownLatch is initialized with a count. The wait method blocks until the current count reaches zero due to invocations of the count_down() method,
//! after which all waiting threads are released and any subsequent invocations of wait return immediately.
//!
//! # Example
//!
//! ```rust
//! use crate::common::countdown_latch::CountDownLatch;
//! use std::sync::Arc;
//! use std::thread;
//! use std::time::Duration;
//!
//! // create a CountDownLatch with count=5
//! let latch = Arc::new(CountDownLatch::new(5));
//! // create 5 threads that sleep for a variable amount of time before calling latch.count_down()
//! for i in 0..5 {
//!   let tlatch = Arc::clone(&latch);
//!   thread::spawn(move || {
//!     thread::sleep(Duration::from_millis(i * 100));
//!     println!("unlatching {}", i);
//!     tlatch.count_down();
//!   });
//! }
//!
//! // await completion of the latch
//! latch.await();
//! // print done, which will appear in the console after all "unlatching" messages
//! println!("done");
//! ```

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

/// A CountDownLatch is used to wait for a given number of tasks to be completed which may be running in multiple threads
pub struct CountDownLatch {
    remaining: AtomicUsize,
    tx: mpsc::SyncSender<()>,
    rx: Mutex<mpsc::Receiver<()>>,
}

impl CountDownLatch {
    /// Construct a CountDownLatch with the given count
    pub fn new(count: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel(count);
        Self {
            remaining: AtomicUsize::new(count),
            tx,
            rx: Mutex::new(rx),
        }
    }

    /// Decrement the count by one
    pub fn count_down(&self) {
        // single send on a channel
        self.tx.send(()).unwrap();
    }

    /// Get the current count
    pub fn get_count(&self) -> usize {
        // try to drain the channel
        let lock = self.rx.try_lock();
        if let Ok(rx) = lock {
            while self.remaining.load(Ordering::SeqCst) > 0 && rx.try_recv().is_ok() {
                self.remaining.fetch_sub(1, Ordering::SeqCst);
            }
        }
        // return the remaining count
        self.remaining.load(Ordering::SeqCst)
    }

    /// Block until the count reaches 0
    pub fn wait(&self) {
        // get lock, indefinite wait
        let rx = self.rx.lock().unwrap();
        // while remaining > 0, receive on the channel and decrement count
        while self.remaining.load(Ordering::SeqCst) > 0 {
            rx.recv().unwrap();
            self.remaining.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CountDownLatch;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn one_thread_await() {
        let latch = Arc::new(CountDownLatch::new(5));
        assert_eq!(latch.get_count(), 5);

        for i in 0..5 {
            let latch = Arc::clone(&latch);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(i * 100));
                println!("unlatch {}", i);
                latch.count_down();
            });
        }

        let timeout = Instant::now() + Duration::from_secs(2);
        while Instant::now() < timeout && latch.get_count() > 0 {
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(latch.get_count(), 0);

        latch.wait();
    }

    #[test]
    fn multi_thread_wait() {
        let delayed_latch = Arc::new(CountDownLatch::new(5));
        for i in 0..5 {
            let delayed_latch = Arc::clone(&delayed_latch);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(i * 100));
                println!("delayed unlatch {}", i);
                delayed_latch.count_down();
            });
        }

        let awaited_latch = Arc::new(CountDownLatch::new(3));
        for i in 0..3 {
            let delayed_latch = Arc::clone(&delayed_latch);
            let awaited_latch = Arc::clone(&awaited_latch);
            thread::spawn(move || {
                delayed_latch.wait();
                println!("awaited unlatch {}", i);
                awaited_latch.count_down();
            });
        }

        awaited_latch.wait();
        assert_eq!(delayed_latch.get_count(), 0);
        assert_eq!(awaited_latch.get_count(), 0);
    }
}
