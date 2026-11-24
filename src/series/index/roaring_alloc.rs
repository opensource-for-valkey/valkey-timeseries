//! The allocator CRoaring allocates through: the module's global allocator, counted.
//!
//! Every posting bitmap, and every temporary bitmap a query builds, lives on CRoaring's heap.
//! [`roaring_heap_bytes`] is that heap's live size. It is exact where the per-db estimate in
//! `memory.rs` cannot see: spare array capacity, pools a removal leaves until the optimizer
//! shrinks them, and the size header croaring's hooks put in front of every allocation. It is
//! process-wide only, so it cannot be split by database.

use allocator_api2::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Bytes CRoaring holds, as the layouts it asked for (allocator size classes not included).
static ROARING_HEAP_BYTES: AtomicUsize = AtomicUsize::new(0);

pub fn roaring_heap_bytes() -> usize {
    ROARING_HEAP_BYTES.load(Ordering::Relaxed)
}

/// [`Global`] (the module's global allocator), with every live byte counted.
struct CountingAllocator;

unsafe impl Allocator for CountingAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let block = Global.allocate(layout)?;
        ROARING_HEAP_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        Ok(block)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let block = Global.allocate_zeroed(layout)?;
        ROARING_HEAP_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        Ok(block)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: forwarded unchanged; `ptr` was allocated by `Global` with `layout`.
        unsafe { Global.deallocate(ptr, layout) };
        ROARING_HEAP_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: forwarded unchanged, under the caller's guarantees.
        let block = unsafe { Global.grow(ptr, old_layout, new_layout) }?;
        ROARING_HEAP_BYTES.fetch_add(new_layout.size() - old_layout.size(), Ordering::Relaxed);
        Ok(block)
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        // SAFETY: forwarded unchanged, under the caller's guarantees.
        let block = unsafe { Global.shrink(ptr, old_layout, new_layout) }?;
        ROARING_HEAP_BYTES.fetch_sub(old_layout.size() - new_layout.size(), Ordering::Relaxed);
        Ok(block)
    }
}

static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;

/// Points CRoaring at [`CountingAllocator`]. Runs once, at module load, before any bitmap
/// exists: memory must be freed by the allocator that allocated it.
pub(crate) fn init_croaring_allocator() {
    static INIT: Once = Once::new();
    // SAFETY: called from module load before any CRoaring object is created, on one thread.
    INIT.call_once(|| unsafe { croaring::configure_custom_alloc(&COUNTING_ALLOCATOR) });
}

#[cfg(test)]
mod tests {
    use super::*;

    // The hooks are process-global and other tests hold bitmaps from the default allocator, so
    // these drive the allocator directly instead of installing it.
    #[test]
    fn counts_allocations_resizes_and_frees() {
        let before = roaring_heap_bytes();
        let small = Layout::from_size_align(24, 8).unwrap();
        let large = Layout::from_size_align(200, 8).unwrap();

        let block = COUNTING_ALLOCATOR.allocate(small).unwrap().cast::<u8>();
        assert_eq!(roaring_heap_bytes() - before, 24);
        let block = unsafe { COUNTING_ALLOCATOR.grow(block, small, large) }
            .unwrap()
            .cast::<u8>();
        assert_eq!(roaring_heap_bytes() - before, 200);
        let block = unsafe { COUNTING_ALLOCATOR.shrink(block, large, small) }
            .unwrap()
            .cast::<u8>();
        assert_eq!(roaring_heap_bytes() - before, 24);
        unsafe { COUNTING_ALLOCATOR.deallocate(block, small) };

        let zeroed = COUNTING_ALLOCATOR
            .allocate_zeroed(large)
            .unwrap()
            .cast::<u8>();
        assert_eq!(roaring_heap_bytes() - before, 200);
        unsafe { COUNTING_ALLOCATOR.deallocate(zeroed, large) };
        assert_eq!(roaring_heap_bytes(), before);
    }
}
