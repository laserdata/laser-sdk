// The zero-overhead guarantee as an assertion instead of prose: the raw
// publish path must hand a caller's `Bytes` payload to the Iggy message
// without copying the body, and building the message must stay within a
// pinned allocation budget. A regression that adds a body copy or a fresh
// allocation on this path fails here deterministically, no benchmark noise.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingAllocator;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// The message body must be the caller's buffer, never a copy: `Bytes` is
// refcounted, so pointer identity is the proof.
#[test]
fn given_a_bytes_payload_when_built_into_a_message_then_the_body_is_not_copied() {
    let payload = bytes::Bytes::from_static(&[0x42; 4096]);
    let message = iggy::prelude::IggyMessage::builder()
        .payload(payload.clone())
        .build()
        .expect("a bare payload builds");
    assert_eq!(
        message.payload.as_ptr(),
        payload.as_ptr(),
        "the raw publish path must hand the caller's buffer through, not copy it"
    );
}

// The allocation budget of assembling one raw no-header message from a ready
// payload. The budget is the measured cost of the message struct itself (the
// builder's internals), pinned so a change that starts allocating per byte of
// body, or per message where it did not before, fails loud. Deliberately a
// ceiling, not an exact count: allocator-internal jitter is not what this
// guards.
#[test]
fn given_a_ready_payload_when_a_message_is_assembled_then_should_stay_in_the_allocation_budget() {
    let payload = bytes::Bytes::from_static(&[0x42; 65536]);
    // Warm up whatever lazy statics the builder touches.
    let _ = iggy::prelude::IggyMessage::builder()
        .payload(payload.clone())
        .build()
        .expect("builds");

    let before = ALLOCATIONS.load(Ordering::SeqCst);
    let message = iggy::prelude::IggyMessage::builder()
        .payload(payload.clone())
        .build()
        .expect("builds");
    let spent = ALLOCATIONS.load(Ordering::SeqCst) - before;
    // The body is 64 KiB. A copy would show as a large-allocation step. The
    // struct assembly itself costs a handful at most.
    assert!(
        spent <= 4,
        "raw message assembly allocated {spent} times; the budget is 4 (did the body start copying?)"
    );
    assert_eq!(message.payload.len(), payload.len());
}
