//! Shared-memory waiter registry for push-based read_with_poll wakeup.
//!
//! ## Design
//!
//! A hash-indexed registry in shared memory: queue names are hashed to a bucket,
//! and each bucket holds a singly-linked list of WaiterSlots. Free slots form a
//! separate linked list rooted at `free_head`.
//!
//! notify_waiters(queue) hashes the name, walks only the slots in that bucket,
//! and calls SetLatch on matches — O(waiters_for_this_queue), not O(MAX_WAITERS).
//! register/unregister are O(1) amortised (free-list pop/push + bucket prepend/unlink).
//!
//! MAX_WAITERS=4096 accommodates the largest PostgreSQL max_connections settings.
//! BUCKET_COUNT=256 keeps average bucket length short even under heavy concurrency.
//!
//! ## Lifecycle
//!
//! _PG_init installs shmem_request_hook + shmem_startup_hook.
//!
//! read_with_poll creates a WaiterGuard that registers its latch before entering
//! the WaitLatch loop; it unregisters on drop (normal return, panic, or
//! query-cancel unwind).
//!
//! send_full / send_batch_internal call register_notify_after_commit which
//! registers a XactCallback. When the sender's transaction commits (and messages
//! become visible), the callback calls notify_waiters → SetLatch on each reader
//! waiting on that queue.
//!
//! ## Degraded mode
//!
//! If the extension is not in shared_preload_libraries, REGISTRY_READY stays false
//! and all operations skip gracefully. read_with_poll falls back to timeout-only
//! WaitLatch polling (still correct, higher latency).

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use pgrx::pg_sys;

const BUCKET_COUNT: usize = 256;  // power of 2 — bitmasked in hash
const MAX_WAITERS: usize = 4096;  // upper bound ≈ max_connections
const NAME_LEN: usize = 49;       // 48 chars (validate_name) + null

// ---------------------------------------------------------------------------
// Shared-memory structs
// ---------------------------------------------------------------------------

#[repr(C)]
struct WaiterSlot {
    latch: usize,             // *mut pg_sys::Latch as usize; 0 = slot is free
    pid: i32,                 // backend PID of the waiter
    next: i32,                // next index in bucket or free list; -1 = end
    queue_name: [u8; NAME_LEN],
}

#[repr(C)]
struct WaiterRegistry {
    active_count: i32,
    free_head: i32,                    // head of free-slot list; -1 = full
    buckets: [i32; BUCKET_COUNT],      // head of per-bucket waiter list; -1 = empty
    slots: [WaiterSlot; MAX_WAITERS],
}

static mut REGISTRY: *mut WaiterRegistry = std::ptr::null_mut();
static mut REGISTRY_LOCK: *mut pg_sys::LWLock = std::ptr::null_mut();
static REGISTRY_READY: AtomicBool = AtomicBool::new(false);

static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

const TRANCHE: &std::ffi::CStr = c"queue_waiters";
const SHMEM_KEY: &std::ffi::CStr = c"queue_waiter_registry";

fn registry_size() -> usize {
    std::mem::size_of::<WaiterRegistry>()
}

// FNV-1a, masked to BUCKET_COUNT (must be power of 2)
fn bucket_for(queue_name: &[u8]) -> usize {
    let mut h: u32 = 2166136261;
    for &b in queue_name {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    h as usize & (BUCKET_COUNT - 1)
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

/// Install shmem hooks.  Call from _PG_init.
pub unsafe fn install_hooks() {
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
        pg_sys::shmem_request_hook = Some(on_shmem_request);

        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(on_shmem_startup);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn on_shmem_request() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
            prev();
        }
        pg_sys::RequestAddinShmemSpace(registry_size());
        pg_sys::RequestNamedLWLockTranche(TRANCHE.as_ptr(), 1);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn on_shmem_startup() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_STARTUP_HOOK {
            prev();
        }

        // AddinShmemInitLock is at index 21 in MainLWLockArray (PostgreSQL convention).
        let addin_lock = &raw mut (*pg_sys::MainLWLockArray.add(21)).lock;
        pg_sys::LWLockAcquire(addin_lock, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let mut found = false;
        let ptr = pg_sys::ShmemInitStruct(
            SHMEM_KEY.as_ptr(),
            registry_size(),
            &mut found,
        ) as *mut WaiterRegistry;

        if !found {
            init_registry(&mut *ptr);
        }

        REGISTRY = ptr;
        REGISTRY_LOCK =
            &raw mut (*pg_sys::GetNamedLWLockTranche(TRANCHE.as_ptr())).lock;

        pg_sys::LWLockRelease(addin_lock);
        REGISTRY_READY.store(true, Ordering::Release);
    }
}

fn init_registry(reg: &mut WaiterRegistry) {
    reg.active_count = 0;
    reg.free_head = 0;
    // All buckets empty
    for b in reg.buckets.iter_mut() {
        *b = -1;
    }
    // Build the free list: slot[i].next = i+1; last slot.next = -1
    for i in 0..MAX_WAITERS {
        reg.slots[i].latch = 0;
        reg.slots[i].pid = 0;
        reg.slots[i].next = if i + 1 < MAX_WAITERS { (i + 1) as i32 } else { -1 };
        reg.slots[i].queue_name = [0u8; NAME_LEN];
    }
}

#[inline]
fn is_ready() -> bool {
    REGISTRY_READY.load(Ordering::Acquire)
}

// ---------------------------------------------------------------------------
// register / unregister — O(1)
// ---------------------------------------------------------------------------

/// Register this backend as a waiter on `queue_name`.
/// Returns Some(idx) on success, None if the registry is unavailable or full.
pub unsafe fn register(queue_name: &str) -> Option<usize> {
    if !is_ready() {
        return None;
    }
    unsafe {
        pg_sys::LWLockAcquire(REGISTRY_LOCK, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let reg = &mut *REGISTRY;

        // Pop a slot from the free list.
        if reg.free_head == -1 {
            pg_sys::LWLockRelease(REGISTRY_LOCK);
            return None;
        }
        let idx = reg.free_head as usize;
        reg.free_head = reg.slots[idx].next;

        // Initialise the slot.
        let slot = &mut reg.slots[idx];
        slot.latch = pg_sys::MyLatch as usize;
        slot.pid = pg_sys::MyProcPid;
        let bytes = queue_name.as_bytes();
        let n = bytes.len().min(NAME_LEN - 1);
        slot.queue_name[..n].copy_from_slice(&bytes[..n]);
        slot.queue_name[n] = 0;

        // Prepend to the bucket list for this queue.
        let b = bucket_for(bytes);
        slot.next = reg.buckets[b];
        reg.buckets[b] = idx as i32;

        reg.active_count += 1;
        pg_sys::LWLockRelease(REGISTRY_LOCK);
        Some(idx)
    }
}

/// Unregister slot `idx` and return it to the free list.
pub unsafe fn unregister(idx: usize) {
    if !is_ready() {
        return;
    }
    unsafe {
        pg_sys::LWLockAcquire(REGISTRY_LOCK, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let reg = &mut *REGISTRY;

        // The slot must still be active (latch != 0) to be unregistered.
        if reg.slots[idx].latch == 0 {
            pg_sys::LWLockRelease(REGISTRY_LOCK);
            return;
        }

        // Find and remove from the bucket list.
        let name_len = reg.slots[idx]
            .queue_name
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(NAME_LEN - 1);
        let b = bucket_for(&reg.slots[idx].queue_name[..name_len]);

        let mut prev: i32 = -1;
        let mut cur = reg.buckets[b];
        while cur != -1 {
            if cur as usize == idx {
                if prev == -1 {
                    reg.buckets[b] = reg.slots[idx].next;
                } else {
                    reg.slots[prev as usize].next = reg.slots[idx].next;
                }
                break;
            }
            prev = cur;
            cur = reg.slots[cur as usize].next;
        }

        // Return slot to the free list.
        reg.slots[idx].latch = 0;
        reg.slots[idx].next = reg.free_head;
        reg.free_head = idx as i32;
        reg.active_count -= 1;

        pg_sys::LWLockRelease(REGISTRY_LOCK);
    }
}

// ---------------------------------------------------------------------------
// notify_waiters — O(waiters_for_this_queue)
// ---------------------------------------------------------------------------

/// Wake every backend waiting on `queue_name`.
/// Acquires a shared LWLock: multiple senders can call this concurrently.
/// SetLatch is safe under shared lock — it only atomically sets a flag in the
/// target PGPROC and sends SIGUSR1; it does not acquire any locks.
pub unsafe fn notify_waiters(queue_name: &str) {
    if !is_ready() {
        return;
    }
    unsafe {
        pg_sys::LWLockAcquire(REGISTRY_LOCK, pg_sys::LWLockMode::LW_SHARED);
        let reg = &*REGISTRY;

        if reg.active_count > 0 {
            let qb = queue_name.as_bytes();
            let b = bucket_for(qb);
            let mut cur = reg.buckets[b];
            while cur != -1 {
                let slot = &reg.slots[cur as usize];
                // Queue name match: same bytes and null-terminated at the right place.
                let n = qb.len();
                if slot.latch != 0
                    && n < NAME_LEN
                    && slot.queue_name[..n] == *qb
                    && slot.queue_name[n] == 0
                {
                    pg_sys::SetLatch(slot.latch as *mut pg_sys::Latch);
                }
                cur = slot.next;
            }
        }

        pg_sys::LWLockRelease(REGISTRY_LOCK);
    }
}

// ---------------------------------------------------------------------------
// XactCallback: fire notify_waiters after the sender's transaction commits
// ---------------------------------------------------------------------------

/// Register a post-commit callback for `queue_name`.
///
/// Fires when the top-level transaction commits — exactly when inserted messages
/// become visible to other backends. The queue name is allocated in
/// TopTransactionContext (valid through callbacks, freed by PG after).
pub unsafe fn register_notify_after_commit(queue_name: &str) {
    if !is_ready() {
        return;
    }
    unsafe {
        let bytes = queue_name.as_bytes();
        let len = bytes.len();
        let p =
            pg_sys::MemoryContextAlloc(pg_sys::TopTransactionContext, len + 1) as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), p, len);
        *p.add(len) = 0;
        pg_sys::RegisterXactCallback(Some(on_xact_commit), p as *mut c_void);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn on_xact_commit(event: pg_sys::XactEvent::Type, arg: *mut c_void) {
    // Unregister before doing any work. RegisterXactCallback allocates the list node in
    // TopMemoryContext, so without this the list grows by one entry per send call and
    // every subsequent commit walks the entire accumulated list. PostgreSQL's
    // CallXactCallbacks captures item->next before invoking the callback, so
    // unregistering the current item from inside the callback is explicitly safe.
    unsafe { pg_sys::UnregisterXactCallback(Some(on_xact_commit), arg) };

    if event == pg_sys::XactEvent::XACT_EVENT_COMMIT
        || event == pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT
    {
        unsafe {
            let p = arg as *const u8;
            let len = (0usize..).take_while(|&i| *p.add(i) != 0).count();
            if let Ok(name) = std::str::from_utf8(std::slice::from_raw_parts(p, len)) {
                notify_waiters(name);
            }
        }
    }
    // Memory freed by TopTransactionContext cleanup after callbacks complete.
}

// ---------------------------------------------------------------------------
// WaiterGuard — RAII unregistration
// ---------------------------------------------------------------------------

/// Unregisters this backend's waiter slot on drop.
/// Ensures cleanup on normal return, panic unwind, or query-cancel unwind.
pub struct WaiterGuard(Option<usize>);

impl WaiterGuard {
    pub unsafe fn new(queue_name: &str) -> Self {
        WaiterGuard(unsafe { register(queue_name) })
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if let Some(idx) = self.0.take() {
            unsafe { unregister(idx) };
        }
    }
}
