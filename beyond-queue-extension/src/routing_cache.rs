//! Generation-based routing cache in shared memory.
//!
//! Caches topic_subscriptions lookups: routing_key → Vec<queue_name>.
//! 256 direct-mapped slots (FNV-1a hash-indexed). A single global generation
//! counter is incremented on any topic_subscriptions write; slots stamped with a
//! stale generation are treated as misses.
//!
//! ## Concurrency
//!
//! - lookup:     shared LWLock (multiple readers concurrent)
//! - insert:     exclusive LWLock
//! - invalidate: exclusive LWLock — O(1) generation bump only
//!
//! Hash collisions evict: the newer routing key simply overwrites the slot.
//! Correctness is unaffected — a miss just re-runs the topic_subscriptions query.
//!
//! ## Degraded mode
//!
//! If the extension is not in shared_preload_libraries, CACHE_READY stays false
//! and all operations are no-ops (every lookup returns None).

use pgrx::pg_sys;
use std::sync::atomic::{AtomicBool, Ordering};

const SLOTS: usize = 256; // power of 2 — bitmasked in hash
const MAX_QUEUES: usize = 32; // max bound queues per routing key
const KEY_LEN: usize = 256; // routing key: up to 255 chars + null
const NAME_LEN: usize = 49; // queue name: up to 48 chars + null (validate_name)

// ---------------------------------------------------------------------------
// Shared-memory structs
// ---------------------------------------------------------------------------

#[repr(C)]
struct CacheSlot {
    generation: u64,
    key_len: u16,
    queue_count: u16,
    key: [u8; KEY_LEN],
    queues: [[u8; NAME_LEN]; MAX_QUEUES],
}

#[repr(C)]
struct RoutingCache {
    generation: u64,
    slots: [CacheSlot; SLOTS],
}

static mut CACHE: *mut RoutingCache = std::ptr::null_mut();
static mut CACHE_LOCK: *mut pg_sys::LWLock = std::ptr::null_mut();
static CACHE_READY: AtomicBool = AtomicBool::new(false);

static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

const TRANCHE: &std::ffi::CStr = c"queue_routing_cache";
const SHMEM_KEY: &std::ffi::CStr = c"queue_routing_cache";

fn cache_size() -> usize {
    std::mem::size_of::<RoutingCache>()
}

// FNV-1a, masked to SLOTS (must be power of 2)
fn slot_for(key: &[u8]) -> usize {
    let mut h: u32 = 2166136261;
    for &b in key {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    h as usize & (SLOTS - 1)
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

/// Install shmem hooks. Call from _PG_init after waiter::install_hooks().
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
        pg_sys::RequestAddinShmemSpace(cache_size());
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
        let ptr = pg_sys::ShmemInitStruct(SHMEM_KEY.as_ptr(), cache_size(), &mut found)
            as *mut RoutingCache;

        if !found {
            init_cache(&mut *ptr);
        }

        CACHE = ptr;
        CACHE_LOCK = &raw mut (*pg_sys::GetNamedLWLockTranche(TRANCHE.as_ptr())).lock;

        pg_sys::LWLockRelease(addin_lock);
        CACHE_READY.store(true, Ordering::Release);
    }
}

fn init_cache(c: &mut RoutingCache) {
    // Global generation starts at 1. Uninitialised slots have generation=0,
    // so they are immediately stale without needing per-slot inspection.
    c.generation = 1;
    // Zero all slots: all integer/byte fields, zero is a valid initial value.
    unsafe {
        std::ptr::write_bytes(
            c.slots.as_mut_ptr() as *mut u8,
            0,
            std::mem::size_of_val(&c.slots),
        );
    }
}

#[inline]
fn is_ready() -> bool {
    CACHE_READY.load(Ordering::Acquire)
}

// ---------------------------------------------------------------------------
// lookup / insert / invalidate
// ---------------------------------------------------------------------------

/// Return cached queue names for `routing_key`, or None on miss / not ready.
pub unsafe fn lookup(routing_key: &str) -> Option<Vec<String>> {
    if !is_ready() {
        return None;
    }
    let key = routing_key.as_bytes();
    let klen = key.len();
    if klen >= KEY_LEN {
        return None;
    }
    let idx = slot_for(key);
    unsafe {
        pg_sys::LWLockAcquire(CACHE_LOCK, pg_sys::LWLockMode::LW_SHARED);
        let cache = &*CACHE;
        let global_gen = cache.generation;
        let slot = &cache.slots[idx];

        let hit = slot.generation == global_gen
            && slot.key_len as usize == klen
            && slot.key[..klen] == *key;

        let result = if hit {
            let count = slot.queue_count as usize;
            let mut names = Vec::with_capacity(count);
            for i in 0..count {
                let nb = &slot.queues[i];
                let nlen = nb.iter().position(|&b| b == 0).unwrap_or(NAME_LEN - 1);
                if let Ok(s) = std::str::from_utf8(&nb[..nlen]) {
                    names.push(s.to_string());
                }
            }
            Some(names)
        } else {
            None
        };

        pg_sys::LWLockRelease(CACHE_LOCK);
        result
    }
}

/// Store `queues` for `routing_key` in the cache at the current generation.
/// Silently skips if the routing key or queue count exceeds capacity.
pub unsafe fn insert(routing_key: &str, queues: &[String]) {
    if !is_ready() {
        return;
    }
    let key = routing_key.as_bytes();
    let klen = key.len();
    if klen >= KEY_LEN || queues.len() > MAX_QUEUES {
        return;
    }
    let idx = slot_for(key);
    unsafe {
        pg_sys::LWLockAcquire(CACHE_LOCK, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let cache = &mut *CACHE;
        let global_gen = cache.generation;
        let slot = &mut cache.slots[idx];

        slot.generation = global_gen;
        slot.key_len = klen as u16;
        slot.queue_count = queues.len() as u16;
        slot.key[..klen].copy_from_slice(key);
        slot.key[klen] = 0;

        for (i, name) in queues.iter().enumerate() {
            let nb = name.as_bytes();
            let nlen = nb.len().min(NAME_LEN - 1);
            slot.queues[i][..nlen].copy_from_slice(&nb[..nlen]);
            slot.queues[i][nlen] = 0;
        }

        pg_sys::LWLockRelease(CACHE_LOCK);
    }
}

/// Invalidate the entire cache by bumping the global generation counter.
/// All existing slots become stale on the next lookup.
pub unsafe fn invalidate() {
    if !is_ready() {
        return;
    }
    unsafe {
        pg_sys::LWLockAcquire(CACHE_LOCK, pg_sys::LWLockMode::LW_EXCLUSIVE);
        let next_gen = (*CACHE).generation.wrapping_add(1);
        // Skip 0: it is the sentinel value for uninitialised slots.
        (*CACHE).generation = if next_gen == 0 { 1 } else { next_gen };
        pg_sys::LWLockRelease(CACHE_LOCK);
    }
}

// ---------------------------------------------------------------------------
// pg_extern — called by the topic_subscriptions invalidation trigger
// ---------------------------------------------------------------------------

/// Called by the AFTER INSERT/UPDATE/DELETE trigger on queue.topic_subscriptions.
/// Bumps the routing cache generation so all cached routes are treated as stale.
#[pgrx::pg_extern(name = "_invalidate_routing_cache", schema = "queue", volatile)]
fn invalidate_routing_cache_fn() {
    unsafe { invalidate() };
}
