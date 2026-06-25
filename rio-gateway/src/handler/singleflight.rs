//! Per-process coalescing of concurrent uploads for the same
//! `(tenant, store-path)`. Pure mechanism: the in-flight registry,
//! its [`Disposition`] (Leader/Follower) split, and the bounded
//! follower wait. NO gRPC, no `PutPath` lane logic — those live in
//! [`super::put_path`] (the lane wrappers) and [`super::grpc`] (the
//! streaming pump). The split keeps the lane wrappers' diff surface
//! small and the mechanism testable in isolation.
//!
//! Differs from `rio-store/src/cas.rs` `Shared<BoxFuture>`: there the
//! work (S3 GET) is caller-independent. Here the leader OWNS a NAR
//! stream/buffer only it can upload — followers can only wait for
//! "done".
//!
//! Tenant-scoped key per `r[store.put.tenant-junction]`: even
//! idempotent-skip writes the caller's per-tenant junction row, so
//! cross-tenant uploads must each reach the store.
//!
//! Per-process only: cross-replica races still hit the store's Aborted;
//! the existing retry/wait-then-adopt loops in `handler/grpc.rs` are
//! the fallback.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rio_common::tenant::NormalizedName;
use rio_nix::store_path::{HASH_BYTES, StorePath};
use tokio::sync::Semaphore;

/// Defensive backstop on [`Follower::wait_bounded`]. NOT a liveness
/// guarantee: the leader bounds itself via `tx.send`'s per-chunk idle
/// bound (the store side); a wedged-CLIENT leader (`read_exact`
/// stalled) is its own session's problem and is NOT bounded —
/// followers wait this cap then fail-open. 30s — half the store's
/// `concurrent_put_wait` ([`rio_proto::DEFAULT_CONCURRENT_PUT_WAIT`] =
/// 60s); long enough for a leader uploading at ≥1 MB/s to finish a
/// 30 MB NAR, short enough that LB/SSH idle timeouts (typically ≥60s)
/// don't fire on the opcode-39 streaming follower's silence (worst
/// case ≈ this cap + ~6s budget poll).
///
/// Latency trade-off: a follower racing a wedged in-process leader
/// surfaces its outcome up to this cap + the post-wait QPI's own
/// transient-retry budget later than pre-singleflight (where it would
/// have polled the store directly). The QPI retry is the bound — no
/// additional retry wraps the post-wait probe. This is the cost of
/// the signal-wait optimization; the success-rate is strictly
/// no-worse.
pub(crate) const FOLLOWER_WAIT_CAP: Duration = Duration::from_secs(30);
const _: () = assert!(
    FOLLOWER_WAIT_CAP.as_millis() * 2 <= rio_proto::DEFAULT_CONCURRENT_PUT_WAIT.as_millis(),
    "FOLLOWER_WAIT_CAP must be ≤ half the store's concurrent_put_wait \
     so an LB/SSH idle timeout tuned ≥ the store wait does not fire on \
     a follower's silence"
);

/// Short cap for the opcode-44 pipeline call sites
/// (`handle_add_multiple_to_store`: spawned-task buffer branch AND the
/// synchronous oversize-streaming branch). At
/// `ADD_MULTIPLE_PIPELINE_DEPTH=32`, `tasks.join_next()` blocks the
/// wire-read; a follower parked for [`FOLLOWER_WAIT_CAP`] on a wedged
/// in-process leader stalls the whole batch with no progress and no
/// error to the client (and may trip the nix client's SSH keepalive).
///
/// Honest worst-case: 5s wait-cap + ~6s `grpc_put_path` retry budget
/// after fail-open ≈ **~11s** spawned-task latency. Pre-singleflight
/// was ~6s. The 5s cap (vs [`FOLLOWER_WAIT_CAP`]) keeps the
/// regression bounded; the wire-read stall is ≤11s, not the ~36s a
/// FOLLOWER_WAIT_CAP cap would give. The
/// buffered Leader drops its [`LeaderGuard`] after the FIRST attempt,
/// so a follower of a leader that is RETRYING (not wedged) wakes after
/// ~one store RTT, not the leader's full budget.
pub(crate) const PIPELINE_FOLLOWER_WAIT_CAP: Duration = Duration::from_secs(5);

/// `PIPELINE_FOLLOWER_WAIT_CAP` was introduced specifically as the
/// short wait for the wire-read-blocking opcode-44 pipeline; if a
/// refactor swaps the two constants at a call site, the long
/// batch-stall is back with no test failing on the `wait_bounded`
/// mechanism alone. Pin the ordering. `as_millis` (not `as_secs`): a
/// future sub-second-precise pair sharing a whole-second floor (e.g.
/// 5000ms vs 5500ms) would compare `5 < 5` under `as_secs` and
/// false-fail the assert.
const _: () = assert!(
    PIPELINE_FOLLOWER_WAIT_CAP.as_millis() < FOLLOWER_WAIT_CAP.as_millis(),
    "PIPELINE_FOLLOWER_WAIT_CAP must be shorter than FOLLOWER_WAIT_CAP \
     (it bounds the opcode-44 wire-read stall)"
);

/// `(tenant, store-path-hash)`. The 20-byte hash uniquely identifies
/// the path (Nix store paths are keyed by hash; the name is
/// descriptive) and is `Copy` — keying on the full `StorePath` would
/// allocate two heap `String`s (`name` + cached `full`) per
/// `acquire()` plus another pair for the Leader's guard, ~4–6 allocs
/// per call on the I-052 45k-entry hot path purely for map identity.
type Key = (Option<NormalizedName>, [u8; HASH_BYTES]);
/// `std::Mutex<HashMap>` not `DashMap`: the lock is held for one
/// `HashMap::entry`/`remove` (sub-µs) twice per PutPath; a PutPath is
/// ≥ one store RTT (~ms). At `ADD_MULTIPLE_PIPELINE_DEPTH=32` × N
/// sessions the lock op rate is ~64N per ~25ms, which is orders below
/// a single-futex contention threshold. The `quota.rs` rationale
/// (bounded keyspace, low rate) does NOT transfer here — this one is
/// "hot but trivially short critical section". If profiling shows
/// contention, `DashMap` (cf. `TenantLimiter`) is the drop-in.
type Map = Mutex<HashMap<Key, Arc<Semaphore>>>;

#[derive(Clone, Default)]
pub struct PutSingleflight {
    inner: Arc<Map>,
    /// Count of followers currently parked inside
    /// `Follower::wait_bounded`. Test instrumentation only — never
    /// read in production paths. Not `cfg(test)`-gated because
    /// integration tests (separate crates) compile this lib without
    /// `cfg(test)`; the cost is one shared `AtomicUsize` plus two
    /// atomic ops per follower wait. ONE-FIELD EXCEPTION: do NOT add
    /// further test-only fields here following this pattern — the next
    /// one introduces a `testing` cargo feature (the standard
    /// integration-test-hook pattern) and migrates this field to it.
    // TODO: gate behind a `testing` cargo feature when the second
    //   test-hook field arrives (the ONE-FIELD EXCEPTION above).
    waiters: Arc<AtomicUsize>,
}

pub(super) enum Disposition {
    Leader(LeaderGuard),
    Follower(Follower),
}

pub(super) struct LeaderGuard {
    map: Arc<Map>,
    key: Key,
    sem: Arc<Semaphore>,
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        // remove BEFORE close — ordering is load-bearing: a caller in
        // the close→remove window would observe an already-closed sem,
        // become a no-op Follower (wait_bounded returns immediately),
        // pay one redundant QPI + a `follower_miss` emission, then
        // fail-open. remove-first means that caller is a fresh Leader
        // and uploads directly. Either ordering is correct (no
        // busy-loop in either); this one avoids the wasted round-trip.
        // `into_inner` on a poisoned mutex: the only writer panicked
        // while holding the lock, but the map is structurally sound —
        // followers MUST still wake (sem.close below), and a stuck
        // entry is worse than a possibly-stale one. Same pattern in
        // `acquire`.
        self.map
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.key);
        self.sem.close();
    }
}

pub(super) struct Follower {
    sem: Arc<Semaphore>,
    waiters: Arc<AtomicUsize>,
}

impl Follower {
    /// Park on the leader's completion signal, bounded by `cap`
    /// ([`FOLLOWER_WAIT_CAP`] for synchronous call sites,
    /// [`PIPELINE_FOLLOWER_WAIT_CAP`] for the spawned-task pipeline
    /// branch). Returns `true` if the leader signaled (guard dropped →
    /// semaphore closed), `false` on timeout (leader wedged — caller
    /// treats as Miss and proceeds as if the leader failed).
    pub(super) async fn wait_bounded(self, cap: Duration) -> bool {
        // Drop-guard the decrement: `wait_bounded()` is awaited from
        // spawned tasks (integration tests) and inside the streaming
        // lane's post-failure path — both can be cancelled mid-await.
        // Without the guard, a cancelled follower leaks a +1 that the
        // integration tests' `settled() == N` sync would then never
        // reach.
        struct Dec(Arc<AtomicUsize>);
        impl Drop for Dec {
            fn drop(&mut self) {
                self.0.fetch_sub(1, SeqCst);
            }
        }
        self.waiters.fetch_add(1, SeqCst);
        let _dec = Dec(Arc::clone(&self.waiters));
        tokio::time::timeout(cap, self.sem.acquire()).await.is_ok()
    }
}

impl PutSingleflight {
    pub fn new() -> Self {
        Self::default()
    }

    // r[impl gw.put.singleflight+2]
    pub(super) fn acquire(&self, tenant: Option<&NormalizedName>, path: &StorePath) -> Disposition {
        let key: Key = (tenant.cloned(), path.hash_bytes());
        let mut map = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        match map.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => Disposition::Follower(Follower {
                sem: Arc::clone(e.get()),
                waiters: Arc::clone(&self.waiters),
            }),
            std::collections::hash_map::Entry::Vacant(e) => {
                // Eager alloc accepted: ~100B/Leader (Arc box +
                // Semaphore inner), GC'd on guard Drop. At the I-052
                // 45k-entry hot path that's ~4.5MB transient — a
                // lazy-init (Vacant inserts None; first Follower
                // upgrades under the lock) would shave it but adds a
                // branch to Drop and complicates the close ordering.
                let sem = Arc::new(Semaphore::new(0));
                // Hash half is Copy; tenant half is Arc-backed
                // (`NormalizedName(Arc<str>)`) so this clone is a
                // pointer bump.
                let key = e.key().clone();
                e.insert(Arc::clone(&sem));
                Disposition::Leader(LeaderGuard {
                    map: Arc::clone(&self.inner),
                    key,
                    sem,
                })
            }
        }
    }

    /// Number of distinct keys with a live leader. Test-hook only.
    pub fn inflight_len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Number of followers currently parked inside
    /// `Follower::wait_bounded`. Test-hook only — integration tests
    /// poll
    /// `put_path_started + waiters() == N` as a structural "all sessions
    /// settled" sync that holds in both the unwired (RED) and wired
    /// (GREEN) states.
    pub fn waiters(&self) -> usize {
        self.waiters.load(SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Key is `(tenant, hash_bytes)`: derive the 32-char HASH from a
    /// stable hash of the FULL `name` so two distinct test paths are
    /// distinct keys (the name suffix alone is not part of the key;
    /// the previous `name[0] % 32` collided on first-byte-mod-32).
    /// Hash chars are drawn from the nixbase32 alphabet (which omits
    /// `e`/`o`/`t`/`u`) so any `name` parses.
    fn sp(name: &str) -> StorePath {
        const ALPHA: &[u8; 32] = b"0123456789abcdfghijklmnpqrsvwxyz";
        let seed = name
            .bytes()
            .fold(0u64, |a, b| a.wrapping_mul(131).wrapping_add(b as u64));
        let hash: String = (0..32)
            .map(|i| ALPHA[(seed.rotate_right(i * 5) % 32) as usize] as char)
            .collect();
        format!("/nix/store/{hash}-{name}").parse().unwrap()
    }

    fn tn(s: &str) -> NormalizedName {
        NormalizedName::new(s).unwrap()
    }

    #[test]
    fn first_is_leader_drop_clears_entry() {
        let sf = PutSingleflight::new();
        let p = sp("foo");
        let d = sf.acquire(None, &p);
        assert!(matches!(d, Disposition::Leader(_)));
        assert_eq!(sf.inflight_len(), 1);
        drop(d);
        assert_eq!(sf.inflight_len(), 0);
    }

    #[test]
    fn second_same_key_is_follower_different_key_is_leader() {
        let sf = PutSingleflight::new();
        let p = sp("foo");
        let _l = sf.acquire(None, &p);
        assert!(matches!(sf.acquire(None, &p), Disposition::Follower(_)));
        // Different key gets its own leader. Bind the guard so its
        // Drop doesn't remove the entry before the len() assertion.
        let l2 = sf.acquire(None, &sp("bar"));
        assert!(matches!(l2, Disposition::Leader(_)));
        assert_eq!(sf.inflight_len(), 2);
    }

    /// Tenant is part of the key (`r[store.put.tenant-junction]`):
    /// `(Some("a"), p)`, `(Some("b"), p)`, `(None, p)` are three
    /// independent leaders; `(Some("a"), p)` twice is Leader+Follower.
    #[test]
    fn tenant_disjoint_keys_are_independent_leaders() {
        let sf = PutSingleflight::new();
        let p = sp("foo");
        let a = tn("ten-a");
        let b = tn("ten-b");
        let la = sf.acquire(Some(&a), &p);
        let lb = sf.acquire(Some(&b), &p);
        let ln = sf.acquire(None, &p);
        assert!(matches!(la, Disposition::Leader(_)));
        assert!(matches!(lb, Disposition::Leader(_)));
        assert!(matches!(ln, Disposition::Leader(_)));
        assert_eq!(sf.inflight_len(), 3, "three disjoint tenant keys");
        // Same tenant + path → follower.
        assert!(matches!(sf.acquire(Some(&a), &p), Disposition::Follower(_)));
        drop((la, lb, ln));
        assert_eq!(sf.inflight_len(), 0);
    }

    /// `sp()` must accept names whose first char is NOT in the nixbase32
    /// alphabet (e/o/t/u) — the helper maps to a valid hash char.
    #[test]
    fn sp_helper_accepts_non_nixbase32_first_char() {
        for n in ["test", "entry", "output", "upload", "foo"] {
            let _ = sp(n);
        }
    }

    #[tokio::test]
    async fn follower_wakes_on_leader_abort() {
        let sf = PutSingleflight::new();
        let p = sp("foo");
        let Disposition::Leader(guard) = sf.acquire(None, &p) else {
            panic!("first acquire must be leader")
        };
        let Disposition::Follower(follower) = sf.acquire(None, &p) else {
            panic!("second acquire must be follower")
        };
        // Leader task holds the guard then sleeps forever.
        let leader = tokio::spawn(async move {
            let _g = guard;
            tokio::time::sleep(Duration::from_secs(3600)).await;
        });
        // Follower task parks on wait_bounded().
        let waiter = tokio::spawn(follower.wait_bounded(FOLLOWER_WAIT_CAP));
        // Structural sync: poll until the follower is parked on the
        // semaphore (waiters() bumped inside wait_bounded()).
        tokio::time::timeout(Duration::from_secs(5), async {
            while sf.waiters() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("follower never parked");
        // Aborting the leader task drops the guard → closes the sem.
        leader.abort();
        let signaled = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("follower not woken by leader abort")
            .unwrap();
        assert!(
            signaled,
            "leader abort closes the sem → wait_bounded()=true"
        );
        assert_eq!(sf.inflight_len(), 0);
    }

    #[tokio::test]
    async fn follower_wait_after_leader_dropped_returns_immediately() {
        // Missed-wakeup property: if the leader's guard drops BEFORE the
        // follower polls wait_bounded(), it must still return signaled
        // (closed sem → acquire() resolves Err immediately, no park).
        let sf = PutSingleflight::new();
        let p = sp("foo");
        let Disposition::Leader(guard) = sf.acquire(None, &p) else {
            panic!("first acquire must be leader")
        };
        let Disposition::Follower(follower) = sf.acquire(None, &p) else {
            panic!("second acquire must be follower")
        };
        drop(guard);
        let signaled = tokio::time::timeout(
            Duration::from_secs(5),
            follower.wait_bounded(FOLLOWER_WAIT_CAP),
        )
        .await
        .expect("wait_bounded() did not return immediately on already-closed sem");
        assert!(signaled);
    }

    /// `FOLLOWER_WAIT_CAP` is the defensive backstop: a wedged leader
    /// must not park followers forever. `start_paused` so the cap is
    /// virtual time (no wall-clock wait).
    #[tokio::test(start_paused = true)]
    async fn follower_wait_bounded_times_out_on_wedged_leader() {
        let sf = PutSingleflight::new();
        let p = sp("foo");
        let Disposition::Leader(guard) = sf.acquire(None, &p) else {
            panic!("first acquire must be leader")
        };
        let Disposition::Follower(follower) = sf.acquire(None, &p) else {
            panic!("second acquire must be follower")
        };
        // Leader never drops (wedged). Auto-advance fires the cap.
        let signaled = follower.wait_bounded(FOLLOWER_WAIT_CAP).await;
        assert!(
            !signaled,
            "wedged leader (guard held) → wait_bounded() must time out (false)"
        );
        assert_eq!(sf.waiters(), 0, "Dec drop-guard fired on timeout return");
        drop(guard);
    }

    /// `Follower::wait_bounded` is awaited from spawned tasks and
    /// inside the streaming lane's post-failure path — both can be
    /// cancelled mid-await. The Drop-guard must restore the counter;
    /// without it a cancelled follower leaks +1 and integration tests'
    /// `settled() == N` sync never converges.
    #[tokio::test]
    async fn follower_waiters_decrements_on_cancel() {
        let sf = PutSingleflight::new();
        let p = sp("foo");
        let Disposition::Leader(guard) = sf.acquire(None, &p) else {
            panic!("first acquire must be leader")
        };
        let Disposition::Follower(follower) = sf.acquire(None, &p) else {
            panic!("second acquire must be follower")
        };
        let waiter = tokio::spawn(follower.wait_bounded(FOLLOWER_WAIT_CAP));
        tokio::time::timeout(Duration::from_secs(5), async {
            while sf.waiters() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("follower never parked");
        // Abort mid-await: the leader still holds the guard, so wait()
        // hasn't returned. The Drop-guard fires on cancel.
        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            sf.waiters(),
            0,
            "cancelled wait() must decrement waiters via Drop-guard"
        );
        drop(guard);
    }
}
