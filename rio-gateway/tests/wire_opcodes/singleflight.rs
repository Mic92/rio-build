// r[verify gw.put.singleflight+2]
//! Gateway-side `PutPath` singleflight: the buffered lane wraps every
//! `grpc_put_path` call site (`grpc_put_path_singleflight`) and
//! coalesces N concurrent uploads to one store call. The streaming
//! lane uses the registry only as a wait signal on Aborted+CONCURRENT
//! — every session uploads (no precheck, no buffer); on the
//! placeholder-contention `Aborted` a follower waits on the in-process
//! leader's signal then QPI before falling back to the budget poll
//! (other Errs surface immediately). Cross-tenant uploads do NOT
//! coalesce (`r[store.put.tenant-junction]`).

use super::*;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rio_common::tenant::NormalizedName;
use rio_gateway::{PutSingleflight, SessionShared};
use rio_nix::protocol::client::{StderrMessage, read_stderr_message};
use rio_proto::{LogServiceClient, SchedulerServiceClient, StoreServiceClient};
use rio_test_support::grpc::{MockStore, spawn_mock_scheduler, spawn_mock_store};
use rio_test_support::wire::{do_handshake, send_set_options};
use tokio::io::DuplexStream;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

/// One handshaken protocol session inside a [`SharedHarness`]. `Drop`
/// aborts the protocol task — matches the [`SessionHandles`] pattern
/// (tokio JoinHandle drop does NOT abort the spawned task; a test that
/// `?`-returns mid-setup would otherwise leave detached tasks running
/// until the multi_thread runtime shuts down).
///
/// [`SessionHandles`]: super::common::SessionHandles
struct Sess {
    stream: DuplexStream,
    task: JoinHandle<()>,
}

impl Drop for Sess {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// N handshaken sessions sharing ONE gated `MockStore` + ONE
/// `PutSingleflight`. The gate starts at 0 permits — every `put_path`
/// arrival increments `put_path_started` then parks; release via
/// `gate.add_permits(1)` (the mock drops the permit immediately, so one
/// permit drains all parked arrivals serially). `Drop` aborts the mock
/// servers + every session's protocol task.
struct SharedHarness {
    store: MockStore,
    sf: PutSingleflight,
    gate: Arc<Semaphore>,
    sessions: Vec<Sess>,
    store_h: JoinHandle<()>,
    sched_h: JoinHandle<()>,
}

impl Drop for SharedHarness {
    fn drop(&mut self) {
        self.store_h.abort();
        self.sched_h.abort();
        // sessions' Drop aborts each protocol task.
    }
}

impl SharedHarness {
    /// One session per entry in `tenants` (empty string = single-tenant
    /// mode). `put_path_gate` is `Arc<RwLock<Option<_>>>` like the other
    /// fault knobs, so it can be set after `spawn_mock_store` (no inline
    /// router build needed).
    async fn new(tenants: &[&str]) -> anyhow::Result<Self> {
        common::init_test_logging();

        let (store, store_addr, store_h) = spawn_mock_store().await?;
        let gate = Arc::new(Semaphore::new(0));
        *store.faults.put_path_gate.write().unwrap() = Some(gate.clone());
        let (_sched, sched_addr, sched_h) = spawn_mock_scheduler().await?;

        let store_client: StoreServiceClient<_> =
            rio_proto::client::connect_single(&store_addr.to_string()).await?;
        let log_client: LogServiceClient<_> =
            rio_proto::client::connect_single(&store_addr.to_string()).await?;
        let sched_client: SchedulerServiceClient<_> =
            rio_proto::client::connect_single(&sched_addr.to_string()).await?;

        let sf = PutSingleflight::new();
        let shared = SessionShared {
            put_singleflight: sf.clone(),
            ..Default::default()
        };
        // Construct the harness BEFORE the per-session loop so its Drop
        // (abort store_h/sched_h + every Sess) fires on a `?`-return
        // from the handshake — same abort-on-drop discipline as
        // SessionHandles. Spawn all sessions (push → Sess::Drop armed),
        // THEN handshake all; previously a handshake failure on session
        // N>0 left store_h/sched_h running detached.
        let mut h = Self {
            store,
            sf,
            gate,
            sessions: Vec::with_capacity(tenants.len()),
            store_h,
            sched_h,
        };
        for t in tenants {
            let (stream, task) = common::spawn_shared_session(
                store_client.clone(),
                log_client.clone(),
                sched_client.clone(),
                NormalizedName::from_maybe_empty(t),
                shared.clone(),
            );
            h.sessions.push(Sess { stream, task });
        }
        for s in &mut h.sessions {
            do_handshake(&mut s.stream).await?;
            send_set_options(&mut s.stream).await?;
        }

        Ok(h)
    }

    fn started(&self) -> u32 {
        self.store.faults.put_path_started.load(Ordering::SeqCst)
    }

    /// `put_path_started + sf.waiters()`. With the gate at 0 permits,
    /// every BUFFERED-lane session that has finished sending its NAR is
    /// parked at exactly one of those two points (store gate, or
    /// singleflight follower `wait_bounded`), so `settled() == N` is the
    /// structural "all N uploads have reached a park point" sync. Holds
    /// in BOTH the unwired (RED: `started == N`, `waiters == 0`) and
    /// wired (GREEN: `started == 1`, `waiters == N-1`) states — the
    /// test then asserts on `started()` alone to distinguish.
    /// Streaming-lane tests poll `started() == N` directly (every
    /// session uploads).
    fn settled(&self) -> usize {
        self.started() as usize + self.sf.waiters()
    }
}

/// Poll `cond` every 1ms under a 10s timeout. The 1ms sleep is for
/// scheduler cooperativeness; the assertion is the counter value the
/// caller checks afterwards (this is the sync, not the test).
async fn poll_until(what: &str, mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for: {what}"));
}

/// Send `wopAddToStoreNar` (39) for `path` with `nar`/`hash`. Same wire
/// shape as `opcodes_write::test_add_to_store_nar_accepts_valid`.
async fn send_nar39(
    s: &mut DuplexStream,
    path: &str,
    nar: &[u8],
    hash: &[u8; 32],
) -> anyhow::Result<()> {
    wire_send!(s;
        u64: 39,
        string: path,
        string: "",
        string: &hex::encode(hash),
        strings: wire::NO_STRINGS,
        u64: 0,
        u64: nar.len() as u64,
        bool: false,
        strings: wire::NO_STRINGS,
        string: "",
        bool: false, bool: true,
        framed: nar,
    );
    Ok(())
}

/// Drain stderr to the terminal frame (`Last` or `Error`). Returns
/// `Ok(())` for `STDERR_LAST`, `Err(message)` for `STDERR_ERROR`. For
/// tests that assert on the multiset of outcomes across N sessions
/// without committing to which session sees which.
async fn drain_terminal(s: &mut DuplexStream) -> anyhow::Result<Result<(), String>> {
    loop {
        match read_stderr_message(s).await? {
            StderrMessage::Last => return Ok(Ok(())),
            StderrMessage::Error(e) => return Ok(Err(e.message)),
            _ => {}
        }
    }
}

// ===========================================================================
// Test 1: streaming lane (non-.drv), 5-way Aborted → signal-wait + wire stays
// positioned. Signal-only design: every session uploads; the 4 that lose the
// store-side placeholder race wait on the in-process leader's signal then QPI.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_five_streaming_aborted_wait_on_signal() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-a"; 5]).await?;
    // ~2 KiB payload → ~2.2 KiB NAR; 5× well under the 256 KiB duplex buffer.
    let (nar, hash) = make_nar(&[0xA1; 2048]);
    let path = "/nix/store/00000000000000000000000000000000-sf-a";

    for s in &mut h.sessions {
        send_nar39(&mut s.stream, path, &nar, &hash).await?;
    }
    // Streaming lane: every session uploads (no precheck), so all 5
    // reach the store and park at the gate.
    poll_until("all 5 sessions reached store gate", || h.started() == 5).await;
    assert_eq!(
        h.started(),
        5,
        "signal-only design: every streaming session uploads (no precheck)"
    );
    assert_eq!(h.sf.inflight_len(), 1, "one (tenant, path) registered");

    // 4 of the 5 get Aborted (placeholder-contention); the 5th commits.
    // Followers whose upload lost wait on the in-process leader's
    // signal then QPI (signal-based — no budget poll); the registry
    // Leader whose upload lost falls to the budget poll and adopts on
    // attempt 1 (the winner already committed).
    //
    // Wall-clock note (PLAUSIBLE-flake budget): a loser's first poll
    // QPI runs after one ~250ms backoff; the winner's in-memory commit
    // (HashMap insert under RwLock) must complete before the loser's
    // ~6s budget exhausts. Generous, but not structural — Test 1b is
    // the structural variant (seeds before release). If this flakes
    // under CI load, convert to the 1b shape and accept losing the
    // "exactly one body committed" assertion below.
    h.store.faults.abort_next_puts.store(4, Ordering::SeqCst);
    h.gate.add_permits(1);
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
    }
    assert_eq!(
        h.store.calls.put_calls.read().unwrap().len(),
        1,
        "exactly one PutPath body committed (the store-race winner)"
    );
    assert_eq!(h.sf.inflight_len(), 0, "singleflight map drained");

    // Second opcode on every session: proves every reader was left
    // positioned at the next opcode (the pump's "reads exactly nar_size
    // bytes" contract holds whether the upload committed or Aborted).
    // Clear the gate so round 2 is ungated — round 1's chain-drained
    // permit is incidental to `arrival_gate`'s drop semantics; the
    // wire-positioning probe must not couple its liveness to that.
    *h.store.faults.put_path_gate.write().unwrap() = None;
    let (nar2, hash2) = make_nar(b"sf-a-second");
    let path2 = "/nix/store/00000000000000000000000000000001-sf-a-second";
    for s in &mut h.sessions {
        send_nar39(&mut s.stream, path2, &nar2, &hash2).await?;
    }
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
    }

    Ok(())
}

// ===========================================================================
// Test 1b: streaming lane, both Aborted → leader adopts via budget-poll,
// follower adopts via signal-wait. Structural (no fault-ordering race, no
// wall-clock seed race): both sessions receive the SAME fault, and the path
// is seeded BEFORE the gate opens, so every post-Aborted QPI (signal-wait
// or poll) finds it on the first probe.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_streaming_aborted_leader_polls_follower_waits() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-p", "ten-p"]).await?;
    let (nar, hash) = make_nar(&[0xB7; 1024]);
    let path = "/nix/store/00000000000000000000000000000009-sf-p";

    // sess0 first → singleflight Leader (acquire happens before the
    // store gate, so this ordering is structural).
    send_nar39(&mut h.sessions[0].stream, path, &nar, &hash).await?;
    poll_until("sess0 (leader) reached store", || h.started() == 1).await;

    // sess1 → singleflight Follower → uploads regardless → parks at gate.
    send_nar39(&mut h.sessions[1].stream, path, &nar, &hash).await?;
    poll_until("both sessions at store gate", || h.started() == 2).await;
    assert_eq!(h.sf.inflight_len(), 1, "one (tenant, path) registered");

    // Seed BEFORE releasing the gate: every post-Aborted QPI (the
    // follower's signal-wait probe and the leader's first poll
    // iteration) finds the path immediately — no wall-clock race
    // against a backoff budget.
    h.store.seed(
        rio_proto::types::PathInfo {
            store_path: path.into(),
            nar_hash: hash.to_vec(),
            nar_size: nar.len() as u64,
            ..Default::default()
        }
        .try_into()
        .expect("valid fixture"),
        nar.clone(),
    );

    // BOTH get Aborted+CONCURRENT — gate drain order is irrelevant.
    // sess0 (Leader): Aborted → drop guard → poll_adopt → found
    // (seeded) → Ok. sess1 (Follower): Aborted → wait on sess0's
    // signal (bounded) → QPI → found (seeded) → coalesced → Ok.
    h.store.faults.abort_next_puts.store(2, Ordering::SeqCst);
    h.gate.add_permits(1);

    let r0 = drain_terminal(&mut h.sessions[0].stream).await?;
    let r1 = drain_terminal(&mut h.sessions[1].stream).await?;
    assert!(
        r0.is_ok() && r1.is_ok(),
        "leader adopts via poll, follower via signal-wait — both Ok; got {r0:?} / {r1:?}"
    );
    assert_eq!(
        h.store.calls.put_calls.read().unwrap().len(),
        0,
        "both PutPaths Aborted — neither committed a body"
    );
    assert_eq!(h.sf.inflight_len(), 0, "guard dropped before poll");

    Ok(())
}

// ===========================================================================
// Test 1c: streaming lane, follower's own upload fails NON-Aborted →
// follower surfaces IMMEDIATELY (no signal-wait, no FOLLOWER_WAIT_CAP stall).
// The signal-wait is scoped to Aborted+CONCURRENT only — a non-retryable
// store rejection (InvalidArgument, PermissionDenied, hash mismatch) reaches
// the nix client within the upload RTT, not after a multi-minute wait on an
// unrelated in-process leader.
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_streaming_follower_surfaces_non_aborted_err_immediately() -> anyhow::Result<()>
{
    let mut h = SharedHarness::new(&["ten-q", "ten-q"]).await?;
    let (nar, hash) = make_nar(&[0xC3; 1024]);
    let path = "/nix/store/0000000000000000000000000000000a-sf-q";

    // Structural pin: sess0 (Leader) STAYS PARKED at the gate (guard
    // held) for the duration of sess1's error path. sess1 (Follower)
    // bypasses the gate entirely and hits InvalidArgument while sess0's
    // guard is live. A regression that re-broadens follower-wait to any
    // Err would park sess1 on `wait_bounded` (open sem) → waiters()==1
    // and the 15s drain timeout fires.
    send_nar39(&mut h.sessions[0].stream, path, &nar, &hash).await?;
    poll_until("sess0 (leader) reached store", || h.started() == 1).await;
    assert_eq!(h.sf.inflight_len(), 1, "sess0 holds the LeaderGuard");

    // Clear the gate (sess0 stays parked on its already-started
    // acquire) so sess1 proceeds ungated; arm one InvalidArgument.
    *h.store.faults.put_path_gate.write().unwrap() = None;
    h.store.faults.fail_next_puts.store(1, Ordering::SeqCst);
    *h.store.faults.fail_next_puts_code.write().unwrap() = Some(tonic::Code::InvalidArgument);

    send_nar39(&mut h.sessions[1].stream, path, &nar, &hash).await?;
    // Structural sync: sess1's PutPath reached the store. Both happy
    // and regression paths upload (streaming lane: every caller
    // uploads); this trims the 15s budget below to the post-RPC
    // surface latency only.
    poll_until("sess1 reached store", || h.started() == 2).await;
    // sess1's error must surface within one upload RTT. Under
    // regression, sess1 parks on wait_bounded for FOLLOWER_WAIT_CAP
    // (30s) AFTER receiving InvalidArgument and this timeout fires.
    // 15s gives the happy path (InvalidArgument → STDERR_ERROR write)
    // generous CI-load slack while keeping 15s margin against the
    // regression case. The structural discriminator is the
    // `waiters()==0` assert below (a regressed sess1 would have
    // parked → waiters()==1); the timeout is the bound.
    let r1 = tokio::time::timeout(
        Duration::from_secs(15),
        drain_terminal(&mut h.sessions[1].stream),
    )
    .await
    .expect(
        "Follower's non-Aborted Err must surface immediately \
         (regression: signal-wait re-broadened past Aborted+CONCURRENT)",
    )?;
    assert!(
        r1.is_err(),
        "non-Aborted Err must surface as STDERR_ERROR; got {r1:?}"
    );
    assert_eq!(
        h.sf.waiters(),
        0,
        "Follower must NOT have parked on wait_bounded for a non-Aborted Err"
    );
    assert_eq!(
        h.sf.inflight_len(),
        1,
        "sess0's LeaderGuard is still held (gate unparked) — sess1's Err \
         path ran while the sem was OPEN"
    );

    // Cleanup: release sess0.
    h.gate.add_permits(1);
    let _ = drain_terminal(&mut h.sessions[0].stream).await?;

    Ok(())
}

// ===========================================================================
// Test 2: buffered lane (.drv), follower fails open on leader failure
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_buffered_follower_fails_open_on_leader_failure() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-b", "ten-b"]).await?;
    // Non-retryable failure: `transient_retry_after` does NOT retry
    // InvalidArgument, so the leader's session surfaces STDERR_ERROR
    // instead of looping.
    h.store.faults.fail_next_puts.store(1, Ordering::SeqCst);
    *h.store.faults.fail_next_puts_code.write().unwrap() = Some(tonic::Code::InvalidArgument);

    let (nar, hash) = make_nar(b"sf-b-drv");
    let path = "/nix/store/00000000000000000000000000000002-sf-b.drv";

    // sess0 first → guaranteed leader (parks at the gate before sess1 sends).
    send_nar39(&mut h.sessions[0].stream, path, &nar, &hash).await?;
    poll_until("leader reached store", || h.started() == 1).await;

    send_nar39(&mut h.sessions[1].stream, path, &nar, &hash).await?;
    // RED: singleflight not wired → sess1 also reaches the store
    // (started → 2); waiters() stays 0; this poll times out. GREEN:
    // sess1 parks as a follower; waiters() → 1.
    poll_until(
        "follower parked on singleflight (RED: call site not wired — sess1 went straight to the store)",
        || h.sf.waiters() == 1,
    )
    .await;

    h.gate.add_permits(1);
    let r0 = drain_terminal(&mut h.sessions[0].stream).await?;
    let r1 = drain_terminal(&mut h.sessions[1].stream).await?;
    let errs = [&r0, &r1].iter().filter(|r| r.is_err()).count();
    assert_eq!(
        errs, 1,
        "exactly one session sees STDERR_ERROR (the failed leader); got {r0:?} / {r1:?}"
    );

    // Relies on `arrival_gate`'s residual-permit pass-through: after
    // `add_permits(1)` cascades through the leader, the semaphore is
    // left at 1 permit, so the follower's fail-open `grpc_put_path`
    // re-entering here passes ungated. A `forget_permits(1)` (or bound
    // permit) in `arrival_gate` would park the fail-open forever and
    // `drain_terminal(sess1)` above would hang.
    assert_eq!(
        h.started(),
        2,
        "leader (failed) + follower fail-open = two store arrivals"
    );
    assert_eq!(
        h.store.calls.put_calls.read().unwrap().len(),
        1,
        "only the follower's fail-open upload completed a PutPath body"
    );

    Ok(())
}

// ===========================================================================
// Test 3: cross-tenant — different keys, no coalesce. BUFFERED lane (.drv)
// so the precheck IS what's tested: a regression that drops the tenant
// component from `acquire`'s key would coalesce these two and `started()`
// would be 1, not 2. (The streaming lane uploads every caller regardless of
// disposition, so it cannot witness this property end-to-end.)
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_cross_tenant_does_not_coalesce() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-x", "ten-y"]).await?;
    let (nar, hash) = make_nar(b"sf-cross-tenant");
    // .drv suffix → buffered lane → precheck runs.
    let path = "/nix/store/00000000000000000000000000000003-sf-cross.drv";

    for s in &mut h.sessions {
        send_nar39(&mut s.stream, path, &nar, &hash).await?;
    }
    // Different tenants → different singleflight keys → both Leaders →
    // both reach the store gate. settled() == started() (no waiters).
    poll_until("both tenants settled", || h.settled() == 2).await;
    assert_eq!(
        h.started(),
        2,
        "cross-tenant uploads must NOT coalesce in the buffered lane \
         (tenant is part of the key per r[store.put.tenant-junction])"
    );
    assert_eq!(h.sf.waiters(), 0, "no follower across tenant boundary");

    h.gate.add_permits(1);
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
    }
    assert_eq!(
        h.store.calls.put_calls.read().unwrap().len(),
        2,
        "per r[store.put.tenant-junction]: cross-tenant uploads each reach the store"
    );

    Ok(())
}

// ===========================================================================
// Test 4: buffered lane (.drv), 2-way coalesce + wire stays positioned
// ===========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_buffered_drv_coalesce_and_wire_stays_positioned() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-d", "ten-d"]).await?;
    let (nar, hash) = make_nar(b"sf-d-drv");
    let path = "/nix/store/00000000000000000000000000000004-sf-d.drv";

    for s in &mut h.sessions {
        send_nar39(&mut s.stream, path, &nar, &hash).await?;
    }
    poll_until("both sessions settled (gate or sf-wait)", || {
        h.settled() == 2
    })
    .await;

    // RED: both reached the store (2). GREEN: 1.
    assert_eq!(
        h.started(),
        1,
        "2 concurrent .drv uploads of the same (tenant, path) must coalesce to one store PutPath"
    );

    h.gate.add_permits(1);
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
    }
    assert_eq!(h.store.calls.put_calls.read().unwrap().len(), 1);

    // Second opcode on the FOLLOWER (and the leader, for symmetry): the
    // buffered lane's drain-then-adopt must leave the wire positioned at
    // the next opcode. Clear the gate so round 2 is ungated (see test 1).
    *h.store.faults.put_path_gate.write().unwrap() = None;
    let (nar2, hash2) = make_nar(b"sf-d-second");
    let path2 = "/nix/store/00000000000000000000000000000005-sf-d-second";
    for s in &mut h.sessions {
        send_nar39(&mut s.stream, path2, &nar2, &hash2).await?;
    }
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
    }

    Ok(())
}

// ===========================================================================
// Test 5: opcode-44 buffered/spawned lane (Site 3), 2-way coalesce
// ===========================================================================

/// Send `wopAddMultipleToStore` (44) with a 1-entry batch. Small NAR
/// (≤16 MiB) → the buffered/spawned-task lane.
async fn send_nar44_single(
    s: &mut DuplexStream,
    path: &str,
    nar: &[u8],
    hash: &[u8; 32],
) -> anyhow::Result<()> {
    let inner = wire_bytes![
        u64: 1,
        string: path,
        string: "",
        string: &hex::encode(hash),
        strings: wire::NO_STRINGS,
        u64: 0,
        u64: nar.len() as u64,
        bool: false,
        strings: wire::NO_STRINGS,
        string: "",
        raw: nar,
    ];
    wire_send!(s;
        u64: 44,
        bool: false,
        bool: true,
        framed: &inner,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_add_multiple_buffered_coalesce() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-e", "ten-e"]).await?;
    let (nar, hash) = make_nar(b"sf-e-multi");
    let path = "/nix/store/00000000000000000000000000000006-sf-e-multi";

    for s in &mut h.sessions {
        send_nar44_single(&mut s.stream, path, &nar, &hash).await?;
    }
    poll_until("both sessions settled (gate or sf-wait)", || {
        h.settled() == 2
    })
    .await;

    assert_eq!(
        h.started(),
        1,
        "2 concurrent wopAddMultipleToStore uploads of the same (tenant, path) \
         must coalesce to one store PutPath in the buffered/spawned lane"
    );

    h.gate.add_permits(1);
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
    }
    assert_eq!(
        h.store.calls.put_calls.read().unwrap().len(),
        1,
        "exactly one PutPath body should have run"
    );
    assert_eq!(h.sf.inflight_len(), 0, "singleflight map drained");

    Ok(())
}

// ===========================================================================
// Test 5b: opcode-8 buffered lane (wopAddTextToStore), 2-way coalesce. Covers
// the `handle_add_text_to_store` call site (the most common .drv-upload
// opcode from `nix-instantiate`). Same name + same text → same CA store-path
// → same singleflight key.
// ===========================================================================

/// Send `wopAddTextToStore` (8). Same wire shape as
/// `opcodes_write::test_add_text_to_store`.
async fn send_addtexttostore8(s: &mut DuplexStream, name: &str, text: &str) -> anyhow::Result<()> {
    wire_send!(s;
        u64: 8,
        string: name,
        string: text,
        strings: wire::NO_STRINGS,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_add_text_to_store_coalesce() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-g", "ten-g"]).await?;
    let text = "sf-g-addtexttostore-same-text";

    for s in &mut h.sessions {
        send_addtexttostore8(&mut s.stream, "sf-g", text).await?;
    }
    poll_until("both sessions settled (gate or sf-wait)", || {
        h.settled() == 2
    })
    .await;

    assert_eq!(
        h.started(),
        1,
        "2 concurrent wopAddTextToStore uploads of the same (tenant, CA-path) \
         must coalesce to one store PutPath (handle_add_text_to_store call site)"
    );

    h.gate.add_permits(1);
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
        // Response is a single store-path string.
        let _ = wire::read_string(&mut s.stream).await?;
    }
    assert_eq!(
        h.store.calls.put_calls.read().unwrap().len(),
        1,
        "exactly one PutPath body should have run"
    );
    assert_eq!(h.sf.inflight_len(), 0, "singleflight map drained");

    Ok(())
}

// ===========================================================================
// Test 6: opcode-7 buffered lane (wopAddToStore), 2-way coalesce. Covers the
// `handle_add_to_store` call site (one of the two buffered sites the existing
// tests left unverified). Same content + same name + same cam_str → same CA
// store-path → same singleflight key.
// ===========================================================================

/// Send `wopAddToStore` (7) with `cam_str="text:sha256"` — text-method
/// CA path. Same wire shape as
/// `opcodes_write::test_add_to_store_text_method`.
async fn send_addtostore7(s: &mut DuplexStream, name: &str, content: &[u8]) -> anyhow::Result<()> {
    wire_send!(s;
        u64: 7,
        string: name,
        string: "text:sha256",
        strings: wire::NO_STRINGS,
        bool: false,
        framed: content,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn singleflight_add_to_store_coalesce() -> anyhow::Result<()> {
    let mut h = SharedHarness::new(&["ten-f", "ten-f"]).await?;
    let content = b"sf-f-addtostore-same-content";

    for s in &mut h.sessions {
        send_addtostore7(&mut s.stream, "sf-f", content).await?;
    }
    poll_until("both sessions settled (gate or sf-wait)", || {
        h.settled() == 2
    })
    .await;

    assert_eq!(
        h.started(),
        1,
        "2 concurrent wopAddToStore uploads of the same (tenant, CA-path) \
         must coalesce to one store PutPath (handle_add_to_store call site)"
    );

    h.gate.add_permits(1);
    for s in &mut h.sessions {
        drain_stderr_until_last(&mut s.stream).await?;
        // 9-field ValidPathInfo response: path + 8-field PathInfoWire.
        let _ = wire::read_string(&mut s.stream).await?;
        let _ = read_path_info(&mut s.stream).await?;
    }
    assert_eq!(
        h.store.calls.put_calls.read().unwrap().len(),
        1,
        "exactly one PutPath body should have run"
    );
    assert_eq!(h.sf.inflight_len(), 0, "singleflight map drained");

    Ok(())
}
