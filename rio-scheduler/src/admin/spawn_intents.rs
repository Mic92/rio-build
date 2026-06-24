//! `AdminService.GetSpawnIntents` implementation.
//!
//! Thin actor query: the actor's `compute_spawn_intents` does the
//! single-pass Ready scan + filter + `solve_intent_for`; this module
//! translates the proto request to the internal `SpawnIntentsRequest`
//! and the actor result to the proto response.

use std::collections::HashSet;

use rio_proto::types::{
    ExecutorKind, GetSpawnIntentsRequest, GetSpawnIntentsResponse, MintExecutorTokensRequest,
    MintExecutorTokensResponse,
};
use tonic::Status;

use crate::actor::{ActorCommand, ActorHandle, AdminQuery, SpawnIntentsRequest};

/// Systems for which `rio_scheduler_spawn_intents_pending_by_system`
/// has ever been emitted. A drained system is absent from
/// `snap.pending_by_system`, so without this universe its label-series
/// is never zero-written and the exporter keeps serving the last
/// nonzero value indefinitely. Same zero-write convention as the
/// controller-side `all_cells` Phase-0 loop in `consolidate.rs`.
static EMITTED_SYSTEMS: std::sync::LazyLock<parking_lot::Mutex<HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

/// Lease-loss zero-write for `rio_scheduler_spawn_intents_pending_by_system`.
///
/// The label axis is open (whatever `system` strings the actor has seen),
/// so this gauge cannot join the closed-axis [`LeaderGauge`] family;
/// instead `clear_persisted_state` calls this alongside the
/// `status_outbox_depth` zero-write so a deposed leader stops serving its
/// last nonzero per-system values (the merged_bug_025 frozen-series
/// double-count under `sum by (system)` with `scheduler.replicas=2`).
/// The exemption rationale (`alert_metrics.rs`) cites this fn.
///
/// [`LeaderGauge`]: crate::observability::LeaderGauge
pub(crate) fn zero_pending_by_system_gauge() {
    for sys in EMITTED_SYSTEMS.lock().iter() {
        metrics::gauge!(
            "rio_scheduler_spawn_intents_pending_by_system",
            "system" => sys.clone(),
        )
        .set(0.0);
    }
}

/// Query the actor for the spawn-intent snapshot, convert to proto.
// r[impl sched.admin.spawn-intents+2]
// r[impl sched.admission.mint-uncapped]
pub(super) async fn get_spawn_intents(
    actor: &ActorHandle,
    is_leader: &std::sync::atomic::AtomicBool,
    req: GetSpawnIntentsRequest,
) -> Result<GetSpawnIntentsResponse, Status> {
    // `optional ExecutorKind` → `Option<i32>` in prost. None =
    // unfiltered; out-of-range falls back to unfiltered.
    let kind = req.kind.and_then(|k| ExecutorKind::try_from(k).ok());
    // I-176: proto3 repeated can't be optional, so the wire shape is
    // `(filter_features: bool, features: Vec)`. Collapse to Option here
    // so the actor sees the tristate directly: false → None
    // (unfiltered); true → Some(vec) (filter, even when vec is empty =
    // "I support no features").
    let features = req.filter_features.then_some(req.features);
    let actor_req = SpawnIntentsRequest {
        kind,
        systems: req.systems,
        features,
    };

    // The pool reconcilers read this to set per-pool spawn targets.
    // Dropping under backpressure blinds the autoscaler exactly when it
    // should scale up — same reasoning as ClusterStatus.
    let snap = super::query_actor(actor, |reply| {
        ActorCommand::Admin(AdminQuery::GetSpawnIntents {
            req: actor_req,
            reply,
        })
    })
    .await?;

    // Round-9 B3 (Banner A-1): the priority-head window. `intents` is
    // priority-sorted descending (the response contract), so cutting
    // at `limit` keeps the head — the critical path is served first
    // and a bounded consumer drops only the lowest-priority tail.
    // `limit == 0` (the proto3 default and every pre-window client) =
    // unbounded, the pre-window behavior. The aggregates
    // (`queued_by_system`, `ice_masked_cells`) stay FULL-population:
    // the demand record is the demand truth (A-2) — a consumer sizing
    // supply work derives its deficit from the aggregate whenever
    // `truncated` is set, never from `len(intents)` (the cover-deficit
    // trap; the controller-side filter is the registered round-9 S4
    // constituent).
    let mut intents = snap.intents;
    let limit = req.limit as usize;
    let truncated = limit != 0 && intents.len() > limit;
    if truncated {
        intents.truncate(limit);
    }
    // r[impl ctrl.nodeclaim.backlog-floor] — observability mirror of
    // proto field 7 so step-3 read-only rollout verification works
    // before the controller consumes it.
    // r[impl obs.metric.spawn-intents-pending]
    {
        let mut emitted = EMITTED_SYSTEMS.lock();
        // Cross-task race close (merged_bug_025 second strike): the
        // caller's `ensure_leader()` passed BEFORE the actor await; a
        // lease loss in that gap → the actor's lose-edge
        // `clear_persisted_state` → [`zero_pending_by_system_gauge`]
        // sweep can land BEFORE this in-flight emission, which would
        // re-stale the deposed replica's series. Recheck `is_leader`
        // INSIDE the `EMITTED_SYSTEMS` lock so the emission is
        // serialised with the zero sweep (which takes the same lock):
        // the lease task stores `is_leader=false` (`on_lose`) before
        // the actor's `LeaderLost` handler runs the sweep, so either
        // this recheck reads false (skip) or the emission lands and
        // the sweep — blocked on this lock — zeroes after it.
        if is_leader.load(std::sync::atomic::Ordering::Relaxed) {
            emitted.extend(snap.pending_by_system.keys().cloned());
            for sys in emitted.iter() {
                let n = snap.pending_by_system.get(sys).copied().unwrap_or(0);
                metrics::gauge!(
                    "rio_scheduler_spawn_intents_pending_by_system",
                    "system" => sys.clone(),
                )
                .set(n as f64);
            }
        }
    }
    let resp = GetSpawnIntentsResponse {
        intents,
        queued_by_system: snap.queued_by_system,
        // Round-10 merged_bug_006: the forecast population class —
        // window-proof like its Ready sibling (the limit cuts
        // `intents`, never the aggregates) but NOT full-population
        // (WO-S8-10/merged_bug_068: that wording restated cross-crate
        // semantics falsely): per the `forecast_by_system` increment
        // site in actor/snapshot.rs, forecast counts at the emit
        // chokepoint — post tenant-budget admission, post the view
        // filter — so it bounds exactly the forecast intents emitted
        // to THIS view. Only `queued_by_system` is pre-filter.
        forecast_by_system: snap.forecast_by_system,
        // ctrl.nodeclaim.backlog-floor: full-population, NOT
        // view-filtered (counted in the status-match arm BEFORE
        // `classify_ready_node` and the kind/feature filters), and
        // window-proof (the limit cuts `intents`, never aggregates).
        pending_by_system: snap.pending_by_system,
        ice_masked_cells: snap.ice_masked_cells,
        truncated,
    };
    // Wire-size measurement (round-9 dossier E2 — the B-2 gate for
    // the GetSpawnIntents pagination constants): the response is the
    // largest unpaginated rio surface (full Ready set per call, 379
    // calls/12min at the incident fleet) and until now NO rio gRPC
    // surface measured encoded response bytes — the 150-400 B/intent
    // figure the admission census used was derived, not observed.
    // `encoded_len` is the exact prost wire size without re-encoding;
    // the per-response intent count alongside it lets PromQL derive
    // observed bytes-per-intent. Emitted at the serving chokepoint so
    // every consumer (per-pool reconcilers, cover sizing) is counted.
    let encoded_len = rio_proto::prost::Message::encoded_len(&resp);
    metrics::histogram!("rio_scheduler_spawn_intents_response_bytes").record(encoded_len as f64);
    metrics::histogram!("rio_scheduler_spawn_intents_per_response")
        .record(resp.intents.len() as f64);
    Ok(resp)
}

/// Query the actor for per-intent `ExecutorClaims` tokens.
/// Controller-only — callers MUST have passed
/// `ensure_service_caller(&["rio-controller"])`.
pub(super) async fn mint_executor_tokens(
    actor: &ActorHandle,
    req: MintExecutorTokensRequest,
) -> Result<MintExecutorTokensResponse, Status> {
    let (tokens, keyless) = super::query_actor(actor, |reply| {
        ActorCommand::Admin(AdminQuery::MintExecutorTokens {
            intent_ids: req.intent_ids,
            reply,
        })
    })
    .await?;
    Ok(MintExecutorTokensResponse { tokens, keyless })
}
