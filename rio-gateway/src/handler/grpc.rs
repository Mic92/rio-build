//! Store-query helpers using gRPC.
//!
//! All helpers take `jwt_token: Option<&str>` and attach it as
//! `x-rio-tenant-token` via [`with_jwt`] / [`jwt_metadata`] (the former
//! wraps a `tonic::Request`, the latter feeds `rio_proto::client::*`
//! helpers; both share one header-construction path). Without the JWT,
//! store-side tenant-scoped operations (substitution, narinfo
//! visibility gate) short-circuit — see `r[gw.jwt.issue]`.

use std::collections::HashMap;

use rio_common::grpc::{DEFAULT_GRPC_TIMEOUT, GRPC_STREAM_TIMEOUT};
use rio_common::limits::MAX_NAR_SIZE;
use rio_nix::derivation::Derivation;
use rio_nix::store_path::StorePath;
use rio_proto::client::NAR_CHUNK_SIZE;
use rio_proto::validated::ValidatedPathInfo;
use rio_proto::{StoreServiceClient, types};
use tokio::io::{AsyncRead, AsyncReadExt};
use tonic::transport::Channel;

use super::put_path::{
    PutCtx, SfLane, SfOutcome, WaitOutcome, emit_outcome, is_actionable_qpi_err,
    wait_then_qpi_if_follower,
};
use super::singleflight::{Disposition, LeaderGuard};
use super::{GatewayError, attach_service_token, jwt_metadata, with_jwt};
use crate::translate;

/// Max attempts + backoff for [`transient_retry_after`]. 250 ms base,
/// ×4, 4 s cap, ±25% jitter. Retry budget is 2 attempts (one retry).
/// Under sustained admission saturation each attempt blocks
/// `SUBSTITUTE_ADMISSION_WAIT` (25 s) server-side, so worst-case
/// latency before surfacing to the user is ~50 s — bounded, but
/// operators should treat sustained `RESOURCE_EXHAUSTED` here as a
/// scaling signal. Gateway clients are interactive (`nix copy`, IFD
/// evals); the single retry covers transient blips without masking
/// genuine overload.
const STORE_TRANSIENT_MAX_ATTEMPTS: u32 = 2;
const STORE_TRANSIENT_BACKOFF: rio_common::backoff::Backoff = rio_common::backoff::Backoff {
    base: std::time::Duration::from_millis(250),
    mult: 4.0,
    cap: std::time::Duration::from_secs(4),
    jitter: rio_common::backoff::Jitter::Proportional(0.25),
};

/// `Some(delay)` if `status` is transient (per
/// [`rio_common::grpc::is_transient`]) and `attempt <
/// STORE_TRANSIENT_MAX_ATTEMPTS`; `None` if the caller should surface
/// the error. Logs the retry/surface decision. Shared by
/// [`grpc_query_path_info`] and [`grpc_get_path`] — both traverse
/// `r[store.substitute.admission+2]` server-side, which returns
/// `ResourceExhausted` after its bounded wait under saturation. The
/// in-process materialization executor re-arms through its job budget
/// (`r[store.materialize.executor+5]`); without this retry the gateway
/// surfaced a hard `STDERR_ERROR` → client sees
/// "store error: ResourceExhausted" on a momentary overload.
///
/// NOT a closure-taking `retry(op)` wrapper: `impl AsyncFnMut`
/// capturing `&mut StoreServiceClient` hits the HRTB-`Send` limitation
/// when the calling future is `tokio::spawn`ed (the gateway proto-task
/// is). Same restriction noted at `rio_common::backoff::retry`'s test.
// r[impl gw.store.transient-retry]
fn transient_retry_after(
    rpc: &'static str,
    attempt: u32,
    status: &tonic::Status,
    // DeadlineExceeded: the per-chunk idle bound (I-211) firing — the
    // store parked (NarBudget under cold-start burst) past one idle
    // window. With KEDA scaling up, a replay may land on a pod with
    // budget; the buffered PutPath lane and GetPath both replay cleanly.
    // NOT in the shared `is_transient` (rio-common documents
    // DeadlineExceeded as the caller's own timeout, never store-side);
    // gateway-local here because the gateway IS that caller and the
    // store's "retry" invitation is the parked semaphore, not a code.
    // SCOPED PER CALLER: only PutPath/GetPath pass `true` — they are
    // the I-211 callers and replay cleanly. QueryPathInfo passes
    // `false` (its DEFAULT_GRPC_TIMEOUT-wrapped DeadlineExceeded means
    // a wedged store, not a NarBudget park; surfacing latency would
    // double for nothing). A future non-idempotent caller MUST pass
    // `false` unless the I-211 rationale applies to it.
    retry_deadline_exceeded: bool,
) -> Option<std::time::Duration> {
    let transient = rio_common::grpc::is_transient(status.code())
        || (retry_deadline_exceeded && status.code() == tonic::Code::DeadlineExceeded);
    if !transient {
        return None;
    }
    if attempt >= STORE_TRANSIENT_MAX_ATTEMPTS {
        tracing::warn!(
            rpc, attempts = attempt, code = ?status.code(),
            "store transient status exhausted retry budget; surfacing"
        );
        return None;
    }
    let delay = STORE_TRANSIENT_BACKOFF.duration(attempt - 1);
    tracing::debug!(
        rpc, attempt, backoff = ?delay, code = ?status.code(), msg = %status.message(),
        "store transient status; retrying"
    );
    Some(delay)
}

/// Consume `remaining` bytes from `nar_reader` into `/dev/null` —
/// honours the "reads exactly nar_size bytes" wire-positioning contract
/// callers depend on when the PutPath pump exits early (rx-dropped,
/// idle-timeout). Short read → typed `NarRead`/`UnexpectedEof` so a
/// truncated client stream surfaces with a position, not as garbage at
/// the next entry's header parse.
async fn drain_nar_remaining<R: AsyncRead + Unpin>(
    nar_reader: &mut R,
    remaining: u64,
    nar_size: u64,
    why: &str,
) -> anyhow::Result<()> {
    let copied = tokio::io::copy(&mut nar_reader.take(remaining), &mut tokio::io::sink())
        .await
        .map_err(|e| GatewayError::NarRead {
            context: format!("draining {remaining} of {nar_size} after {why}"),
            source: e,
        })?;
    if copied < remaining {
        return Err(GatewayError::NarRead {
            context: format!("{why}: short read ({copied} of {remaining}, total {nar_size})"),
            source: std::io::ErrorKind::UnexpectedEof.into(),
        }
        .into());
    }
    Ok(())
}

/// Streaming-Aborted lane: poll-then-adopt after the store returned
/// `Aborted("concurrent PutPath in progress")`. Owns the FULL
/// `attempt="1"..=PUT_PATH_ABORTED_MAX_ATTEMPTS` emit range —
/// `attempt="1"` at entry (so every Aborted+CONCURRENT counts,
/// signal-adopted or not, matching the buffered lane's loop-head emit
/// shape), then `attempt` in `2..=PUT_PATH_ABORTED_MAX_ATTEMPTS` per
/// poll miss. The caller does NOT pre-emit; the emit-shape contract
/// (`1..=8` each exactly once on exhaust) lives in ONE function. Same
/// emit-count and poll-count shape as [`grpc_put_path`]'s buffered
/// retry loop (single-axis schema shared with that lane: same
/// store-side contention, same dashboard cell;
/// `attempt=PUT_PATH_ABORTED_MAX_ATTEMPTS` is the budget-exhausted
/// signal). The per-iteration body differs (replay-from-buffer vs
/// QPI-poll) AND the buffered loop interleaves Aborted with
/// transient-retry; the emit/exhaust SHAPE both produce
/// (`attempt="1".."8"`, exhaust at 8) is pinned by
/// `putpath_aborted_retry_emit_shape_pinned_across_lanes`, and
/// `putpath_retry_attempt_axis_matches_the_emit_law` pins the seeded
/// label set to the const.
///
/// `signal_adopted`: `true` if the caller's pre-poll signal-wait QPI
/// already found the path — the helper emits `attempt="1"` (one
/// Aborted observation) and returns `Ok(false)` without polling.
///
/// The original Aborted status was ALREADY surfaced at the rpc match
/// in [`grpc_put_path_streaming`] (one operation, one observation per
/// the bug_118 emit law); on budget exhaust it is returned verbatim
/// WITHOUT re-surfacing — the caller propagates `?`. A QPI error
/// mid-poll is split by [`is_actionable_qpi_err`] (deny-list):
/// actionable (PermissionDenied, InvalidArgument, …) propagates
/// immediately — the user sees it, not the original Aborted after a
/// futile ~6s poll; everything else (DeadlineExceeded / Internal /
/// Cancelled / the transient set / non-`Status` roots) is debug-logged
/// and treated as not-yet-present (continue polling) — symmetric with
/// [`wait_then_qpi_if_follower`]: QueryPathInfo is a PROBE, its
/// infra-failure does not change the PutPath outcome (NOT a PutPath
/// observation; the emit law binds to the operation). Under a degraded
/// QPI plane (infra errors while PutPath works), the budget exhausts
/// and the original Aborted surfaces — same as if every poll returned
/// NotFound.
pub(super) async fn poll_adopt_after_aborted(
    store_client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    store_path: &StorePath,
    original_status: tonic::Status,
    signal_adopted: bool,
) -> anyhow::Result<bool> {
    // attempt=1: every Aborted+CONCURRENT counts (signal-adopted or
    // not), matching the buffered lane's loop-head emit shape.
    metrics::counter!(
        "rio_gateway_putpath_aborted_retries_total",
        "attempt" => "1",
    )
    .increment(1);
    if signal_adopted {
        return Ok(false);
    }
    for attempt in 1..PUT_PATH_ABORTED_MAX_ATTEMPTS {
        let delay = PUT_PATH_BACKOFF.duration(attempt - 1);
        tracing::debug!(
            %store_path, attempt, backoff = ?delay, lane = "streaming",
            "PutPath: polling for the concurrent uploader's result"
        );
        tokio::time::sleep(delay).await;
        match grpc_query_path_info(store_client, jwt_token, store_path.as_str()).await {
            Ok(Some(_)) => {
                tracing::debug!(
                    %store_path, attempt, lane = "streaming",
                    "PutPath: adopted concurrent uploader's result"
                );
                return Ok(false);
            }
            Ok(None) => {}
            // Actionable (PermissionDenied, InvalidArgument, … — the
            // is_actionable_qpi_err deny-list): propagate so the user
            // sees it instead of the original Aborted after a futile
            // ~6s poll. Everything else (DeadlineExceeded / Internal /
            // Cancelled / the transient set, non-Status roots —
            // grpc_query_path_info already retried the transient set):
            // treat as not-yet-present, keep polling; on exhaust,
            // surface the ORIGINAL Aborted.
            Err(e) if is_actionable_qpi_err(&e) => return Err(e),
            Err(e) => {
                tracing::debug!(
                    %store_path, attempt, error = %e, lane = "streaming",
                    "PutPath: poll QPI probe failed non-actionably; treating as not-yet-present"
                );
            }
        }
        // Poll N missed → record attempt N+1. The final iteration
        // emits PUT_PATH_ABORTED_MAX_ATTEMPTS, the budget-exhausted
        // signal, then falls through to surface.
        metrics::counter!(
            "rio_gateway_putpath_aborted_retries_total",
            "attempt" => (attempt + 1).to_string(),
        )
        .increment(1);
    }
    tracing::warn!(
        %store_path, attempts = PUT_PATH_ABORTED_MAX_ATTEMPTS, lane = "streaming",
        "PutPath: still absent after wait-then-adopt budget; surfacing"
    );
    Err(original_status.into())
}

/// Query PathInfo from store via gRPC. Returns None if NOT_FOUND.
///
/// Retries transient status per [`transient_retry_after`] — store-side
/// `QueryPathInfo` traverses `try_substitute_on_miss`
/// (`r[store.substitute.admission+2]`).
pub(crate) async fn grpc_query_path_info(
    store_client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    store_path: &str,
) -> anyhow::Result<Option<ValidatedPathInfo>> {
    let md = jwt_metadata(jwt_token);
    let mut attempt = 0u32;
    loop {
        match rio_proto::client::query_path_info_opt(
            store_client,
            store_path,
            DEFAULT_GRPC_TIMEOUT,
            &md,
        )
        .await
        {
            Ok(v) => return Ok(v),
            Err(status) => {
                attempt += 1;
                match transient_retry_after("QueryPathInfo", attempt, &status, false) {
                    Some(delay) => tokio::time::sleep(delay).await,
                    // Status preserved as the anyhow root (downcast-able
                    // — `is_actionable_qpi_err` keys on it). The
                    // context embeds the status display so `{e}` at
                    // `stderr_err!` callers carries it (drops the
                    // pre-change `store gRPC: ` prefix from the old
                    // `GatewayError::Store` variant; `{e:#}` repeats
                    // the status once — `{e}` is the intended display).
                    None => {
                        let msg = format!("QueryPathInfo failed: {status}");
                        return Err(anyhow::Error::new(status).context(msg));
                    }
                }
            }
        }
    }
}

/// QueryRealisation with NotFound→None mapping. Any non-NotFound status
/// is returned as Err — caller MUST `stderr_err!` it. Never swallow.
///
/// Chokepoint for the CA-aware opcode handlers (40, 41, 43) and the
/// build-result store verification (`check_targets_against_store`,
/// reached by opcodes 9/36/46). `NotFound` is the *only* store status
/// that maps to wire-level "no result"; `Unavailable` / `DeadlineExceeded`
/// / `Internal` are infrastructure errors that the client must see via
/// `STDERR_ERROR` — otherwise (per the doc-comment on
/// `handle_query_derivation_output_map`) the client receives `outPath=""`
/// → `assert(maybeOutputPath)` at nix-build.cc:722 with no indication
/// the store was unreachable.
pub(super) async fn grpc_query_realisation(
    client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    drv_hash: [u8; 32],
    output_name: &str,
) -> anyhow::Result<Option<types::Realisation>> {
    let req = with_jwt(
        types::QueryRealisationRequest {
            drv_hash: drv_hash.to_vec(),
            output_name: output_name.to_string(),
        },
        jwt_token,
    )?;
    match rio_common::grpc::with_timeout(
        "QueryRealisation",
        DEFAULT_GRPC_TIMEOUT,
        client.query_realisation(req),
    )
    .await
    {
        Ok(resp) => Ok(Some(resp.into_inner())),
        Err(e)
            if e.downcast_ref::<tonic::Status>()
                .is_some_and(|s| s.code() == tonic::Code::NotFound) =>
        {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// For each floating-CA output of `drv`, query the Realisations table.
///
/// Returns `(modular_hash, name→output_path)`. NotFound entries are absent
/// from the map (caller falls back to `""` / `forced_build`).
/// `modular_hash` is `None` iff [`compute_modular_hash_cached`] failed
/// (already `warn!`-logged) — IA outputs are still resolvable from the
/// `.drv`, only floating-CA stays empty.
///
/// Non-NotFound store errors propagate as `Err` — caller `stderr_err!`s.
///
/// Shared resolver for opcodes 9/36/40/41/46 (the build opcodes reach it
/// via `check_targets_against_store`); before this extraction each
/// caller open-coded the same `compute_modular_hash_cached → per-output
/// QueryRealisation` loop with inconsistent error handling (two of the
/// four swallowed non-NotFound — see [`grpc_query_realisation`]).
///
/// [`compute_modular_hash_cached`]: crate::translate::compute_modular_hash_cached
pub(super) async fn resolve_floating_outputs(
    drv: &Derivation,
    drv_path: &str,
    store_client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    drv_cache: &HashMap<StorePath, Derivation>,
    hash_cache: &mut HashMap<String, [u8; 32]>,
) -> anyhow::Result<(Option<[u8; 32]>, HashMap<String, String>)> {
    let mut realized: HashMap<String, String> = HashMap::new();
    let has_floating = drv.outputs().iter().any(|o| o.path().is_empty());
    if !has_floating {
        return Ok((None, realized));
    }
    let Some(hash) = translate::compute_modular_hash_cached(drv, drv_path, drv_cache, hash_cache)
    else {
        // compute_modular_hash_cached already warn!-logged. IA outputs
        // still get their .drv paths; CA outputs stay unresolved.
        return Ok((None, realized));
    };
    for out in drv.outputs() {
        if !out.path().is_empty() {
            continue;
        }
        match grpc_query_realisation(store_client, jwt_token, hash, out.name()).await? {
            Some(r) => {
                realized.insert(out.name().to_string(), r.output_path);
            }
            None => {
                tracing::info!(
                    drv_hash = %hex::encode(hash),
                    output = %out.name(),
                    "no realisation for floating-CA output (not yet built)"
                );
            }
        }
    }
    Ok((Some(hash), realized))
}

/// Check validity via QueryPathInfo -- returns true if path exists.
pub(super) async fn grpc_is_valid_path(
    store_client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    path: &StorePath,
) -> anyhow::Result<bool> {
    Ok(grpc_query_path_info(store_client, jwt_token, path.as_str())
        .await?
        .is_some())
}

/// Max attempts for `Code::Aborted` retry in [`grpc_put_path`]. The
/// store returns Aborted when another upload holds the placeholder for
/// this path (I-068) or on PG serialization/deadlock conflicts — both
/// clear in one round-trip (.drv NARs are KB). GC no longer blocks
/// PutPath at all (I-192).
///
/// 50 ms base, ×2, full jitter, 2 s cap. 8 attempts → ≤~6 s budget —
/// generous for the remaining (fast-clearing) cases; kept as a safety
/// margin rather than tightened. Shared with rio-builder's PutPath
/// retry (`upload.rs`): both hit the same store-side placeholder
/// contention, so they use the same curve+budget.
///
/// The store now fronts each attempt with its own bounded wait on the
/// in-flight uploader (`store.put.concurrent-wait`, default 60 s), so
/// an Aborted reaching this loop means the winner outlived that
/// budget — each retry here buys another store-side wait window.
// pub(crate): the alert-seed axis pin in lib.rs
// (putpath_retry_attempt_axis_matches_the_emit_law) derives the seeded
// label product from this bound.
pub(crate) const PUT_PATH_ABORTED_MAX_ATTEMPTS: u32 = 8;
const PUT_PATH_BACKOFF: rio_common::backoff::Backoff = rio_common::backoff::Backoff {
    base: std::time::Duration::from_millis(50),
    mult: 2.0,
    cap: std::time::Duration::from_secs(2),
    jitter: rio_common::backoff::Jitter::Full,
};

// r[impl gw.putpath.emit-law]
/// bug_118: THE single emit site for the PutPath failure series. The
/// emit law binds to the OPERATION, not a callsite — this fn is the
/// only place in the module that names the metric, and every
/// failure-surfacing arm on every PutPath lane routes through one of
/// the two typed surfacing adapters below, which consume the failure
/// en route to the response (an arm that bypasses them has no error
/// value to return — the R24 shape on an observability law; the
/// W11-BI source census pins the discipline). The KEDA store
/// ScaledObject's demand-side scale-collapse inhibitor consumes
/// `sum(rate())` of exactly this series: pre-fix only the buffered
/// lane emitted, so a streaming-dominated store outage (all non-.drv
/// wopAddToStoreNar traffic + oversize AddMultiple) left the
/// inhibitor FLAT — the merged_bug_038 defect re-instantiated one
/// lane over.
fn emit_put_path_failure(class: &'static str) {
    metrics::counter!(
        "rio_gateway_putpath_retry_events_total",
        "class" => class,
    )
    .increment(1);
}

// r[impl gw.putpath.emit-law]
/// Surface one status-bearing PutPath failure observation (buffered
/// lane: every attempt's failure, retried or terminal; streaming
/// lane: the store's terminal Status): emits the class-labeled
/// counter and hands the status back for disposition.
fn surface_put_path_failure(status: tonic::Status) -> tonic::Status {
    emit_put_path_failure(rio_common::grpc::code_class_label(status.code()));
    status
}

// r[impl gw.putpath.emit-law]
/// Surface one statusless PutPath terminal failure (client NAR
/// short-read, task panic, pre-stream channel close): a store status
/// never existed, so the class is `"unknown"` BY DEFINITION — a
/// first-class member of the seeded `CODE_CLASS_LABELS` alphabet,
/// not a catch-all over it (a status-bearing failure downcasts and
/// classifies by code instead).
fn surface_put_path_failure_any(err: anyhow::Error) -> anyhow::Error {
    match err.downcast_ref::<tonic::Status>() {
        Some(s) => emit_put_path_failure(rio_common::grpc::code_class_label(s.code())),
        None => emit_put_path_failure(rio_common::grpc::code_class_label(tonic::Code::Unknown)),
    }
    err
}

/// Upload a path to the store via gRPC PutPath (metadata + NAR chunks).
///
/// Retries on `Code::Aborted` — concurrent same-path upload (store's
/// `put_path.rs` returns this when another writer holds the placeholder
/// row) OR PG serialization conflict (I-189; rio-common/src/grpc.rs
/// documents both as the same retryable shed). I-068: with the I-052
/// 32-way pipeline × N clients × shared closure, collisions are
/// guaranteed; before this retry the gateway surfaced Aborted as a hard
/// wopAddMultipleToStore failure and the client died mid-push.
///
/// merged_bug_097: non-Aborted failures take the same-file transient
/// lane ([`transient_retry_after`]) — the store's typed sheds
/// (`rio_common::grpc::STORE_SHED_CLASSES`: the NAR-budget
/// `ResourceExhausted` "retry" invitation) previously hit a terminal
/// arm and hard-failed the nix client's push despite the machinery
/// sitting in this file wired only to QueryPathInfo/GetPath. Every
/// failure observation emits the class-labeled
/// `rio_gateway_putpath_retry_events_total` (merged_bug_038, H8″ —
/// the inhibitor's series must move during reachability outages).
///
/// `nar_data` is held as `Arc<[u8]>` so each retry rebuilds the request
/// stream without copying the buffer. `info` is `Clone` (cheap — strings
/// and Vecs already heap-allocated).
///
/// `first_attempt_guard`: the singleflight Leader's [`LeaderGuard`]
/// (or `None` for fail-open / non-singleflight callers). Dropped via
/// `.take()` after the FIRST store response (success or fail) — the
/// guard's purpose is "signal followers when MY upload ATTEMPT is
/// decided", not "when I've exhausted retries". Followers wake after
/// one store RTT and retry concurrently from then on instead of
/// serializing behind this caller's full ~6s budget.
// r[impl gw.put.aborted-retry+2]
pub(super) async fn grpc_put_path(
    store_client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    service_signer: Option<&rio_auth::hmac::HmacSigner>,
    info: ValidatedPathInfo,
    nar_data: Vec<u8>,
    mut first_attempt_guard: Option<LeaderGuard>,
) -> anyhow::Result<bool> {
    let nar: std::sync::Arc<[u8]> = nar_data.into();
    let mut attempt = 0u32;
    let mut transient_attempt = 0u32;
    loop {
        let stream =
            rio_proto::client::chunk_nar_for_put(info.clone(), std::sync::Arc::clone(&nar));
        // emit-law exempt `?`: with_jwt's only fallible step is
        // `MetadataValue::try_from(base64url-ASCII)`, which cannot
        // fail on a real JWT (handler/mod.rs documents the `?` as
        // defensive). A failure here would be a programmer error in
        // rio_auth's encoding, not a PutPath observation.
        let mut req = with_jwt(stream, jwt_token)?;
        attach_service_token(&mut req, service_signer);
        let result = rio_common::grpc::with_timeout_status(
            "PutPath",
            GRPC_STREAM_TIMEOUT,
            store_client.put_path(req),
        )
        .await;
        // First store response observed — signal any followers. From
        // this point on (retry or terminal), every caller is
        // concurrent (the pre-singleflight shape). Idempotent: `.take()`
        // is None on every later iteration.
        drop(first_attempt_guard.take());
        let status = match result {
            Ok(resp) => return Ok(resp.into_inner().created),
            // THE class-labeled emit law (merged_bug_038, H8″; bound
            // to the operation by bug_118): every failure observation
            // at this chokepoint counts, labeled by its typed class —
            // Unavailable/DeadlineExceeded/… cannot be non-emitting
            // arms (R21: the uncounted terminal arm died). The store
            // ScaledObject's demand-side inhibitor consumes this
            // series; the label alphabet is
            // rio_common::grpc::CODE_CLASS_LABELS (boot-seeded per
            // class, lib.rs — bug_322 birth-gap discipline). The
            // surfacing fn IS the emit site (one per module).
            Err(status) => surface_put_path_failure(status),
        };
        // Buffered lane: retry on any Code::Aborted — the store
        // returns Aborted for placeholder-contention (I-068,
        // CONCURRENT_PUTPATH_MSG) AND PG serialization conflicts
        // (I-189; rio-common/src/grpc.rs:429), which both need this
        // 8-attempt budget. The streaming lane uses the narrower
        // `is_concurrent_putpath_aborted` predicate because it gates
        // the wait-then-adopt (a PG-conflict has nobody to wait on;
        // streaming can't replay regardless). The
        // `putpath_aborted_retry_emit_shape_pinned_across_lanes` test
        // pins this loop's emit/exhaust shape against
        // `poll_adopt_after_aborted`'s.
        if status.code() == tonic::Code::Aborted {
            attempt += 1;
            // I-168: dashboard-visible retry budget (was log-only).
            metrics::counter!(
                "rio_gateway_putpath_aborted_retries_total",
                "attempt" => attempt.to_string(),
            )
            .increment(1);
            if attempt >= PUT_PATH_ABORTED_MAX_ATTEMPTS {
                tracing::warn!(
                    store_path = %info.store_path,
                    attempts = attempt,
                    "PutPath: store still Aborted after retry budget; surfacing"
                );
                return Err(status.into());
            }
            // FULL jitter (`U(0, capᵃ]`): N clients retrying the
            // SAME path don't re-collide in lockstep, and the
            // I-068 placeholder case stays fast (first retry
            // ≤50 ms) while the I-168 mark-busy case gets a
            // multi-second window. `attempt-1` so attempt=1 uses
            // mult⁰ = base.
            let delay = PUT_PATH_BACKOFF.duration(attempt - 1);
            tracing::debug!(
                store_path = %info.store_path,
                attempt,
                backoff = ?delay,
                msg = %status.message(),
                "PutPath: store Aborted; retrying with exponential backoff"
            );
            tokio::time::sleep(delay).await;
        } else {
            // merged_bug_097: the store's typed sheds
            // (STORE_SHED_CLASSES — the NAR-budget ResourceExhausted
            // "retry" invitation) and transport-class blips absorb
            // through the same-file transient machinery the other
            // store unaries use; the classifier (is_transient) is a
            // superset of the shed set BY THE SHARED CONST. The
            // stream rebuilds from the Arc'd buffer, so replay is
            // safe (unlike the streaming path, which stays
            // non-retried by design).
            transient_attempt += 1;
            match transient_retry_after("PutPath", transient_attempt, &status, true) {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return Err(status.into()),
            }
        }
    }
}

/// Upload a path to the store, streaming NAR bytes from a reader.
///
/// Reads exactly `nar_size` bytes from `nar_reader` in `NAR_CHUNK_SIZE`
/// chunks and forwards each as a NarChunk. Forwards the client-declared
/// hash in the trailer — store re-hashes and validates (same security
/// property as [`grpc_put_path`]; the gateway is a dumb pipe here).
///
/// `nar_reader` must yield exactly `nar_size` bytes; short read = error.
/// Caller is responsible for the `nar_size <= MAX_NAR_SIZE` check.
///
/// NOT replayed on `Aborted` (unlike [`grpc_put_path`]): the reader is
/// consumed and the bytes are forwarded as they arrive, so there is
/// nothing to replay. sh-004: an `Aborted` carrying the I-068
/// placeholder-contention message ([`rio_proto::CONCURRENT_PUTPATH_MSG`])
/// instead enters wait-then-adopt — the pump already drains exactly
/// `nar_size` bytes regardless (the early-Ok wire-positioning contract
/// below), so the lane backs off via [`PUT_PATH_BACKOFF`], polls
/// [`grpc_query_path_info`], and returns `Ok(false)` once the
/// concurrent uploader's path exists. The retry budget and the
/// `rio_gateway_putpath_aborted_retries_total{attempt}` emit are
/// shared with the buffered lane (single-axis schema; same
/// store-side contention, same curve, same dashboard cell). The store
/// also fronts the race server-side (`store.put.concurrent-wait`),
/// resolving as `created: false` once the in-flight winner commits;
/// an Aborted only escapes when the winner outlives BOTH wait budgets.
///
/// `r[gw.put.singleflight]` participation is signal-only: every
/// streaming caller registers in the singleflight map at entry (one
/// becomes the registry Leader, the rest Followers) and uploads
/// regardless — no precheck, no buffer. The registry's only effect is
/// on Aborted+CONCURRENT: a Follower whose own upload hit the
/// placeholder-contention `Aborted` waits on the in-process leader's
/// completion signal then QPI ([`wait_then_qpi_if_follower`]) before
/// falling back to the budget poll. Other Errs surface immediately
/// (non-retryable rejection or wire mispositioned). The `guard`
/// (Leader) is dropped before any post-attempt poll so a Follower's
/// signal-wait does not span this caller's poll budget.
///
/// `wait_cap`: the Follower's bounded signal-wait. Synchronous call
/// sites (`handle_add_to_store_nar`, wire already past the NAR) pass
/// [`FOLLOWER_WAIT_CAP`]; the opcode-44 oversize-streaming branch
/// passes [`PIPELINE_FOLLOWER_WAIT_CAP`] (5s) — that branch runs
/// inside the per-entry loop and a long wait stalls the whole batch
/// with no progress to the client (the same hazard the spawned-task
/// branch was capped for; pre-singleflight worst case here was ~6s).
///
/// [`FOLLOWER_WAIT_CAP`]: super::singleflight::FOLLOWER_WAIT_CAP
/// [`PIPELINE_FOLLOWER_WAIT_CAP`]: super::singleflight::PIPELINE_FOLLOWER_WAIT_CAP
// r[impl gw.put.aborted-retry+2]
pub(super) async fn grpc_put_path_streaming<R: AsyncRead + Unpin>(
    ctx: PutCtx<'_>,
    wait_cap: std::time::Duration,
    info: ValidatedPathInfo,
    nar_reader: &mut R,
    nar_size: u64,
    client_nar_hash: Vec<u8>,
) -> anyhow::Result<bool> {
    // ~1 MiB in flight at 256 KiB chunks.
    const CHANNEL_BUF: usize = 4;

    let PutCtx {
        store_client,
        jwt_token,
        service_signer,
        sf,
        tenant,
    } = ctx;

    // r[impl gw.put.singleflight+2]
    // Signal-only: every caller uploads. The guard/follower split is
    // recorded so a Follower whose own upload hits Aborted+CONCURRENT
    // can wait on the in-process leader's signal before the budget
    // poll (other Errs surface immediately). Both arms emit at acquire
    // (Leader|Follower) so `sum({lane="streaming"})` counts every
    // streaming acquire — a follower whose own upload won the store
    // race would otherwise emit nothing.
    //
    // Guard lifetime: held across the WHOLE pump (dropped at the
    // explicit `drop(guard)` after the rpc result, or via RAII on the
    // Ok / Pump::Failed early-returns) — unlike the buffered lane,
    // which drops after the FIRST store response. There is only one
    // attempt here (bytes consumed), so "first response" == "result";
    // a same-path buffered follower (cross-opcode: opcode-44 ≤16 MiB
    // entry vs an opcode-39 streaming caller) parks on this Leader for
    // up to its `wait_cap` then fails open — bounded, no-worse than
    // the pre-singleflight concurrent upload it falls back to.
    let (guard, follower) = match sf.acquire(tenant, &info.store_path) {
        Disposition::Leader(g) => {
            emit_outcome(SfLane::Streaming, SfOutcome::Leader);
            (Some(g), None)
        }
        Disposition::Follower(f) => {
            emit_outcome(SfLane::Streaming, SfOutcome::Follower);
            (None, Some(f))
        }
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<types::PutPathRequest>(CHANNEL_BUF);

    // Metadata first. Zero nar_hash/nar_size in the PathInfo (the
    // trailer carries the authoritative pair), but DECLARED size set
    // (N1): this sender reads exactly `nar_size` bytes from the
    // reader — the size is knowable up front, so the store reserves
    // single-shot pre-stream instead of charging chunk-by-chunk
    // while holding.
    let store_path = info.store_path.clone();
    let mut raw: types::PathInfo = info.into();
    raw.nar_hash = Vec::new();
    raw.nar_size = 0;
    tx.send(types::PutPathRequest {
        msg: Some(types::put_path_request::Msg::Metadata(
            types::PutPathMetadata {
                info: Some(raw),
                declared_nar_size: nar_size,
            },
        )),
    })
    .await
    .map_err(|_| {
        // bug_118: terminal arm — surfaces through the emit law.
        surface_put_path_failure_any(
            GatewayError::GrpcStream("PutPath channel closed before metadata".into()).into(),
        )
    })?;

    // Drive the gRPC call. Clone: tonic Channel is Arc-backed.
    // JWT wrapped BEFORE the spawn — jwt_token's lifetime doesn't
    // extend into the 'static task. emit-law exempt `?`: see the
    // identical note at the buffered lane's `with_jwt` call (cannot
    // fail on a real base64url-ASCII JWT; programmer-error-only).
    let mut client = store_client.clone();
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
    let mut req = with_jwt(outbound, jwt_token)?;
    attach_service_token(&mut req, service_signer);
    // I-211: NO whole-stream timeout here — `GRPC_STREAM_TIMEOUT` is a
    // per-chunk IDLE bound applied in the pump (each `tx.send`) and on
    // the final response wait below, mirroring GetPath. The previous
    // whole-stream wrap meant a store parked on `NarBudget` (cold-start
    // burst, KEDA scaler blind ~2min post-deploy) hit DeadlineExceeded
    // at 300s regardless of progress; the idle bound trips only when the
    // store makes no progress on a single chunk for the window.
    // bug_118: the synthesized timeout below is a typed
    // `Status::deadline_exceeded` so it classifies, not launders through
    // anyhow into "unknown".
    // streaming-open-ban: the open routes through `bounded_open` and the
    // per-chunk idle detector IS the abort future — the pump /
    // response-wait fires `idle_tx` on expiry, so the Aborted arm is the
    // live bounding mechanism (not a `pending()` witness a future caller
    // could copy as the sanctioned way to satisfy the lint without
    // bounding anything). `bound` stays `MAX`: a finite value here is a
    // whole-stream deadline, which would re-introduce I-211.
    let (idle_tx, idle_rx) = tokio::sync::oneshot::channel::<()>();
    let mut rpc: tokio::task::JoinHandle<
        Result<tonic::Response<types::PutPathResponse>, tonic::Status>,
    > = tokio::spawn(async move {
        match rio_common::transport::bounded_open(
            async move {
                let _ = idle_rx.await;
            },
            std::time::Duration::MAX,
            client.put_path(req),
        )
        .await
        {
            rio_common::transport::OpenOutcome::Opened(r) => r,
            // Per-chunk idle bound fired (pump or response-wait sent
            // `idle_tx`). The contextual message is synthesized at the
            // outer site that fired it; this generic Err is observed
            // only if the outer site drops `idle_tx` without
            // synthesizing (it does not).
            rio_common::transport::OpenOutcome::Aborted => Err(tonic::Status::deadline_exceeded(
                "PutPath per-chunk idle bound",
            )),
            rio_common::transport::OpenOutcome::TimedOut { .. } => {
                unreachable!(
                    "bound is Duration::MAX (a finite bound is the I-211 whole-stream deadline)"
                )
            }
        }
    });

    // Read exactly nar_size bytes in NAR_CHUNK_SIZE chunks, forward each.
    // Backpressure: tx.send blocks when rpc isn't pulling. On a short read
    // we still drop tx and await rpc so the spawned task completes before
    // we return. A closed channel is NOT a pump error: it means the rpc
    // task has already completed (dropping rx) — but the rpc may have
    // returned Ok(created:false) early (store-side AlreadyComplete /
    // Concurrent-race after `drain_stream` timed out), so the pump must
    // STILL consume nar_size to honor the "reads exactly nar_size bytes"
    // contract callers depend on for wire positioning.
    enum Pump {
        /// Pump completed (or rx dropped early); reader is at `nar_size`.
        Done,
        /// Per-chunk idle bound fired (store parked); reader drained to
        /// `nar_size`.
        Idle,
        /// NarRead — client short read.
        Failed(anyhow::Error),
    }
    let pump = async {
        let mut remaining = nar_size;
        let mut chunk = vec![0u8; NAR_CHUNK_SIZE];
        while remaining > 0 {
            let n = (remaining.min(NAR_CHUNK_SIZE as u64)) as usize;
            // No per-chunk CLIENT-side timeout: a nix client reading
            // its source NAR off slow/contended storage may
            // legitimately gap >GRPC_STREAM_TIMEOUT between chunks. A
            // wedged client is its OWN session's problem; followers
            // are bounded by `FOLLOWER_WAIT_CAP` (defensive backstop)
            // and fail-open regardless.
            if let Err(e) = nar_reader.read_exact(&mut chunk[..n]).await {
                return Pump::Failed(
                    GatewayError::NarRead {
                        context: format!("at {} of {nar_size}", nar_size - remaining),
                        source: e,
                    }
                    .into(),
                );
            }
            remaining -= n as u64;
            // I-211: per-chunk idle bound. `tx.send` blocks once the
            // bounded(CHANNEL_BUF) channel is full — i.e. when the rpc
            // task isn't pulling because the store isn't reading the
            // stream. A parked store (NarBudget semaphore) shows up as a
            // stalled send; the store pulling a chunk is progress and
            // re-arms the deadline. A 4 GiB NAR completes as long as the
            // store accepts a chunk every <`GRPC_STREAM_TIMEOUT`.
            let (done, why) = match tokio::time::timeout(
                GRPC_STREAM_TIMEOUT,
                tx.send(types::PutPathRequest {
                    msg: Some(types::put_path_request::Msg::NarChunk(chunk[..n].to_vec())),
                }),
            )
            .await
            {
                Ok(Ok(())) => continue,
                // rx dropped → rpc task already finished. It MAY have
                // returned Ok(created:false) early, so drain nar_reader
                // to nar_size before returning — otherwise the caller's
                // framed reader is left mid-NAR and the next entry's
                // header parses garbage. If rpc_result is Err, that
                // surfaces via rpc_result? below regardless.
                Ok(Err(_)) => (Pump::Done, "store early-Ok"),
                // Idle bound hit. Same drain-to-nar_size before
                // signalling timeout to the caller.
                Err(_elapsed) => (Pump::Idle, "idle timeout"),
            };
            return match drain_nar_remaining(nar_reader, remaining, nar_size, why).await {
                Ok(()) => done,
                Err(e) => Pump::Failed(e),
            };
        }

        // Trailer: client-declared hash. Store validates independently.
        match tokio::time::timeout(
            GRPC_STREAM_TIMEOUT,
            tx.send(types::PutPathRequest {
                msg: Some(types::put_path_request::Msg::Trailer(
                    types::PutPathTrailer {
                        nar_hash: client_nar_hash,
                        nar_size,
                    },
                )),
            }),
        )
        .await
        {
            // Sent, or rx already dropped — either way the pump is done.
            Ok(_) => Pump::Done,
            Err(_elapsed) => Pump::Idle,
        }
    }
    .await;

    drop(tx); // close channel → ReceiverStream yields None → rpc completes

    // Error priority: pump error (NarRead — client short read) > rpc
    // error. A short read truncates the stream; the useful message is
    // "NAR read at X of Y", not "store rejected incomplete stream". The
    // pump's only Failed variant is NarRead — a closed channel returns
    // Done above, so an early store rejection (auth/quota/validation)
    // surfaces via rpc_result with the store's actual Status, not a
    // generic "channel closed". Hoisted ABOVE the response wait: when
    // the pump already knows the terminal NarRead error, waiting up to
    // GRPC_STREAM_TIMEOUT on a store that may be parked — and
    // discarding whatever it returns — is dead latency holding the SSH
    // channel; and an rpc-task panic at the join below would otherwise
    // `?`-surface BEFORE the pump error, inverting the documented
    // priority. Abort the rpc task (`idle_tx` — bounded_open's abort)
    // and await it so the spawned task completes before we return.
    // bug_118: exactly ONE emission per terminal failure — the error
    // that SURFACES is the one surfaced through the emit law (a
    // swallowed rpc error behind a winning pump error stays uncounted
    // by design: one operation, one terminal observation).
    let pump_idle = match pump {
        Pump::Failed(e) => {
            let _ = idle_tx.send(());
            let _ = (&mut rpc).await;
            return Err(surface_put_path_failure_any(e));
        }
        Pump::Idle => true,
        Pump::Done => false,
    };

    // I-211: the per-chunk idle bound also covers the final response
    // wait — the NarBudget park happens before the first chunk-ack, so a
    // tiny NAR (fewer than CHANNEL_BUF chunks; no send blocks) observes
    // the park HERE, not in the pump. On either idle the store is parked
    // and won't respond on stream-close: fire `idle_tx` (bounded_open's
    // abort) and synthesize the typed DeadlineExceeded the surfacing fn
    // expects (bug_118). One abort+synthesize tail; the two idle paths
    // differ only in the message — both feed the surfacing fn and
    // `transient_retry_after` keys on Code::DeadlineExceeded for retry
    // classification, so they MUST share one synthesis site.
    let rpc_join = if pump_idle {
        None
    } else {
        tokio::time::timeout(GRPC_STREAM_TIMEOUT, &mut rpc)
            .await
            .ok()
    };
    let rpc_result = match rpc_join {
        Some(join) => join.map_err(|e| {
            // bug_118: terminal arm (task panic — no store status).
            surface_put_path_failure_any(
                GatewayError::GrpcStream(format!("PutPath task panicked: {e}")).into(),
            )
        })?,
        None => {
            let why = if pump_idle {
                "stream idle" // pump-idle: store not pulling chunks
            } else {
                "response idle" // response-wait timeout: store parked
            };
            let _ = idle_tx.send(());
            let _ = (&mut rpc).await;
            Err(tonic::Status::deadline_exceeded(format!(
                "PutPath {why} for {GRPC_STREAM_TIMEOUT:?} (store parked on NarBudget)"
            )))
        }
    };

    let status = match rpc_result {
        Ok(resp) => return Ok(resp.into_inner().created),
        // bug_118: the surfacing fn IS the emit site — the store's
        // failure observation counts here, retried-by-adopt or
        // terminal (one operation, one observation; the buffered
        // lane's loop-head emit has the same shape).
        Err(status) => surface_put_path_failure(status),
    };
    // The guard's purpose is "signal followers when MY upload attempt
    // is done" — it is. The Ok early-return above (and the
    // Pump::Failed / task-join `?` arms further up) drop `guard` via
    // RAII at the same instant they return — no extra latency. Only
    // THIS arm continues into post-attempt work (signal-wait, budget
    // poll); holding the guard through it would make a parked Follower
    // wait this caller's full poll budget before its own, so drop
    // explicitly here. Woken followers each poll independently (same
    // as pre-singleflight; the signal saved them the upload, not the
    // poll). N followers × 7 QPIs is bounded by the same N×8 PutPath
    // attempts the pre-singleflight shape would have spent.
    drop(guard);
    // r[impl gw.put.singleflight+2]
    // Signal-wait on Aborted+CONCURRENT only (other Errs surface
    // immediately — non-retryable store rejection or wire
    // mispositioned; the Pump::Failed and task-join `?` arms above
    // return before this point for the same reason). On the I-068
    // placeholder-contention Aborted, a Follower waits on the
    // in-process leader's bounded signal then QPI before falling back
    // to the budget poll.
    if rio_proto::is_concurrent_putpath_aborted(&status) {
        let signal_adopted = match follower {
            Some(f) => {
                match wait_then_qpi_if_follower(f, wait_cap, store_client, jwt_token, &store_path)
                    .await?
                {
                    WaitOutcome::Adopted => {
                        emit_outcome(SfLane::Streaming, SfOutcome::Coalesced);
                        true
                    }
                    WaitOutcome::Miss => {
                        emit_outcome(SfLane::Streaming, SfOutcome::FollowerMiss);
                        false
                    }
                }
            }
            None => false,
        };
        // sh-004: wait-then-adopt on the I-068 placeholder-contention
        // Aborted. The reader is already drained to `nar_size` (the
        // pump's rx-dropped arm above), so the framed reader stays
        // positioned for the caller; the lane polls for the concurrent
        // uploader's result instead of replaying. Precedent:
        // rio-builder upload/chunked.rs
        // `is_concurrent_putpath_aborted` → wait-then-adopt.
        // bug_118 census: the original Aborted was already surfaced at
        // the rpc match above; the helper returns it verbatim on
        // budget-exhaust WITHOUT re-surfacing (one operation, one
        // observation). The helper owns the FULL attempt=1..MAX emit
        // range (no pre-emit here); QPI probe failures are split per
        // is_actionable_qpi_err (symmetric with
        // wait_then_qpi_if_follower).
        return poll_adopt_after_aborted(
            store_client,
            jwt_token,
            &store_path,
            status,
            signal_adopted,
        )
        .await;
    }
    // Non-CONCURRENT failure (any non-Aborted, and Aborted-without-
    // CONCURRENT_PUTPATH_MSG e.g. a PG serialization-conflict per
    // I-189): surface immediately. Asymmetry vs the buffered lane
    // (which retries every Aborted): this lane's bytes are consumed
    // and cannot be replayed, and a non-CONCURRENT Aborted has no
    // concurrent uploader to wait on — so `follower: Some(f)` is
    // dropped unused here. Pre-singleflight surfaced identically; the
    // signal-wait is strictly an Aborted+CONCURRENT optimization.
    Err(status.into())
}

/// Fetch NAR data from store via gRPC GetPath.
/// Returns (PathInfo, NAR bytes) or None if not found.
///
/// Delegates to `rio_proto::client::get_path_nar` — DO NOT inline that
/// helper's await structure here. Under `#[tokio::test(start_paused =
/// true)]`, the exact suspend-point layout determines whether tokio's
/// auto-advance fires the GRPC_STREAM_TIMEOUT before in-process gRPC
/// I/O completes (observed in wire_opcodes::build reconnect tests when
/// P0465 initially inlined this; reverted to delegation).
/// `GRPC_STREAM_TIMEOUT` is a per-chunk IDLE bound (I-211), not a
/// whole-call deadline — a 4 GiB NAR completes as long as chunks keep
/// arriving.
pub(crate) async fn grpc_get_path(
    store_client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    store_path: &str,
) -> anyhow::Result<Option<(ValidatedPathInfo, Vec<u8>)>> {
    use rio_proto::client::NarCollectError;
    let md = jwt_metadata(jwt_token);
    // r[impl gw.store.transient-retry]
    // Store-side `GetPath` traverses `try_substitute_on_miss` on a local
    // miss; admission rejection arrives as `NarCollectError::Stream(RE)`
    // before any chunks flow, so a retry replays cleanly (no bytes
    // consumed). `SizeExceeded`/`Validation`/`Io` are non-transient.
    let mut attempt = 0u32;
    loop {
        match rio_proto::client::get_path_nar(
            store_client,
            store_path,
            GRPC_STREAM_TIMEOUT,
            MAX_NAR_SIZE,
            &md,
        )
        .await
        {
            Ok(v) => return Ok(v),
            Err(NarCollectError::Stream(s)) => {
                attempt += 1;
                match transient_retry_after("GetPath", attempt, &s, true) {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => {
                        return Err(
                            GatewayError::Store(format!("GetPath for {store_path}: {s}")).into(),
                        );
                    }
                }
            }
            Err(e) => {
                return Err(GatewayError::Store(format!("GetPath for {store_path}: {e}")).into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering::SeqCst;

    /// sha256(b"nar!") — the mock store re-hashes and validates the
    /// trailer like the real one.
    const NAR_FIXTURE: &[u8] = b"nar!";
    const NAR_FIXTURE_SHA256: &str =
        "a5a407e7848d7d3863f1dbf17d78856ef27efce7310082eec2157ee03c7b17f3";

    fn put_info(path: &str) -> rio_proto::validated::ValidatedPathInfo {
        let mut nar_hash = vec![0u8; 32];
        hex::decode_to_slice(NAR_FIXTURE_SHA256, &mut nar_hash).expect("fixture hex");
        rio_proto::types::PathInfo {
            store_path: path.into(),
            nar_hash,
            nar_size: NAR_FIXTURE.len() as u64,
            ..Default::default()
        }
        .try_into()
        .expect("valid fixture path")
    }

    /// W10-BP (merged_bug_097): the store's typed NAR-budget shed
    /// (ResourceExhausted + "retry") is an explicit retry invitation
    /// — the producer's "absorbed by the upload plane's retry
    /// machinery" claim binds to STORE_SHED_CLASSES, and this caller's
    /// classifier must be a superset. Parameterized over the shed
    /// const: the consumer-census leg for the gateway PutPath lane.
    #[tokio::test]
    async fn put_path_absorbs_every_store_shed_class() {
        for (i, shed) in rio_common::grpc::STORE_SHED_CLASSES.into_iter().enumerate() {
            let (store, addr, _h) = rio_test_support::grpc::spawn_mock_store()
                .await
                .expect("mock store");
            match shed {
                tonic::Code::ResourceExhausted => {
                    store.faults.shed_next_puts.store(1, SeqCst);
                }
                tonic::Code::Aborted => {
                    store.faults.abort_next_puts.store(1, SeqCst);
                }
                other => panic!("unscripted shed class {other:?}: extend the mock faults"),
            }
            let mut client = rio_proto::StoreServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("connect");
            let info = put_info(&format!(
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa{i}-shed-1.0"
            ));
            let res =
                grpc_put_path(&mut client, None, None, info, NAR_FIXTURE.to_vec(), None).await;
            assert!(
                res.is_ok(),
                "left: the {shed:?} shed hits the terminal arm and the push \
                 hard-fails (the lost invited retry) / right: retried and \
                 absorbed; got {res:?}"
            );
        }
    }

    // r[verify gw.putpath.emit-law]
    /// W10-BQ (merged_bug_038, the H8'' emit law): every failure
    /// class observed at the PutPath chokepoint emits the
    /// class-labeled counter — an injected Unavailable outage must
    /// move the series (pre-fix the only emit arm was Aborted-only,
    /// so the inhibitor trigger was structurally flat during the
    /// reachability outages it guards).
    #[test]
    fn put_path_emit_law_counts_every_failure_class() {
        let rec = rio_test_support::metrics::CountingRecorder::default();
        let _guard = metrics::set_default_local_recorder(&rec);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (store, addr, _h) = rio_test_support::grpc::spawn_mock_store()
                .await
                .expect("mock store");
            // One Unavailable, then success: the retried observation
            // must still be counted.
            store.faults.fail_next_puts.store(1, SeqCst);
            let mut client = rio_proto::StoreServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("connect");
            let info = put_info("/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-emit-1.0");
            let res =
                grpc_put_path(&mut client, None, None, info, NAR_FIXTURE.to_vec(), None).await;
            assert!(
                res.is_ok(),
                "one Unavailable then success must absorb; got {res:?}"
            );
        });
        assert_eq!(
            rec.get("rio_gateway_putpath_retry_events_total{class=unavailable}"),
            1,
            "left: the inhibitor series is flat during an Unavailable \
             outage (the uncounted terminal arm) / right: the class-labeled \
             emit law counts it; keys seen: {:?}",
            rec.all_keys()
        );
    }

    /// W11-BH (bug_118): the emit law binds to the OPERATION — the
    /// STREAMING lane's failures must move the same class-labeled
    /// series the buffered lane emits. All non-.drv wopAddToStoreNar
    /// traffic plus oversize AddMultiple route through this lane, so
    /// a streaming-dominated store outage pre-fix left the KEDA
    /// scale-collapse inhibitor FLAT while every failure surfaced via
    /// `pump_result?`/`rpc_result?` with zero emission — the
    /// merged_bug_038 defect re-instantiated one lane over.
    ///
    /// Pre-fix red (the streaming-lane outage, counter flat):
    ///   left: the streaming lane surfaces the store failure with
    ///   ZERO emission (inhibitor flat) / right: every PutPath
    ///   terminal failure increments exactly one class cell on every
    ///   lane: `assertion failed ... left: 0 right: 1`.
    // r[verify gw.putpath.emit-law]
    #[test]
    fn put_path_streaming_failures_emit_the_same_law() {
        let rec = rio_test_support::metrics::CountingRecorder::default();
        let _guard = metrics::set_default_local_recorder(&rec);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (store, addr, _h) = rio_test_support::grpc::spawn_mock_store()
                .await
                .expect("mock store");
            // A store outage on the streaming lane: the put fails
            // Unavailable; the lane is non-retried by design, so the
            // failure is TERMINAL — and must still emit.
            store.faults.fail_next_puts.store(1, SeqCst);
            let mut client = rio_proto::StoreServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("connect");
            let info = put_info("/nix/store/cccccccccccccccccccccccccccccccc-strm-1.0");
            let mut nar_hash = vec![0u8; 32];
            hex::decode_to_slice(NAR_FIXTURE_SHA256, &mut nar_hash).expect("fixture hex");
            let mut reader = std::io::Cursor::new(NAR_FIXTURE.to_vec());
            let sf = crate::handler::singleflight::PutSingleflight::new();
            let ctx = PutCtx {
                store_client: &mut client,
                jwt_token: None,
                service_signer: None,
                sf: &sf,
                tenant: None,
            };
            let res = grpc_put_path_streaming(
                ctx,
                crate::handler::singleflight::FOLLOWER_WAIT_CAP,
                info,
                &mut reader,
                NAR_FIXTURE.len() as u64,
                nar_hash,
            )
            .await;
            assert!(
                res.is_err(),
                "the injected outage must surface; got {res:?}"
            );
        });
        assert_eq!(
            rec.get("rio_gateway_putpath_retry_events_total{class=unavailable}"),
            1,
            "left: the streaming lane surfaces the store failure with ZERO \
             emission (inhibitor flat during streaming-dominated outages) / \
             right: every PutPath terminal failure increments exactly one \
             class cell on every lane; keys seen: {:?}",
            rec.all_keys()
        );
    }
    /// W11-BI (bug_118, [GEN-SET]): the lane census — the emit law's
    /// single-site discipline pinned structurally over THIS module's
    /// source. Generator: the module source itself (include_str!);
    /// the census derives the populations by token scan instead of an
    /// author-typed list, so a new failure-surfacing arm that
    /// bypasses the chokepoint moves a counted population and goes
    /// red here.
    ///
    ///   (1) the metric literal appears exactly ONCE in production
    ///       code (inside `emit_put_path_failure` — the law has one
    ///       emit site);
    ///   (2) every `return Err(` in the two PutPath lane fns routes
    ///       through a surfacing adapter (`return Err(status.into())`
    ///       shapes are gone);
    ///   (3) the streaming lane's terminal `?` arms each name a
    ///       surfacing adapter (metadata-send, task-join, pump, rpc —
    ///       four sites, counted from the source).
    // r[verify gw.putpath.emit-law]
    #[test]
    fn put_path_emit_law_census_one_site_every_lane() {
        let src = include_str!("grpc.rs");
        let prod = &src[..src.find("#[cfg(test)]").expect("test module marker")];

        // (1) one emit site.
        assert_eq!(
            prod.matches("\"rio_gateway_putpath_retry_events_total\"")
                .count(),
            1,
            "the emit law has exactly ONE site (emit_put_path_failure); \
             inline emits of the law's series are the bug_118 shape"
        );

        // Slice the two lane fns (production region order: buffered
        // then streaming then get_path).
        let buf_start = prod
            .find("async fn grpc_put_path(")
            .expect("buffered lane fn");
        let strm_start = prod
            .find("async fn grpc_put_path_streaming")
            .expect("streaming lane fn");
        let strm_end = prod.find("async fn grpc_get_path").unwrap_or(prod.len());
        let buffered = &prod[buf_start..strm_start];
        let streaming = &prod[strm_start..strm_end];

        // (2) no naked terminal returns in either lane: every
        // `return Err(` names a surfacing adapter on the same
        // statement.
        for (lane, body) in [("buffered", buffered), ("streaming", streaming)] {
            for (i, _) in body.match_indices("return Err(") {
                let stmt = &body[i..body[i..].find(';').map(|e| i + e).unwrap_or(body.len())];
                assert!(
                    stmt.contains("surface_put_path_failure") || stmt.contains("status.into()"),
                    "{lane} lane: un-surfaced terminal arm: {stmt}"
                );
            }
            // The buffered lane's `status.into()` returns are lawful:
            // the status was ALREADY surfaced at the loop's single
            // observation point (every failure observation counts,
            // retried or terminal — emitting again at the terminal
            // would double-count).
        }
        assert!(
            buffered.contains("Err(status) => surface_put_path_failure(status)"),
            "buffered lane: the loop's failure observation must route \
             through the surfacing fn"
        );

        // (3) the streaming lane's four terminal arms. (The
        // poll-adopt-after-aborted call is NOT a fifth: the original
        // Aborted was already surfaced at the rpc match; the helper
        // returns it verbatim without re-surfacing.)
        assert_eq!(
            streaming.matches("surface_put_path_failure").count(),
            4,
            "streaming lane: four terminal arms (metadata-send, pump, \
             task-join, rpc) each surface through the law; a changed \
             count means an arm was added or bypassed — re-derive the \
             census"
        );
    }

    /// The buffered Aborted loop and `poll_adopt_after_aborted` differ
    /// in per-iteration body (replay-from-buffer vs QPI-poll; the
    /// buffered loop also interleaves transient-retry). Pin the SHAPE
    /// both produce — `attempt="1".."8"` exactly once
    /// each, exhaust at 8 — so a drift in one loop's emit position
    /// (loop-head vs post-body) or backoff index can't ship a
    /// `attempt=8` budget-exhausted signal that fires on one lane and
    /// not the other. `start_paused` so the ~6s of full-jitter backoff
    /// in each lane is virtual time (in-process duplex transport per
    /// `spawn_mock_store_inproc` — real TCP under start_paused fires
    /// auto-advance during kernel accept, §2.7).
    #[tokio::test(start_paused = true)]
    async fn putpath_aborted_retry_emit_shape_pinned_across_lanes() {
        let max = PUT_PATH_ABORTED_MAX_ATTEMPTS;
        let attempt_key =
            |a: u32| format!("rio_gateway_putpath_aborted_retries_total{{attempt={a}}}");

        // Buffered: store returns Aborted on every attempt → exhaust.
        let rec = rio_test_support::metrics::CountingRecorder::default();
        let _g = metrics::set_default_local_recorder(&rec);
        let (store, mut client) = rio_test_support::grpc::spawn_mock_store_inproc()
            .await
            .expect("inproc store");
        store.faults.abort_next_puts.store(max, SeqCst);
        let info = put_info("/nix/store/dddddddddddddddddddddddddddddddd-shape-1.0");
        let res = grpc_put_path(&mut client, None, None, info, NAR_FIXTURE.to_vec(), None).await;
        assert!(
            res.is_err(),
            "buffered: persistent Aborted must exhaust; got {res:?}"
        );
        for a in 1..=max {
            assert_eq!(
                rec.get(&attempt_key(a)),
                1,
                "buffered: attempt={a} must emit exactly once on exhaust; \
                 keys: {:?}",
                rec.all_keys()
            );
        }
        drop(_g);

        // Streaming-side: poll_adopt_after_aborted owns the FULL
        // attempt=1..MAX range (emits attempt=1 at entry, then polls
        // a never-seeded path → exhaust at attempt=8). Same store;
        // path absent. No pre-emit at the call site.
        let rec = rio_test_support::metrics::CountingRecorder::default();
        let _g = metrics::set_default_local_recorder(&rec);
        let path: rio_nix::store_path::StorePath =
            "/nix/store/iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii-shape-1.0"
                .parse()
                .expect("path");
        let res = poll_adopt_after_aborted(
            &mut client,
            None,
            &path,
            tonic::Status::aborted(rio_proto::CONCURRENT_PUTPATH_MSG),
            false,
        )
        .await;
        assert!(
            res.is_err(),
            "poll: never-seeded path must exhaust; got {res:?}"
        );
        for a in 1..=max {
            assert_eq!(
                rec.get(&attempt_key(a)),
                1,
                "streaming poll: attempt={a} must emit exactly once on \
                 exhaust (helper owns 1..=MAX); keys: {:?}",
                rec.all_keys()
            );
        }
    }

    /// `drain_nar_remaining`: a short-read (client EOF mid-NAR after
    /// store early-Ok'd) returns a typed `NarRead`/`UnexpectedEof` so
    /// the truncation surfaces with a position, not as garbage at the
    /// next entry's header parse. Exact-length read returns `Ok(())`.
    #[tokio::test]
    async fn drain_nar_remaining_short_read_is_typed_eof() {
        // 4 bytes available, 8 expected → short read.
        let mut short = std::io::Cursor::new(vec![0u8; 4]);
        let err = drain_nar_remaining(&mut short, 8, 16, "store early-Ok")
            .await
            .expect_err("short read must error");
        let gw = err
            .downcast_ref::<GatewayError>()
            .expect("typed GatewayError");
        assert!(
            matches!(gw, GatewayError::NarRead { source, .. }
                if source.kind() == std::io::ErrorKind::UnexpectedEof),
            "short read must surface as NarRead/UnexpectedEof; got {gw:?}"
        );
        // Exact-length: drains cleanly.
        let mut exact = std::io::Cursor::new(vec![0u8; 8]);
        drain_nar_remaining(&mut exact, 8, 16, "store early-Ok")
            .await
            .expect("exact-length drain must succeed");
    }

    /// F2 structural property: the buffered Leader's guard is dropped
    /// after the FIRST store response, not after the retry budget. A
    /// follower of a leader whose first attempt Aborted wakes after
    /// one store RTT (sem closed) instead of after the leader's full
    /// ~6s. Structural assertion: under `start_paused`, a `yield_now`
    /// poll loop prevents auto-advance — the leader's post-attempt-1
    /// backoff sleep stays parked while we observe `inflight_len()==0`
    /// AND `put_path_started==1` (guard dropped after exactly one
    /// store call, before the retry).
    #[tokio::test(start_paused = true)]
    async fn buffered_leader_drops_guard_after_first_attempt() {
        use crate::handler::singleflight::{Disposition, PutSingleflight};
        let (store, mut client) = rio_test_support::grpc::spawn_mock_store_inproc()
            .await
            .expect("inproc store");
        // Persistent Aborted — leader will exhaust its budget.
        store
            .faults
            .abort_next_puts
            .store(PUT_PATH_ABORTED_MAX_ATTEMPTS, SeqCst);
        let sf = PutSingleflight::new();
        let path: rio_nix::store_path::StorePath =
            "/nix/store/gggggggggggggggggggggggggggggggg-guard-1.0"
                .parse()
                .expect("path");
        let Disposition::Leader(g) = sf.acquire(None, &path) else {
            panic!("first acquire must be leader")
        };
        let Disposition::Follower(f) = sf.acquire(None, &path) else {
            panic!("second acquire must be follower")
        };
        assert_eq!(sf.inflight_len(), 1);
        let leader = tokio::spawn(async move {
            grpc_put_path(
                &mut client,
                None,
                None,
                put_info(path.as_str()),
                NAR_FIXTURE.to_vec(),
                Some(g),
            )
            .await
        });
        // Drive until the guard drops. yield_now() keeps this task
        // runnable so auto-advance cannot fire the leader's backoff
        // sleep — when inflight_len()==0 the leader is parked AT
        // attempt 1's backoff, not past it.
        while sf.inflight_len() != 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            store.faults.put_path_started.load(SeqCst),
            1,
            "LeaderGuard must drop after the FIRST store response — held \
             across retry means followers serialize behind the full ~6s \
             budget (observed N>1 attempts before guard-drop)"
        );
        // Follower's sem is already closed → wait_bounded resolves
        // signaled without auto-advance.
        let signaled = f.wait_bounded(std::time::Duration::from_secs(3600)).await;
        assert!(signaled, "follower must observe sem-closed, not timeout");
        // Drain the leader (auto-advance now fires the backoff sleeps).
        let r = leader.await.expect("join");
        assert!(r.is_err(), "persistent Aborted must exhaust; got {r:?}");
    }
}
