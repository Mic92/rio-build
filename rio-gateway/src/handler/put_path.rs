//! `PutPath` lane wrappers over the pure [`super::singleflight`]
//! mechanism.
//!
//! Two lanes, two roles for the registry:
//!
//! - **Buffered lane** ([`grpc_put_path_singleflight`]): the first
//!   caller becomes the leader and uploads; followers wait on the
//!   leader's [`LeaderGuard`](super::singleflight::LeaderGuard) drop
//!   then `QueryPathInfo`. On miss (leader failed) the follower fails
//!   open — it still has `nar_data`, so it uploads directly via
//!   [`grpc_put_path`] (concurrently with any other miss-followers;
//!   bounded by that fn's own Aborted retry — exactly the
//!   pre-singleflight shape).
//! - **Streaming lane** ([`grpc_put_path_streaming`]): every caller
//!   uploads (no precheck — the bytes arrive once and cannot be
//!   buffered without unbounded memory). The registry participates
//!   only as a wait signal on Aborted+CONCURRENT: a Follower whose
//!   own upload hit the placeholder-contention `Aborted` waits on the
//!   in-process leader's signal then QPI before falling back to the
//!   budget poll. Strictly no-worse in success rate than
//!   pre-singleflight (every session uploads either way); adds up to
//!   [`FOLLOWER_WAIT_CAP`] latency when racing a wedged in-process
//!   leader (see that const's doc for the trade-off).
//!
//! The Adopted return is correct without the in-process Leader's
//! PutPath ever reaching the store: `QueryPathInfo` applies the same
//! tenant-visibility gate as the castore surface
//! (`r[store.tenant.valid-paths-filter]`), so `Some(_)` ⇒ the caller's
//! tenant has visibility — whichever process wrote the junction row
//! (cross-replica leader, or this one).
//!
//! [`grpc_put_path_streaming`]: super::grpc::grpc_put_path_streaming
//! [`FOLLOWER_WAIT_CAP`]: super::singleflight::FOLLOWER_WAIT_CAP

use std::time::Duration;

use rio_common::tenant::NormalizedName;
use rio_nix::store_path::StorePath;
use rio_proto::StoreServiceClient;
use rio_proto::validated::ValidatedPathInfo;
use tonic::transport::Channel;

use super::SessionContext;
use super::grpc::{grpc_put_path, grpc_query_path_info};
use super::singleflight::{Disposition, Follower, PutSingleflight};

/// Deny-list classifier for the post-wait QPI probe's `Err` arm. Both
/// QPI-probe sites ([`wait_then_qpi_if_follower`] and
/// [`poll_adopt_after_aborted`](super::grpc::poll_adopt_after_aborted))
/// call this so the split stays symmetric.
///
/// Returns `true` for status codes that are user-actionable and will
/// not change on retry — those propagate so the user sees the error
/// instead of a fail-open success that masks an authz/input misconfig.
/// Everything else (including DeadlineExceeded / Internal / Cancelled
/// / the `is_transient` set, and any non-`Status` anyhow root) is
/// infra-transient → caller debug-logs and treats as Miss /
/// not-yet-present (fail-open). The pre-singleflight shape had no QPI
/// probe at all, so a probe's infra-failure cannot make a follower's
/// outcome worse than that baseline — the deny-list keeps the
/// "no-worse" guarantee while still surfacing genuine rejections.
pub(super) fn is_actionable_qpi_err(e: &anyhow::Error) -> bool {
    use tonic::Code;
    e.downcast_ref::<tonic::Status>().is_some_and(|s| {
        // refusal-census: allow(QPI-probe propagate-vs-fail-open
        //   deny-list — not a refusal adjudication seam; judge_refusal
        //   is too broad: DeadlineExceeded/Internal/Cancelled must
        //   fail-open per the no-worse-than-pre-singleflight guarantee)
        matches!(
            s.code(),
            Code::PermissionDenied
                | Code::InvalidArgument
                | Code::NotFound
                | Code::Unauthenticated
                | Code::FailedPrecondition
                | Code::Unimplemented
        )
    })
}

/// `lane` axis of `rio_gateway_putpath_singleflight_total`. Separates
/// the two emission semantics (buffered: one-per-acquire; streaming:
/// leader on every acquire, follower-side outcomes only on a
/// post-failure signal-wait).
#[derive(Clone, Copy)]
pub(super) enum SfLane {
    Buffered,
    Streaming,
}

impl SfLane {
    fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Streaming => "streaming",
        }
    }
}

/// `outcome` axis of `rio_gateway_putpath_singleflight_total`. The
/// enum IS the closed alphabet — adding a variant is the (only) way to
/// add a label value, and rustc's exhaustiveness check is the pin.
#[derive(Clone, Copy)]
pub(super) enum SfOutcome {
    /// First caller for `(tenant, path)` — uploads.
    Leader,
    /// Streaming-lane acquire-time: an in-process leader exists; this
    /// caller uploads anyway (no precheck). Paired with [`Leader`] so
    /// `sum({lane="streaming",outcome=~"leader|follower"})` counts
    /// every streaming acquire — without it a follower whose own
    /// upload won the store race emits nothing and the per-lane call
    /// volume reads low. Buffered lane never emits this (its follower
    /// resolves to [`Coalesced`] or [`FollowerMiss`]).
    ///
    /// [`Leader`]: Self::Leader
    /// [`Coalesced`]: Self::Coalesced
    /// [`FollowerMiss`]: Self::FollowerMiss
    Follower,
    /// Follower's post-wait QueryPathInfo found the path.
    Coalesced,
    /// Follower's post-wait QueryPathInfo did NOT find the path
    /// (leader failed/cancelled/wedged) OR the QPI probe itself
    /// failed non-actionably (debug-logged inside
    /// [`wait_then_qpi_if_follower`]; actionable propagates per
    /// [`is_actionable_qpi_err`]). A degraded QPI plane (infra errors
    /// while PutPath works) inflates this label and reads as a low
    /// dedup ratio — check the debug log before concluding
    /// singleflight is ineffective.
    FollowerMiss,
}

impl SfOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Follower => "follower",
            Self::Coalesced => "coalesced",
            Self::FollowerMiss => "follower_miss",
        }
    }
}

/// Emit `rio_gateway_putpath_singleflight_total{outcome,lane}`.
pub(super) fn emit_outcome(lane: SfLane, outcome: SfOutcome) {
    metrics::counter!(
        "rio_gateway_putpath_singleflight_total",
        "lane" => lane.as_str(),
        "outcome" => outcome.as_str()
    )
    .increment(1);
}

/// Borrow-view over the per-session state every PutPath helper needs.
/// Mirrors [`crate::SessionShared`]'s fix one layer up: collapses the
/// 5-field unpack (`ctx.shared.put_singleflight`, `ctx.tenant_name`,
/// `store_client`, `jwt_token`, `service_signer`) every
/// `opcodes_write.rs` call site open-codes, so the next per-process
/// shared field widens one struct instead of two signatures × N call
/// sites. Per-call-SITE state only — `wait_cap` stays a separate
/// param (it differs at the same handler's two branches). Construct
/// via [`SessionContext::put_ctx`] where the borrow originates from a
/// live `SessionContext`; the spawned-task call sites in
/// `handle_add_multiple_to_store` build the literal directly from
/// owned clones.
pub(crate) struct PutCtx<'a> {
    pub store_client: &'a mut StoreServiceClient<Channel>,
    pub jwt_token: Option<&'a str>,
    pub service_signer: Option<&'a rio_auth::hmac::HmacSigner>,
    pub sf: &'a PutSingleflight,
    pub tenant: Option<&'a NormalizedName>,
}

impl SessionContext {
    /// Borrow this session as a [`PutCtx`]. Refreshes the JWT
    /// (lazily, via [`SessionJwt::token`](super::SessionJwt::token))
    /// and field-splits the rest. Holds `&mut self.store_client` plus
    /// shared borrows of `jwt`/`shared`/`tenant_name` — callers that
    /// also need `&mut self.drv_cache` take that borrow before/after
    /// (disjoint fields; the Rust borrow checker handles the split at
    /// the call site, not across this method boundary).
    pub(super) fn put_ctx(&mut self) -> PutCtx<'_> {
        // Field-split: `jwt.token()` takes `&mut self.jwt` and returns
        // a borrow of it; the remaining fields are disjoint, so NLL
        // accepts the simultaneous `&mut self.store_client`.
        PutCtx {
            jwt_token: self.jwt.token(),
            store_client: &mut self.store_client,
            service_signer: self.shared.service_signer.as_deref(),
            sf: &self.shared.put_singleflight,
            tenant: self.tenant_name.as_ref(),
        }
    }
}

/// Result of [`wait_then_qpi_if_follower`].
pub(super) enum WaitOutcome {
    /// Leader's signal fired (or wait timed out) and QueryPathInfo
    /// found the path — caller adopts as `created=false`.
    Adopted,
    /// Leader's signal fired (or wait timed out) but QueryPathInfo did
    /// NOT find the path — leader failed/cancelled/stalled. Caller
    /// falls back (buffered: fail-open upload; streaming: budget poll
    /// or surface).
    Miss,
}

/// Streaming-lane post-Aborted wait. Called from
/// [`grpc_put_path_streaming`] when this caller's own upload hit
/// Aborted+CONCURRENT and it is a singleflight Follower (an in-process
/// leader was uploading `(tenant, path)`). Waits on the leader's
/// completion signal (bounded by `cap`) then QPI. Returns
/// [`WaitOutcome::Adopted`] if the path now exists,
/// [`WaitOutcome::Miss`] otherwise — including when QPI itself failed
/// non-actionably (debug-logged here; a probe's infra-failure is NOT a
/// PutPath observation per `r[gw.putpath.emit-law]`, and both callers
/// fail-open on Miss). An ACTIONABLE QPI status (PermissionDenied,
/// InvalidArgument, … — see [`is_actionable_qpi_err`]) propagates as
/// `Err` — the user must see it, not a fail-open success that masks an
/// authz misconfig.
///
/// [`grpc_put_path_streaming`]: super::grpc::grpc_put_path_streaming
pub(super) async fn wait_then_qpi_if_follower(
    follower: Follower,
    cap: Duration,
    store_client: &mut StoreServiceClient<Channel>,
    jwt_token: Option<&str>,
    path: &StorePath,
) -> anyhow::Result<WaitOutcome> {
    if !follower.wait_bounded(cap).await {
        tracing::warn!(
            %path, ?cap,
            "PutPath singleflight: follower wait cap reached \
             (leader wedged); probing anyway"
        );
    }
    match grpc_query_path_info(store_client, jwt_token, path.as_str()).await {
        Ok(Some(_)) => Ok(WaitOutcome::Adopted),
        Ok(None) => Ok(WaitOutcome::Miss),
        Err(e) if is_actionable_qpi_err(&e) => Err(e),
        Err(e) => {
            tracing::debug!(
                %path, error = %e,
                "PutPath singleflight: follower QPI probe failed non-actionably; \
                 treating as Miss (fail-open)"
            );
            Ok(WaitOutcome::Miss)
        }
    }
}

/// Buffered-lane singleflight wrapper around [`grpc_put_path`]. The
/// first caller becomes Leader and uploads; a Follower waits on the
/// Leader's signal (bounded by `wait_cap`) then QPI. On `Adopted` it
/// returns `Ok(false)`; on `Miss` (leader failed, cancelled, or
/// wedged) or non-actionable QPI `Err` it **fails open** — uploads
/// directly via [`grpc_put_path`], concurrently with any other
/// miss-followers (an actionable QPI `Err` propagates per
/// [`is_actionable_qpi_err`]). No re-acquire loop:
/// serializing N miss-followers through leadership multiplied
/// time-to-error by N under a persistent store fault; concurrent
/// fail-open is bounded by [`grpc_put_path`]'s own Aborted retry —
/// exactly the pre-singleflight shape. Between the leader's guard drop
/// and the follower's fail-open upload, the singleflight map is empty
/// for the QPI's transient-retry window (~2s); a 3rd caller arriving
/// in that window becomes a fresh Leader and uploads concurrently with
/// this follower's fail-open — exactly the pre-singleflight shape (all
/// callers concurrent), so no worse than before. Covers every buffered
/// `PutPath` call site in `opcodes_write.rs` (wopAddToStoreNar `.drv`,
/// wopAddToStore, wopAddTextToStore, wopAddMultipleToStore small-entry
/// pipeline). Emits exactly one
/// `rio_gateway_putpath_singleflight_total{outcome,lane="buffered"}`
/// per call.
///
/// The Leader's [`LeaderGuard`](super::singleflight::LeaderGuard) is
/// dropped after the **first** store response (passed into
/// [`grpc_put_path`] as `first_attempt_guard`): the guard's purpose is
/// "signal followers when MY upload ATTEMPT is decided", not "when
/// I've exhausted retries". A leader retrying its 8-attempt Aborted
/// budget no longer parks followers for the full ~6s — they wake after
/// one store RTT, QPI, fail-open if absent, and retry concurrently
/// from then on (the pre-singleflight shape after the first attempt).
/// Under persistent store-side Aborted, time-to-error stays ~6s, not
/// the ~12s a guard-across-full-budget gives.
///
/// `wait_cap`: synchronous call sites (wire already past the NAR) pass
/// [`FOLLOWER_WAIT_CAP`]; the wopAddMultipleToStore spawned-task site
/// passes [`PIPELINE_FOLLOWER_WAIT_CAP`] — that task blocks the
/// wire-read pipeline at backpressure depth, so a long wait stalls the
/// whole batch.
///
/// [`FOLLOWER_WAIT_CAP`]: super::singleflight::FOLLOWER_WAIT_CAP
/// [`PIPELINE_FOLLOWER_WAIT_CAP`]: super::singleflight::PIPELINE_FOLLOWER_WAIT_CAP
// r[impl gw.put.singleflight+2]
pub(crate) async fn grpc_put_path_singleflight(
    ctx: PutCtx<'_>,
    wait_cap: Duration,
    info: ValidatedPathInfo,
    nar_data: Vec<u8>,
) -> anyhow::Result<bool> {
    match ctx.sf.acquire(ctx.tenant, &info.store_path) {
        Disposition::Leader(g) => {
            // Emit-before-await: counts the ACQUIRE (this caller is
            // the leader), not the upload outcome. `grpc_put_path`'s
            // pre-store `with_jwt(..)?` is emit-law-exempt (cannot
            // fail on real JWTs — see that fn's note), so no phantom
            // Leader skews the dedup-ratio numerator in practice.
            emit_outcome(SfLane::Buffered, SfOutcome::Leader);
            grpc_put_path(
                ctx.store_client,
                ctx.jwt_token,
                ctx.service_signer,
                info,
                nar_data,
                Some(g),
            )
            .await
        }
        Disposition::Follower(f) => {
            match wait_then_qpi_if_follower(
                f,
                wait_cap,
                ctx.store_client,
                ctx.jwt_token,
                &info.store_path,
            )
            .await?
            {
                WaitOutcome::Adopted => {
                    emit_outcome(SfLane::Buffered, SfOutcome::Coalesced);
                    Ok(false)
                }
                // Leader failed (or QPI probe failed non-actionably —
                // debug-logged inside the helper; actionable
                // propagated above via `?`); we have bytes; upload
                // directly. Concurrent fail-open is bounded by
                // grpc_put_path's own Aborted retry — the
                // pre-singleflight shape.
                WaitOutcome::Miss => {
                    emit_outcome(SfLane::Buffered, SfOutcome::FollowerMiss);
                    tracing::debug!(
                        path = %info.store_path, lane = "buffered",
                        "PutPath singleflight: leader failed/absent; fail-open"
                    );
                    grpc_put_path(
                        ctx.store_client,
                        ctx.jwt_token,
                        ctx.service_signer,
                        info,
                        nar_data,
                        None,
                    )
                    .await
                }
            }
        }
    }
}
