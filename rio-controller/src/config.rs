//! `rio-controller` binary configuration: layered-config-loaded `Config`
//! struct, clap `CliArgs` overlay, and the `ValidateConfig` bounds
//! checks. Extracted from `main.rs` so `tests/config_schema.rs` can
//! snapshot `schema_for!(Config)` into the committed
//! `tests/fixtures/config-schema.json` that `xtask regen docs-data`
//! reads.

use clap::Parser;
use serde::{Deserialize, Serialize};

use crate::reconcilers::nodeclaim_pool::NodeClaimPoolConfig;

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct Config {
    /// rio-scheduler upstream. Env: `RIO_SCHEDULER__ADDR` /
    /// `__BALANCE_HOST` / `__BALANCE_PORT`. `balance_host` used two
    /// ways: (1) injected into worker pods as
    /// `RIO_SCHEDULER__BALANCE_HOST`; (2) THIS process's autoscaler
    /// uses it for leader-aware ClusterStatus polling. `None` →
    /// single-channel via `addr` (ClusterIP — round-robins to the
    /// standby ~50% of the time with replicas=2).
    pub scheduler: rio_common::config::UpstreamAddrs,
    /// rio-store upstream. Env: `RIO_STORE__ADDR` / `__BALANCE_HOST`
    /// / `__BALANCE_PORT`. Injected into worker pod containers by
    /// the Pool reconciler. I-077: balance host needed so
    /// scaling rio-store 1→4 actually spreads load.
    pub store: rio_common::config::UpstreamAddrs,
    #[serde(flatten)]
    pub common: rio_common::config::CommonConfig,
    /// HTTP /healthz listen address. K8s livenessProbe hits this.
    pub health_addr: std::net::SocketAddr,
    /// GC cron interval (hours). 0 = disabled (reconciler not
    /// spawned). The cron calls StoreAdminService.TriggerGC with
    /// default params (dry_run=false, force=false, store's
    /// `DEFAULT_GC_GRACE_HOURS` grace). `store_addr` is the connect
    /// target — StoreAdminService
    /// is hosted on the store's gRPC port alongside StoreService.
    pub gc_interval_hours: u64,
    /// ADR-023 §13b NodeClaim pool reconciler. `enabled = false` =
    /// reconciler not spawned (legacy 12-NodePool mode). Env:
    /// `RIO_NODECLAIM_POOL__ENABLED` / `__DATABASE_URL` / `__LEASE_NAME`
    /// / `__NODE_CLASS_REF` / `__MAX_FLEET_CORES` / etc.
    pub nodeclaim_pool: NodeClaimPoolConfig,
    /// HMAC key for minting `x-rio-service-token` on AdminService
    /// calls. SAME file as the gateway/scheduler/store
    /// `service_hmac_key_path` (one shared `rio-service-hmac` Secret).
    /// `None` = dev mode (no header attached; scheduler's verifier is
    /// also `None` and passes through). Env:
    /// `RIO_SERVICE_HMAC_KEY_PATH`. See `r[sec.authz.service-token]`.
    pub service_hmac_key_path: Option<std::path::PathBuf>,
    /// ADR-023 §13a: pod `requests.memory` floor for the
    /// `rio.build/hw-bench-needed` gate (STREAM triad working-set
    /// safety). MUST match the scheduler's `[sla].hw_bench_mem_floor`;
    /// helm renders both from `sla.hwBenchMemFloor`. Env:
    /// `RIO_HW_BENCH_MEM_FLOOR`.
    pub hw_bench_mem_floor: u64,
    /// Cluster identity axis for the node-informer's exposure uids
    /// (merged_bug_001). `interrupt_samples` lives in the shared-PG
    /// (global-DB) topology of ADR-023 §2.13 and M_047's partial
    /// unique index on `event_uid` is table-GLOBAL, so every exposure
    /// idempotency key MUST carry the cluster
    /// (`exposure:{cluster}:{hw}:{window-slot}`) or two clusters'
    /// informers silently absorb each other's λ-denominator windows.
    /// MIRRORS the scheduler's `[sla].cluster`: helm renders BOTH from
    /// the one values expression (`scheduler.sla.cluster`, falling
    /// back to `karpenter.clusterName`, then `""`) into the two TOMLs
    /// — never set them apart by hand. Empty = single-cluster default
    /// (matches the scheduler's `DEFAULT ''` column). bug_022: the
    /// default is safe ONLY while this deployment's PG is private to
    /// it — two deployments both at `""` on one PG mint identical
    /// uids and silently absorb each other's λ evidence; the chart
    /// refuses to render an empty id when the external-secrets PG
    /// path (the shared-capable topology) is enabled, and the
    /// informer warns at activation on the empty default.
    pub cluster: String,
    /// Namespace the gateway pods run in. Non-empty ENABLES the
    /// [`crate::reconcilers::gateway_cost`] annotator (sh-028: stamp
    /// `pod-deletion-cost` = scraped `rio_gateway_connections_active`
    /// so KEDA scale-down evicts the least-loaded replica). Empty
    /// (default) → annotator not spawned (non-k8s `cargo run`, the
    /// standalone-fixture VM scenarios). Helm sets it from the
    /// downward-API `metadata.namespace` (gateway and controller share
    /// the system namespace). Env: `RIO_GATEWAY_NAMESPACE`.
    pub gateway_namespace: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            scheduler: rio_common::config::UpstreamAddrs::with_port(9001),
            store: rio_common::config::UpstreamAddrs::with_port(9002),
            // 9094: gateway=9090, scheduler=9091, store=9092,
            // worker=9093. Controller is next.
            common: rio_common::config::CommonConfig::new(9094),
            // Same +100 pattern as gateway/worker.
            health_addr: rio_common::default_addr(9194),
            // 24h: typical store growth between sweeps is a few
            // thousand paths. Lower values are fine for VM tests.
            gc_interval_hours: 24,
            nodeclaim_pool: NodeClaimPoolConfig::default(),
            service_hmac_key_path: None,
            // 8 GiB: matches `rio_scheduler::sla::config::
            // default_hw_bench_mem_floor`. STREAM triad's 3×4×LLC
            // working set tops out ~4.6 GiB on c7a.48xlarge.
            hw_bench_mem_floor: 8 * (1 << 30),
            // Single-cluster default — mirrors the scheduler's
            // `[sla].cluster` `DEFAULT ''`.
            cluster: String::new(),
            gateway_namespace: String::new(),
        }
    }
}

#[derive(Parser, Serialize, Default)]
#[command(name = "rio-controller", about = "Kubernetes operator for rio-build")]
pub struct CliArgs {
    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics_addr: Option<std::net::SocketAddr>,

    #[arg(long)]
    #[serde(skip_serializing_if = "Option::is_none")]
    health_addr: Option<std::net::SocketAddr>,
}

impl rio_common::config::ValidateConfig for Config {
    /// Bounds checks on operator-settable fields. Extracted from
    /// `main()` so the checks are unit-testable without spinning up
    /// the full controller (kube-client connect, reconciler spawn).
    /// Every `ensure!` documents a specific crash or silent-wrong
    /// that occurs AFTER startup if the bad value gets through.
    fn validate(&self) -> anyhow::Result<()> {
        self.scheduler
            .ensure_required("scheduler.addr", "controller")?;
        rio_common::config::ensure_required(
            &self.nodeclaim_pool.database_url,
            "nodeclaim_pool.database_url",
            "controller",
        )?;
        anyhow::ensure!(
            self.nodeclaim_pool.max_fleet_cores > 0,
            "nodeclaim_pool.max_fleet_cores must be > 0"
        );
        // Validated once at boot so the per-cell-per-tick read site
        // (`compute_warm_floor_for`) can trust the field — the prior
        // per-tick `is_nan() / clamp(0,1)` guard was the wrong
        // altitude (immutable config sanitised on every read) and the
        // sibling f64 knobs (max_consolidation_time, lead_time_seed,
        // default_lead_time_seed) had no equivalent guard.
        let cap_ratio = self.nodeclaim_pool.backlog_floor_cap_ratio;
        anyhow::ensure!(
            (0.0..=1.0).contains(&cap_ratio),
            "nodeclaim_pool.backlog_floor_cap_ratio must be in [0.0, 1.0] \
             (got {cap_ratio}); 0.0 disables the floor, 1.0 = never \
             idle-reap while admitted backlog exists",
        );
        // `compute_warm_floor_for` computes ⌈live × ratio⌉ at
        // milli-precision; a sub-‰ nonzero value (0.0 < r < 0.001)
        // rounds to 0 milli — operator intent ("nonzero floor")
        // silently becomes "floor disabled". Reject at boot so the
        // per-tick read site can use a plain round with no
        // `0 if r>0.0 => 1` special-case. Helm renders the ratio via
        // `rio.requiredFloatTOML` (full precision, always a TOML float
        // literal) so a sub-‰ helm override reaches THIS ensure
        // verbatim — helm and direct-TOML agree on operator feedback
        // (loud reject, not silent round-to-0.000 by the chart).
        anyhow::ensure!(
            cap_ratio == 0.0 || cap_ratio >= 0.001,
            "nodeclaim_pool.backlog_floor_cap_ratio must be 0.0 or ≥ 0.001 \
             (got {cap_ratio}); sub-‰ nonzero rounds to 0 at the \
             milli-precision ⌈live × ratio⌉ read site"
        );
        // `compute_warm_floor_for`'s milli-quantise scopes its own
        // correctness to "any 3-decimal-place input"; enforce that
        // precondition here so a 4th-dp tie (e.g. 0.0025 → ratio_milli
        // = round(2.5) = 3) cannot diverge from the documented
        // `⌈registered × ratio⌉`. Round-trip via the same quantise the
        // read site uses; tolerance bounds float64 representation noise
        // (≤1e-13 at this scale) without admitting any operator-typed
        // 4th decimal (smallest delta: 5e-4 at the half-tie).
        let milli = (cap_ratio * 1000.0).round();
        anyhow::ensure!(
            (milli / 1000.0 - cap_ratio).abs() < 1e-9,
            "nodeclaim_pool.backlog_floor_cap_ratio must have at most 3 \
             decimal places (got {cap_ratio}); the read site quantises \
             to milli-precision and a 4th-dp value diverges from the \
             documented ⌈registered × ratio⌉"
        );
        // `≥ 0 ∧ finite` is field-intrinsic, not warm-floor-coupled —
        // checked unconditionally. Both knobs are read by the
        // NA-threshold / hold_open path regardless of `cap_ratio`.
        //
        // Why `≥ 0` (NOT `> 0`): 0.0 is a no-op for both consumers —
        // `consolidate_after`'s `(boot_median/2).max(min)` lets the
        // model floor win, and `hold_open_threshold`'s `.max(na)`
        // clamps `max=Some(0.0)` to bare `na` — so a previously-booted
        // `min[k]=0.0` / `max=0.0` override remains valid after
        // rollout (origin/main accepts it; rejecting it here would
        // crash-loop a config the upgrade didn't change). The
        // staleness bound collapsing to 0 under a 0.0 override is the
        // operator's explicit choice and is itself a no-op at the
        // shipped `cap_ratio=0.0`. Only negative/NaN/Inf are rejected
        // — those are nonsensical regardless of consumer.
        if let Some(max) = self.nodeclaim_pool.max_consolidation_time {
            anyhow::ensure!(
                max.is_finite() && max >= 0.0,
                "nodeclaim_pool.max_consolidation_time must be a finite \
                 non-negative float when set (got {max})"
            );
        }
        for (k, &v) in &self.nodeclaim_pool.min_consolidation_time {
            anyhow::ensure!(
                v.is_finite() && v >= 0.0,
                "nodeclaim_pool.min_consolidation_time[{k:?}] must be a \
                 finite non-negative float (got {v})"
            );
        }
        Ok(())
    }
}

rio_common::impl_has_common_config!(Config);
