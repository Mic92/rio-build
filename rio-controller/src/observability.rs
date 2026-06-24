//! Metric series ownership: boot-seeded alert series (merged_bug_236 —
//! the rio-scheduler C3 pattern extended to the controller).
//!
//! Every PrometheusRule `expr:`-referenced controller counter is born
//! at 0 from [`crate::describe_metrics`] (the bug_322 birth-gap class:
//! `increase(...) > 0` and `count(sum by (cell)(rate(...)) > 0)`
//! evaluate an ABSENT series until the first increment, so the first
//! drop/reap burst after a fresh rollout is invisible to the alert).
//!
//! Two seeding planes:
//!
//! - [`ALERT_SEEDED_COUNTERS`] — static label products, seeded
//!   unconditionally at boot ([`seed_alert_counters`]).
//! - [`seed_per_cell_reap_series`] — `rio_controller_nodeclaim_
//!   {reaped,reap_suppressed}_total` carry a config-derived `cell`
//!   axis the static table cannot know; the `by (cell)` alert needs
//!   per-cell series birth, so the {reap,suppress}-reasons × live-cell
//!   product is seeded at `HwClassConfig` load/refresh (`.absolute(0)`
//!   is idempotent — re-seeding on every 300 s refresh is free).
//!
//! The alert-parity test (`tests/alert_metrics.rs`) fails if a rule
//! references a counter missing from the table; the
//! `alert-parity-adoption` misc-check fails if any component's metrics
//! reach a shipped alert expr without that component carrying the
//! parity test at all — the CLASS chokepoint that makes "new
//! component, seeded alerts forgotten" CI-red instead of a silent
//! birth gap.
// r[impl obs.metric.alert-counter-seeded]

/// One boot-seeded counter family: bare name plus its closed label
/// axis. Mirrors `rio_test_support::metrics::SeededCounter` so the
/// parity test consumes this exact table.
pub struct SeededSeries {
    pub name: &'static str,
    pub label: Option<(&'static str, &'static [&'static str])>,
}

/// `rio_controller_nodeclaim_reaped_total`'s closed `reason` set
/// (health::ReapReason::as_str ∪ the vanished emit site — `idle`
/// joined the enum with bug_112's lane re-type).
pub const REAP_REASONS: &[&str] = &["ice", "boot-timeout", "dead", "vanished", "idle"];

/// The backlog-warm-floor suppression reason — named so emit sites
/// reference THIS, never `SUPPRESS_REASONS[0]` (a positional read flips
/// silently when the slice grows or reorders; the sibling
/// [`REAP_REASONS`] axis uses named `ReapReason::as_str()` at every
/// emit site for the same reason).
pub const SUPPRESS_BACKLOG_FLOOR: &str = "backlog_floor";

/// `rio_controller_nodeclaim_reap_suppressed_total`'s closed `reason`
/// set. Single member today; the const exists so a second suppress
/// reason joins the same SeededSeries closed-axis discipline as
/// [`REAP_REASONS`] / [`INTENT_DROP_REASONS`] (one edit, both emit and
/// seed sites covered) instead of two open-coded literals drifting.
pub const SUPPRESS_REASONS: &[&str] = &[SUPPRESS_BACKLOG_FLOOR];

/// `rio_controller_nodeclaim_intent_dropped_total`'s closed `reason`
/// set (cover sizing, pool-coverage retain, hosting-class resolve,
/// ICE-mask exhaustion, ceiling lookup).
pub const INTENT_DROP_REASONS: &[&str] = &[
    "all_cells_ice_masked",
    "exceeds_cell_cap",
    // merged_bug_013: the forecast half of the masked split — the
    // ready lane alerts, this one observes (the witnessed-ready-bit
    // keying keeps live_050(a)'s loud lane to solved waiting demand).
    "forecast_all_cells_ice_masked",
    "no_hosting_class",
    "no_pool_covers",
    // live_050(a): the READY all-masked population (solved demand,
    // named hosting classes) — split from `all_cells_ice_masked` so
    // the silently-starved population the live hang measured has its
    // own series; minted at the `PlacementOutcome` fold.
    "ready_all_cells_ice_masked",
    "unknown_hw_class",
];

/// Every alert-`expr:`-referenced rio_controller counter. The
/// `reaped_total` entry's static seed births the reason axis only —
/// the cell-crossed series the `by (cell)` alert groups on are born by
/// [`seed_per_cell_reap_series`] when the config arrives.
pub const ALERT_SEEDED_COUNTERS: &[SeededSeries] = &[
    SeededSeries {
        name: "rio_controller_nodeclaim_intent_dropped_total",
        label: Some(("reason", INTENT_DROP_REASONS)),
    },
    SeededSeries {
        name: "rio_controller_nodeclaim_reaped_total",
        label: Some(("reason", REAP_REASONS)),
    },
];

/// Birth every [`ALERT_SEEDED_COUNTERS`] series at 0. Called from
/// [`crate::describe_metrics`] — `rio_common::server` installs the
/// real exporter via `init_metrics` BEFORE `describe_metrics()`, so
/// the seeds land on the scrape surface from boot.
pub fn seed_alert_counters() {
    for s in ALERT_SEEDED_COUNTERS {
        match s.label {
            None => metrics::counter!(s.name).absolute(0),
            Some((axis, values)) => {
                for v in values {
                    metrics::counter!(s.name, axis => *v).absolute(0);
                }
            }
        }
    }
}

/// Birth the `reasons × cells` product for
/// `rio_controller_nodeclaim_reaped_total` and the per-cell
/// `rio_controller_nodeclaim_reap_suppressed_total{reason="backlog_floor"}`
/// — both group `by (cell)`, so each (reason, cell) series must exist
/// from the moment the cell is configured, not from its first
/// reap/suppression. The suppressed counter's only write site is
/// inside the floor gate (consolidate.rs), which never runs at
/// `cap_ratio=0.0` — the documented read-only rollout step would
/// otherwise show no-data per cell while the sibling
/// warm_floor/backlog_pending gauges (Phase-0 zero-written) show 0:
/// the bug_322 birth-gap class. Called at `HwClassConfig` load/refresh
/// with the live cell set; `.absolute(0)` is idempotent so refresh
/// re-seeding is free and never resets a counted value.
pub fn seed_per_cell_reap_series<I: IntoIterator<Item = String>>(cells: I) {
    // Table-driven via the same [`SeededSeries`] shape as
    // [`ALERT_SEEDED_COUNTERS`] so a third per-cell reason-labeled
    // counter is one row, not a third open-coded inner loop.
    // r[impl ctrl.nodeclaim.backlog-floor]
    const PER_CELL: &[SeededSeries] = &[
        SeededSeries {
            name: "rio_controller_nodeclaim_reaped_total",
            label: Some(("reason", REAP_REASONS)),
        },
        SeededSeries {
            name: "rio_controller_nodeclaim_reap_suppressed_total",
            label: Some(("reason", SUPPRESS_REASONS)),
        },
    ];
    for cell in cells {
        for s in PER_CELL {
            let (axis, values) = s.label.expect("per-cell seeds always carry a reason axis");
            for v in values {
                metrics::counter!(s.name, axis => *v, "cell" => cell.clone()).absolute(0);
            }
        }
    }
}
