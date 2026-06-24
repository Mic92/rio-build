# sh-043: Sprig `default` treats integer 0 as empty — an operator
# setting `karpenter.nodeclaimPool.maxInflightUnlaunched: 0` as an
# emergency mint-halt would have rendered `= 50`. The chart-level
# default in values.yaml covers nil; the template MUST NOT shadow it
# (the 47-template-default-ban single-default convention). Explicit 0
# is the meaningful kill-switch the law's own doc treats as halt.
#
# THE Sprig nil→0 lecture (single home; controller.yaml/_helpers.tpl
# point HERE): `int64 nil`/`float64 nil` coerce to 0, so `--set …=null`
# silently renders the dangerous 0 (a 0s lead-time seed reaps every
# NodeClaim before boot; 0 inflight is the operator's mint-halt
# kill-switch and MUST be explicit; a 0 maxLeadTime caps every alert
# threshold to its floor). The chart has no values.schema.json, so
# Helm applies no nullability guard. `rio.requiredInt`/
# `rio.requiredFloat` (_helpers.tpl) wrap every numeric .Values leaf
# in `required`, which checks nil/"" only — explicit 0 still passes.
# r4: per-mechanism close — every sibling in the [nodeclaim_pool]/[sla]
# scalar block goes through the helpers, not just the two r3 named.

. "$(dirname "$0")/_lib.sh"

# Explicit 0 renders 0 (NOT the Sprig-swallowed 50; NOT a `required`
# refusal — Helm `required` checks nil/"" only).
got=$(render_controller_toml --set karpenter.nodeclaimPool.maxInflightUnlaunched=0 \
  | toml_int_key max_inflight_unlaunched)
test "$got" = "0" || {
  echo "FAIL: maxInflightUnlaunched=0 rendered max_inflight_unlaunched=$got, want 0" >&2
  echo "  (Sprig 'default N' treats integer 0 as empty — the operator's mint-halt" >&2
  echo "   kill-switch was silently swallowed; sh-043-r1)" >&2
  exit 1
}

# Unset renders the values.yaml default (50).
got=$(render_controller_toml | toml_int_key max_inflight_unlaunched)
test "$got" = "50" || {
  echo "FAIL: unset maxInflightUnlaunched rendered $got, want values.yaml default 50" >&2
  exit 1
}

# nil → render REFUSES (the planted-red gate leg). Without the
# `required` wrapper, `int64 nil` / `float64 nil` coerce to 0.
err=$TMPDIR/nil-guard.err
if render_karpenter --set karpenter.nodeclaimPool.maxInflightUnlaunched=null >/dev/null 2>"$err"; then
  echo "FAIL: maxInflightUnlaunched=null rendered — required guard fail-open (nil→0 is the mint-halt)" >&2
  exit 1
fi
grep -q "maxInflightUnlaunched must be set" "$err" || {
  echo "FAIL: maxInflightUnlaunched=null refused but without naming the key:" >&2
  sed 's/^/  /' "$err" >&2
  exit 1
}
if render_karpenter --set scheduler.sla.defaultLeadTimeSeed=null >/dev/null 2>"$err"; then
  echo "FAIL: defaultLeadTimeSeed=null rendered — required guard fail-open (nil→0.0 reaps every NodeClaim before boot)" >&2
  exit 1
fi
grep -q "defaultLeadTimeSeed must be set" "$err" || {
  echo "FAIL: defaultLeadTimeSeed=null refused but without naming the key:" >&2
  sed 's/^/  /' "$err" >&2
  exit 1
}
# r4: ONE more sibling proves the per-mechanism sweep (rio.requiredFloat
# covers maxLeadTime in controller.yaml + scheduler.yaml +
# prometheusrule.yaml — three call sites, one guard message).
if render_karpenter --set scheduler.sla.maxLeadTime=null >/dev/null 2>"$err"; then
  echo "FAIL: maxLeadTime=null rendered — rio.requiredFloat fail-open (nil→0.0 caps StuckPending/BootTimeoutLoop thresholds to floor)" >&2
  exit 1
fi
grep -q "maxLeadTime must be set" "$err" || {
  echo "FAIL: maxLeadTime=null refused but without naming the key:" >&2
  sed 's/^/  /' "$err" >&2
  exit 1
}

# rio.requiredFloatTOML: %v full-precision then .0-suffix-if-integer.
# A sub-‰ override must reach Config::validate verbatim (loud reject),
# NOT be rounded to 0.000 by the chart (silent disable). A whole-number
# override must render as a TOML float literal (0.0, not 0 — toml-rs
# rejects an integer literal for an f64 field).
got=$(render_controller_toml --set karpenter.nodeclaimPool.backlogFloorCapRatio=0.0004 \
  | { grep -E '^backlog_floor_cap_ratio = ' || true; })
test "$got" = "backlog_floor_cap_ratio = 0.0004" || {
  echo "FAIL: backlogFloorCapRatio=0.0004 rendered '$got', want 0.0004 (full precision — helm/direct-TOML channel agreement)" >&2
  exit 1
}
got=$(render_controller_toml --set karpenter.nodeclaimPool.backlogFloorCapRatio=1 \
  | { grep -E '^backlog_floor_cap_ratio = ' || true; })
test "$got" = "backlog_floor_cap_ratio = 1.0" || {
  echo "FAIL: backlogFloorCapRatio=1 rendered '$got', want 1.0 (TOML float literal, not integer)" >&2
  exit 1
}
if render_karpenter --set karpenter.nodeclaimPool.backlogFloorCapRatio=null >/dev/null 2>"$err"; then
  echo "FAIL: backlogFloorCapRatio=null rendered — rio.requiredFloatTOML fail-open" >&2
  exit 1
fi
grep -q "backlogFloorCapRatio must be set" "$err" || {
  echo "FAIL: backlogFloorCapRatio=null refused but without naming the key:" >&2
  sed 's/^/  /' "$err" >&2
  exit 1
}

# rio.requiredFloat non-finite guard (planted-red): NaN/Inf accepted by
# Sprig float64 → would render `NaN`/`+Inf` (invalid TOML / PromQL).
# One %.1f TOML-sink callsite proves the guard fires from
# requiredFloat itself, not only the TOML wrapper.
if render_karpenter --set scheduler.sla.maxLeadTime=NaN >/dev/null 2>"$err"; then
  echo "FAIL: maxLeadTime=NaN rendered — rio.requiredFloat non-finite guard fail-open" >&2
  exit 1
fi
grep -q "maxLeadTime must be a finite float" "$err" || {
  echo "FAIL: maxLeadTime=NaN refused but without the finite-guard message:" >&2
  sed 's/^/  /' "$err" >&2
  exit 1
}
# controller.yaml minConsolidationTime[k] < 0 guard (planted-red, sibling
# maxConsolidationTime guard is symmetric). 0.0 is ACCEPTED (no-op vs
# boot_median/2 floor); only negative is rejected.
if render_karpenter --set 'karpenter.nodeclaimPool.minConsolidationTime.gpu=-1' >/dev/null 2>"$err"; then
  echo "FAIL: minConsolidationTime[gpu]=-1 rendered — <0 guard fail-open" >&2
  exit 1
fi
grep -q 'minConsolidationTime\["gpu"\] must be ≥ 0' "$err" || {
  echo "FAIL: minConsolidationTime[gpu]=-1 refused but without the ≥0 message:" >&2
  sed 's/^/  /' "$err" >&2
  exit 1
}
