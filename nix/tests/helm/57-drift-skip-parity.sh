# `xtask k8s deploy --wait-drift` skips NodePools listed in
# DRIFT_SKIP_NODEPOOLS (deploy.rs) because their NodeClaims stay
# `Drifted=True` by design — they carry `budgets:[{nodes:"0",
# reasons:[Drifted]}]` in values.yaml. d10edc802 added rio-store to
# DRIFT_SKIP_NODEPOOLS but the second `wait_drift_settled(&[])` caller
# in `rotate_general` was missed; the structural fix is this bijection
# check (the helm-56 nvme-partition pattern):
#
#   {NodePools with budgets[nodes:"0",reasons:[Drifted]]}
#       == DRIFT_SKIP_NODEPOOLS
#
# Both directions: a future Drifted-0 pool without a skip-list entry
# would block --wait-drift forever; a skip-list entry without the
# budget would silently exempt a converging pool from the wait.

. "$(dirname "$0")/_lib.sh"

# rust side: extract the const from staged deploy.rs (see misc-checks.nix
# .deploy-source.rs staging). The const is a one-line `&[&str]` array.
rust_set=$(grep -E '^pub\(crate\) const DRIFT_SKIP_NODEPOOLS:' .deploy-source.rs \
  | grep -oE '"[^"]+"' | tr -d '"' | sort)
[ -n "$rust_set" ] || {
  echo "FAIL: could not extract DRIFT_SKIP_NODEPOOLS from deploy.rs" >&2
  echo "      (const renamed/moved/multi-line? staging in misc-checks.nix broken?)" >&2
  exit 1
}

# helm side: NodePools whose disruption budgets include nodes:"0" with
# Drifted in reasons. Read AUTHORED values.yaml directly — the budget
# is per-pool config, not template-derived.
helm_set=$(yq '
  .karpenter.nodePools[]
  | select(.budgets[]?
           | (.nodes == "0" and (.reasons // [] | contains(["Drifted"]))))
  | .name' values.yaml | sort)

if [ "$rust_set" != "$helm_set" ]; then
  echo "FAIL: DRIFT_SKIP_NODEPOOLS ⇔ values.yaml Drifted-0 budget bijection violated" >&2
  echo "  deploy.rs DRIFT_SKIP_NODEPOOLS:" >&2
  printf '%s\n' "$rust_set" | sed 's/^/    /' >&2
  echo "  values.yaml NodePools with budgets[nodes:0,reasons:[Drifted]]:" >&2
  printf '%s\n' "$helm_set" | sed 's/^/    /' >&2
  echo "  → add the missing pool to DRIFT_SKIP_NODEPOOLS (xtask/src/k8s/eks/deploy.rs)" >&2
  echo "    AND a rotate-<pool> xtask subcommand, or drop the budget." >&2
  exit 1
fi

# Non-degeneracy: at least one pool on each side. An empty intersection
# (e.g. const renamed → grep matches nothing → both sides empty) would
# pass vacuously above.
n=$(printf '%s\n' "$rust_set" | grep -c .)
[ "$n" -ge 1 ] || {
  echo "FAIL: drift-skip parity check is vacuous (0 pools)" >&2
  exit 1
}

echo "drift-skip parity: $n pools — DRIFT_SKIP_NODEPOOLS ⇔ values.yaml Drifted-0 budgets"
