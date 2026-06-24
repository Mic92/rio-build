# sh-017 single-gen-everywhere: every builder hwClass pins EXACTLY ONE
# instance-generation (`In ["N"]`, len==1). Capacity depth across
# gen 6+7+8 comes from the ladder closure (hi → hi-g7 → mid → lo),
# never from widening a parent's `instance-generation In [...]` —
# widening pollutes the per-class hw_perf factor (mixed-gen samples
# under one cell key) and is the live_101 cache-poison-workaround
# anti-pattern this fragment prevents reverting to.
#
# Allowlisted exceptions: hi-nvme-x86-g7 + mid-nvme-x86 stay
# `["6","7"]` because gen-7 x86 c/m/r have NO d-variants (a `["7"]`
# pin would be a (0,0)-ceiling exclusion). Retire the allowlist entry
# the day AWS ships c7id/m7id/r7id.
#
# Scope: classes with an `instance-generation` requirement using
# operator `In` (the band-tier builders). `Gt` users (metal, fetcher)
# are out of scope by construction.

. "$(dirname "$0")/_lib.sh"

T=$TMPDIR/hwclass-single-gen
rm -rf "$T"; mkdir -p "$T"

allow="hi-nvme-x86-g7 mid-nvme-x86"

render_scheduler_toml >"$T/sched.toml"

# {name, n} for every class with an `In` instance-generation
# requirement. Exactly one such requirement per class is asserted by
# 56-hwclass-nvme-partition.sh's adjacent type/arity guards; here we
# only check `values | length`.
yq -p toml -o json '.sla.hw_classes' "$T/sched.toml" \
  | jq -r 'to_entries[]
      | .key as $name
      | .value.requirements[]
      | select(.key == "karpenter.k8s.aws/instance-generation"
               and .operator == "In")
      | "\($name) \(.values | length)"' \
  >"$T/gen.txt"

# Non-degeneracy: at least one class scanned (catches a path/key
# rename silently emptying the predicate).
[ -s "$T/gen.txt" ] || {
  echo "FAIL: zero hwClasses with instance-generation In — predicate vacuous" >&2
  echo "      (sla.hw_classes path or requirement-key rename?)" >&2
  exit 1
}

bad=$(while read -r name n; do
  case " $allow " in *" $name "*) continue ;; esac
  [ "$n" -eq 1 ] || echo "    $name (len=$n)"
done <"$T/gen.txt")

if [ -n "$bad" ]; then
  echo "FAIL: sh-017 single-gen invariant — these builder hwClasses carry" >&2
  echo "  instance-generation In with len≠1 (widen via ladder, not here):" >&2
  echo "$bad" >&2
  echo "  Allowlist (gen-7-x86-no-d-variants): $allow" >&2
  exit 1
fi

# Self-check: a synthetic widen MUST be flagged.
if render_scheduler_toml \
     --set-json 'scheduler.sla.hwClasses.hi-ebs-x86.requirements[1].values=["7","8"]' \
     >"$T/bad.toml" 2>/dev/null \
   && n=$(yq -p toml -o json '.sla.hw_classes."hi-ebs-x86".requirements[]
            | select(.key=="karpenter.k8s.aws/instance-generation")
            | .values | length' "$T/bad.toml") \
   && [ "$n" -eq 1 ]; then
  echo "FAIL: planted ['7','8'] widen on hi-ebs-x86 not observable — fragment regression" >&2
  exit 1
fi

echo "hwclass single-gen: $(wc -l <"$T/gen.txt") In-gen classes; allowlist {$allow}; sh-017 invariant holds"
