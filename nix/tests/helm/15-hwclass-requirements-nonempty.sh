# Every `scheduler.sla.hwClasses.*` entry MUST carry a non-empty
# AUTHORED `requirements` list. bug_044: the vmtest-full.yaml overlay
# defined `hwClasses.vmtest` with `labels` only; helm rendered
# `requirements = []`, `SlaConfig::validate` rejected it, and 14
# k3sFull VM scenarios timed out on scheduler crash-loop.
#
# scheduler.yaml now unconditionally injects the instance-local-nvme
# partition requirement (Gt "0" / DoesNotExist) into every class, so
# the RENDERED `requirements` is never empty — checking the rendered
# TOML is structurally vacuous. Check the AUTHORED values instead: a
# class with `requirements: []` (or omitted) would match every
# ebs-only instance type across all categories/generations/arches —
# the over-broad-ceiling shape the original guard existed to catch.
# The Rust-side `!def.requirements.is_empty()` ensure (config.rs)
# retains value for non-helm config paths.

check_authored() {
  local label="$1" file="$2"
  # to_entries → name + requirements length per class. yq emits
  # `null` for an absent key; `// [] | length` normalizes to 0.
  bad=$(yq '
    .scheduler.sla.hwClasses // {}
    | to_entries[]
    | select((.value.requirements // [] | length) == 0)
    | .key' "$file")
  if [ -n "$bad" ]; then
    echo "FAIL ($label): hwClasses with empty/absent authored requirements:" >&2
    printf '%s\n' "$bad" | sed 's/^/    /' >&2
    echo "  (only constraint would be the template-injected nvme partition —" >&2
    echo "   class matches every ebs-only instance type; over-broad ceiling)" >&2
    return 1
  fi
  n=$(yq '.scheduler.sla.hwClasses // {} | length' "$file")
  echo "  $label: $n hwClasses, all with non-empty authored requirements"
}

# Non-degeneracy: prod values.yaml MUST define at least one class (a
# path/key rename would otherwise pass vacuously via `// {}`).
n_prod=$(yq '.scheduler.sla.hwClasses | length' values.yaml)
[ "${n_prod:-0}" -gt 0 ] || {
  echo "FAIL: prod values.yaml .scheduler.sla.hwClasses is empty/absent" >&2
  echo "      (key path renamed? this check is vacuous)" >&2
  exit 1
}

check_authored prod values.yaml
check_authored vmtest-full values/vmtest-full.yaml
