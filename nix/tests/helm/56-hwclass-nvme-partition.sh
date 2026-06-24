# scheduler.yaml injects the instance-local-nvme partition on every
# hwClass as a single-source bijection — `Gt "0"` ⇔ nodeClass=rio-nvme,
# `DoesNotExist` ⇔ nodeClass≠rio-nvme — so neither half can be
# forgotten by per-class authoring:
#  · d-variants cannot land on rio-default/rio-metal (the AMI's
#    rio-kubelet-mount NVMe branch would leave xvdb+xvdc
#    attached-unmounted)
#  · ebs-only types cannot land on rio-nvme (instanceStorePolicy:RAID0
#    would find zero instance-store devices to stripe)
# Asserted here:
#  (a) every nvme class renders Gt ["0"]; every non-nvme renders
#      DoesNotExist [] — exactly one local-nvme requirement per class
#  (b) the values-required guard `fail`s on an In requirement with
#      `values` omitted (e.g. typo'd as singular `value:`)
#  (c) the values-type guard `fail`s on a SCALAR `values` (incl. the
#      sprig-falsy 0/"" case the old `and .values …` short-circuit
#      hid) AND on a list-of-non-string `values: [7]` (which the
#      `toString` arity check would mask but `toJson` preserves into
#      the rendered TOML as an integer array)
#  (d) the template-owned-key guard `fail`s on an AUTHORED
#      instance-local-nvme requirement (the key is injected, not
#      per-class authored; an authored copy makes the conjunction
#      unsatisfiable)
#  (e) the per-operator arity guard `fail`s on Gt with len≠1, Gt
#      with non-integer, Exists with non-empty, unknown operator —
#      coverage matches `config::validate_shape`

. "$(dirname "$0")/_lib.sh"

T=$TMPDIR/hwclass-nvme-partition
rm -rf "$T"; mkdir -p "$T"

# Planted-RED helper: render with one extra requirement, expect helm
# `fail` with stderr matching $want. Used for (b)-(e).
must_fail_with() {
  local label=$1 want=$2 reqjson=$3
  local errf=$T/$label.err
  if render_karpenter --set-json "scheduler.sla.hwClasses.mid-ebs-x86.requirements[0]=$reqjson" >/dev/null 2>"$errf"; then
    echo "FAIL: ($label) rendered — guard should fail at helm-template time" >&2
    exit 1
  fi
  grep -q "$want" "$errf" || {
    echo "FAIL: ($label) error does not match /$want/:" >&2
    sed 's/^/  /' "$errf" >&2
    exit 1
  }
}

render_scheduler_toml >"$T/sched.toml"

# (a): for each hw_classes table, the local-nvme requirement is a
# bijection on node_class. yq's TOML mode walks the tables.
yq -p toml -o json '.sla.hw_classes' "$T/sched.toml" \
  | yq -p json -o json 'to_entries[] | {"name": .key, "node_class": .value.node_class, "nvme_reqs": [.value.requirements[] | select(.key == "karpenter.k8s.aws/instance-local-nvme") | {"op": .operator, "vals": .values}]}' \
  >"$T/classes.jsonl"

# nvme classes: exactly one nvme requirement, Gt ["0"].
bad_nvme=$(yq -p json -o json '
  select(.node_class == "rio-nvme")
  | select((.nvme_reqs | length) != 1
        or .nvme_reqs[0].op != "Gt"
        or (.nvme_reqs[0].vals | length) != 1
        or .nvme_reqs[0].vals[0] != "0")
  | .name' "$T/classes.jsonl")
# non-nvme classes: exactly one nvme requirement, DoesNotExist [].
bad_ebs=$(yq -p json -o json '
  select(.node_class != "rio-nvme")
  | select((.nvme_reqs | length) != 1
        or .nvme_reqs[0].op != "DoesNotExist"
        or (.nvme_reqs[0].vals | length) != 0)
  | .name' "$T/classes.jsonl")
n=$(yq -p json '.name' "$T/classes.jsonl" | wc -l)
n_nvme=$(yq -p json 'select(.node_class == "rio-nvme") | .name' "$T/classes.jsonl" | wc -l)
n_ebs=$((n - n_nvme))
# Non-degeneracy: at least one class on EACH side. A nodeClass rename
# (rio-nvme → X) would otherwise pass vacuously — every class falls to
# the ≠rio-nvme branch and bad_nvme is trivially empty.
[ "$n_nvme" -gt 0 ] && [ "$n_ebs" -gt 0 ] || {
  echo "FAIL: bijection check is vacuous — n=$n nvme=$n_nvme ebs=$n_ebs" >&2
  echo "      (zero hw_classes extracted, OR no class on one side of" >&2
  echo "      the rio-nvme partition — nodeClass renamed? ConfigMap" >&2
  echo "      name / data key / [sla.hw_classes] path drifted?)" >&2
  exit 1
}
[ -z "$bad_nvme$bad_ebs" ] || {
  echo "FAIL: instance-local-nvme partition bijection violated:" >&2
  echo "  (Gt [\"0\"] ⇔ nodeClass=rio-nvme; DoesNotExist [] ⇔ otherwise; exactly one)" >&2
  printf '%s\n%s\n' "$bad_nvme" "$bad_ebs" | sed '/^$/d; s/^/    /' >&2
  exit 1
}

# (b): planted RED — `In` with omitted `values` must fail at render.
must_fail_with empty-values "needs non-empty" \
  '{"key":"karpenter.k8s.aws/instance-category","operator":"In"}'

# (c): planted REDs — type guard. Scalar `values` (incl. the
# sprig-falsy 0 the old `and .values …` short-circuit hid) and
# list-of-non-string `[7]` (which `toString` masks for the arity check
# but `toJson` would emit as a TOML integer array) must all fail.
must_fail_with scalar-values "must be a list" \
  '{"key":"kubernetes.io/arch","operator":"In","values":"amd64"}'
must_fail_with falsy-scalar "must be a list" \
  '{"key":"kubernetes.io/arch","operator":"In","values":0}'
must_fail_with numeric-elem-in "must be a string" \
  '{"key":"karpenter.k8s.aws/instance-generation","operator":"In","values":[7]}'
must_fail_with numeric-elem-gt "must be a string" \
  '{"key":"karpenter.k8s.aws/instance-generation","operator":"Gt","values":[0]}'

# (d): planted RED — an authored instance-local-nvme requirement must
# fail at render (the key is template-owned; an authored copy on a
# non-nvme class would conjunct `Gt "0"` ∧ `DoesNotExist`).
must_fail_with authored-nvme-key "template-owned" \
  '{"key":"karpenter.k8s.aws/instance-local-nvme","operator":"Gt","values":["0"]}'

# (e): planted REDs — per-operator arity mirrors validate_shape().
must_fail_with gt-two          "exactly one integer"   '{"key":"karpenter.k8s.aws/instance-generation","operator":"Gt","values":["5","6"]}'
must_fail_with gt-nonint       "exactly one integer"   '{"key":"karpenter.k8s.aws/instance-generation","operator":"Gt","values":["O"]}'
must_fail_with exists-nonempty "must have empty"       '{"key":"kubernetes.io/arch","operator":"Exists","values":["x"]}'
must_fail_with unknown-op      "unrecognized operator" '{"key":"kubernetes.io/arch","operator":"gt","values":["5"]}'

echo "hwclass nvme partition: $n classes ($n_nvme nvme / $n_ebs ebs); Gt⇔nvme / DoesNotExist⇔non-nvme bijection; type/arity/template-owned guards fire"
