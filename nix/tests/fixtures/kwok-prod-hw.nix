# CHECKED MIRROR of `.scheduler.sla.hwClasses | keys` from
# infra/helm/rio-build/values.yaml. NOT hand-maintained without
# verification: the kwok-prodHw-drift check (nix/misc-checks.nix)
# diffs this list against `yq -r '... | keys[]' | sort` over the
# chart values and fails CI on mismatch.
#
# Why a checked-mirror literal instead of deriving at eval time:
# evaluating the YAML key-set requires IFD (builtins.readFile over a
# yq runCommand), which blocks vm-{sla-sizing,backlog-floor}-kwok eval
# behind a build under nix-eval-jobs — against the repo's no-IFD
# posture. The previous IFD form (9dd33a8c) was correct (deterministic
# over a checked-in input) but made the two KWOK checks the only
# checks.* entries whose eval depended on a build.
#
# Keep sorted (LC_ALL=C) — the drift check sorts the yq side.
[
  "fetcher-arm"
  "fetcher-x86"
  "hi-ebs-arm"
  "hi-ebs-arm-g7"
  "hi-ebs-x86"
  "hi-ebs-x86-g7"
  "hi-nvme-arm"
  "hi-nvme-arm-g7"
  "hi-nvme-x86"
  "hi-nvme-x86-g7"
  "lo-ebs-arm"
  "lo-ebs-x86"
  "lo-nvme-arm"
  "lo-nvme-x86"
  "metal-arm"
  "metal-x86"
  "mid-ebs-arm"
  "mid-ebs-x86"
  "mid-nvme-arm"
  "mid-nvme-x86"
]
