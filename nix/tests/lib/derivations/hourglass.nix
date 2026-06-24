# `>-<` hourglass DAG for vm-backlog-floor-kwok.
#
# Two independently-buildable attrs:
#   wide  — 8 zero-dep leaves (Phase A: all 8 ready → 8 SpawnIntents
#           → cover_deficit mints ≥8 NodeClaims with maxCores=4)
#   tails — 8 leaves all depending on `neck` (Phase B: 1 ready + 8
#           Queued → pending_by_system≥8 while only 1 SpawnIntent —
#           the DAG-depth bottleneck the warm-floor must hold through)
#
# `wide` and `neck` share no edges: the hourglass is a SCHEDULING
# shape (wide → bottleneck+backlog), not a build-graph edge. Under
# vm-backlog-floor-kwok the KWOK NodeClaims have NO backing
# Node/kubelet/builder, so the `wide` builds never actually run —
# Phase B's `kill $(cat /tmp/wide.pid)` (ssh-ng disconnect → DAG GC)
# is the load-bearing mechanism that clears the 8 leaves. `neck` sleeps
# 600s so the 8 tails stay Queued (pending_by_system=8, only 1
# SpawnIntent) through the whole exposure window; with an instant
# neck the 8 tails go Ready → 8 SpawnIntents → FFD reserves all 8
# nodes → none ever reach the floor-suppression gate.
{
  busybox,
  marker ? "hg",
}:
let
  inherit (import ./_busybox.nix { inherit busybox; }) bb mkDrv;
  mkStep = name: deps: mkDrv name "echo ${toString deps} >$out" { };
  neck = mkDrv "rio-${marker}-neck" "${bb} sleep 600; echo neck >$out" { };
  tails = builtins.genList (i: mkStep "rio-${marker}-tail-${toString i}" [ neck ]) 8;
  wide = builtins.genList (i: mkStep "rio-${marker}-wide-${toString i}" [ ]) 8;
in
{
  inherit tails wide;
}
