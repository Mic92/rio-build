# ctrl.nodeclaim.backlog-floor end-to-end under KWOK fake-Karpenter.
#
# What unit tests CAN'T cover (and this does):
#   - GetSpawnIntentsResponse.pending_by_system round-trips scheduler
#     → controller over the real gRPC client (proto field 7 wired)
#   - helm `karpenter.nodeclaimPool.backlogFloorCapRatio` flows into
#     the controller ConfigMap's `[nodeclaim_pool]` TOML
#   - reap_idle's floor gate fires against real apiserver-round-tripped
#     NodeClaim status (Registered=True, allocatable populated)
#   - the warm_floor / reap_suppressed_total / backlog_pending gauges
#     are registered AND emitted on the controller /metrics surface
#
# Hourglass shape (`>-<`, derivations/hourglass.nix):
#   Phase A — `-A wide`: 8 zero-dep leaves → 8 SpawnIntents →
#             cover_deficit mints 8 NodeClaims (maxCores=4 = probe.cpu
#             so n_lo = ⌈8×4/4⌉ = 8). Wait Registered=True ×8.
#   Phase B — `-A tails`: neck (sleep 600) is the 1 ready intent,
#             8 tails Queued → pending_by_system=8 while only 1
#             SpawnIntent → FFD reserves 1 node. The other 7 are idle
#             (no backing Node objects → requested=0) and past the
#             ~65s na_threshold floor — reap_idle would delete 7
#             without the warm-floor. Assert it deletes 0.
#   Phase C — created_total grew by ≤1 vs Phase A (slack for the
#             kill→resubmit gap; 0 expected).
#
# Structural assertions only — no wall-clock gates on the property
# under test (the 80s dwell is the EXPOSURE window, not a measured
# latency).
{
  pkgs,
  common,
  fixture,
  # Per-wait registration budget. THREADED FROM default.nix so the
  # eval-time `2×leadTimeSeed > 2×regWaitSecs+10` dominance assertion
  # there fails BUILD (not flakes) when this is widened — the prior
  # comment-only re-derivation left a 10s margin one builder-tail can
  # eat.
  regWaitSecs,
}:
let
  inherit (fixture) ns;
  drvs = import ../lib/derivations.nix { inherit pkgs; };
in
pkgs.testers.runNixOSTest {
  name = "rio-backlog-floor";
  skipTypeCheck = true;
  globalTimeout = 900 + common.covTimeoutHeadroom;

  inherit (fixture) nodes;

  testScript = ''
    ${common.mkBootstrap {
      inherit fixture;
      withSeed = true;
    }}

    import json as _json
    import time

    # ── kwok-controller + kube-build-scheduler up ────────────────────
    k3s_server.wait_until_succeeds(
        "k3s kubectl -n kube-system rollout status deploy/kwok-controller "
        "--timeout=60s",
        timeout=120,
    )
    k3s_server.wait_until_succeeds(
        "k3s kubectl -n ${ns} rollout status deploy/kube-build-scheduler "
        "--timeout=60s",
        timeout=120,
    )
    # NodeClaim CRD established (k3s deploy-controller retries on
    # NotFound; this gates the rio-controller's first list).
    k3s_server.wait_until_succeeds(
        "k3s kubectl get crd nodeclaims.karpenter.sh", timeout=60
    )
    # KWOK's StagesManager runs RESTMapper discovery once on Stage-CR
    # add; the k3s deploy-controller starts kwok-controller alongside
    # the karpenter CRDs, so the karpenter.sh apigroup commonly isn't
    # served yet → `failed to get gvk for gvr` and the NodeClaim
    # resourceRef is dropped permanently. Restart now that the CRD is
    # established so discovery resolves.
    k3s_server.succeed(
        "k3s kubectl -n kube-system rollout restart deploy/kwok-controller"
    )
    k3s_server.wait_until_succeeds(
        "k3s kubectl -n kube-system rollout status deploy/kwok-controller "
        "--timeout=60s",
        timeout=90,
    )

    # ── canary gate: positive evidence the Stage machinery is live ───
    # The restart above is exposed to the same discovery race it works
    # around — a miss drops the NodeClaim resourceRef permanently
    # again and the Deployment reports Ready but never acts on a
    # NodeClaim. Waiting longer cannot recover that state, so before
    # the real test runs a throwaway canary NodeClaim must reach
    # Launched=True. The canary carries NO labels: the
    # rio-controller's owner-label watch and every label-selector
    # assertion below are blind to it.
    canary = _json.dumps({
        "apiVersion": "karpenter.sh/v1",
        "kind": "NodeClaim",
        "metadata": {"name": "canary-stage-liveness"},
        "spec": {
            "nodeClassRef": {
                "group": "karpenter.k8s.aws",
                "kind": "EC2NodeClass",
                "name": "rio-default",
            },
            "requirements": [
                {
                    "key": "kubernetes.io/os",
                    "operator": "In",
                    "values": ["linux"],
                },
            ],
            "resources": {
                "requests": {
                    "cpu": "1",
                    "memory": "1Gi",
                    "ephemeral-storage": "1Gi",
                },
            },
        },
    })

    def stage_machinery_live():
        try:
            k3s_server.succeed(
                "k3s kubectl apply -f - <<'EOF'\n" + canary + "\nEOF"
            )
            k3s_server.wait_until_succeeds(
                "k3s kubectl get nodeclaims canary-stage-liveness "
                "-o jsonpath='{.status.conditions[?(@.type==\"Launched\")].status}' "
                "| grep -q True",
                timeout=15,
            )
            return True
        except Exception:
            return False
        finally:
            # execute(), not succeed(): a non-zero exit here would
            # raise out of finally, supersede the except-block's
            # `return False`, and escape the retry loop uncaught.
            k3s_server.execute(
                "k3s kubectl delete nodeclaims canary-stage-liveness "
                "--ignore-not-found"
            )

    for attempt in range(1, 4):
        if stage_machinery_live():
            break
        print(f"=== canary NodeClaim not Launched (attempt {attempt}/3) ===")
        if attempt == 3:
            print(k3s_server.execute(
                "k3s kubectl get stages.kwok.x-k8s.io -o yaml; "
                "k3s kubectl -n kube-system logs deploy/kwok-controller --tail=200"
            )[1])
            raise Exception(
                "kwok Stage machinery dead after 3 kwok-controller "
                "restarts — not a flake; check the dumped Stage objects "
                "and kwok-controller log (resourceRef discovery, kwok "
                "image)"
            )
        k3s_server.succeed(
            "k3s kubectl -n kube-system rollout restart deploy/kwok-controller"
        )
        k3s_server.wait_until_succeeds(
            "k3s kubectl -n kube-system rollout status deploy/kwok-controller "
            "--timeout=60s",
            timeout=90,
        )
    # The in-loop finally uses execute() (best-effort — succeed() there
    # would supersede `return False` and escape the retry); re-assert
    # cleanup ONCE here with succeed() now that Stage liveness is
    # confirmed, so a leaked canary cannot be miscounted by an
    # unfiltered `kubectl get nodeclaims | wc -l` downstream.
    # --ignore-not-found: the in-loop delete almost always already
    # landed.
    k3s_server.succeed(
        "k3s kubectl delete nodeclaims canary-stage-liveness --ignore-not-found"
    )

    # ── controller-metrics scrape helpers ────────────────────────────
    # rio-controller is never restarted in this scenario; resolve the
    # pod name once instead of on every scrape (~14× including the
    # warm_floor poll — ~10-20s of avoidable kubectl round-trips under
    # full-gate TCG load inside a 900s globalTimeout).
    ctrl_pod = kubectl(
        "get pods -l app.kubernetes.io/name=rio-controller "
        "-o jsonpath='{.items[0].metadata.name}'"
    ).strip()

    def ctrl_metrics():
        raw = k3s_server.succeed(
            "k3s kubectl get --raw "
            f"'/api/v1/namespaces/${ns}/pods/{ctrl_pod}:9094/proxy/metrics'"
        )
        return parse_prometheus(raw)

    def series_sum(m, name, *needles):
        # Σ over every label-series of `name` whose raw `{...}` label
        # string contains every needle. Robust to label ordering.
        return sum(
            v for lbl, v in m.get(name, {}).items()
            if all(n in lbl for n in needles)
        )

    def dump_ctrl():
        print(k3s_server.execute(
            "k3s kubectl get nodeclaims -o wide; "
            "k3s kubectl -n ${ns} logs deploy/rio-controller --tail=200"
        )[1])

    # ── Phase A: wide fan-out → 8 NodeClaims Registered ──────────────
    # 8 zero-dep leaves → 8 SpawnIntents on tick 1. With
    # hwClasses.vmtest.maxCores=4 (= probe.cpu floor), sizing()'s
    # n_lo = ⌈8×4/4⌉ = 8, so cover_deficit mints exactly 8.
    client.succeed(
        "nix-build ${drvs.hourglass} -A wide "
        "--arg busybox '(builtins.storePath ${common.busybox})' "
        "--no-out-link --store 'ssh-ng://k3s-server:32222' "
        ">/tmp/wide.log 2>&1 & echo $! >/tmp/wide.pid"
    )
    try:
        k3s_server.wait_until_succeeds(
            "test $(k3s kubectl get nodeclaims "
            "-l rio.build/nodeclaim-pool=builder -o name | wc -l) -ge 8",
            timeout=${toString regWaitSecs},
        )
        # KWOK Stage: 2s Launched + 3s Registered. ${toString regWaitSecs}s
        # budget — composed-tree contention tail (forecast-provisioning
        # precedent).
        k3s_server.wait_until_succeeds(
            "test $(k3s kubectl get nodeclaims "
            "-l rio.build/nodeclaim-pool=builder "
            "-o jsonpath='{.items[*].status.conditions[?(@.type==\"Registered\")].status}' "
            "| tr ' ' '\\n' | grep -c True) -ge 8",
            timeout=${toString regWaitSecs},
        )
    except Exception:
        print("=== Phase A: <8 owned NodeClaims Registered ===")
        dump_ctrl()
        raise

    snap_a = ctrl_metrics()
    # created_* / Phase-C re-mint guard are cell-agnostic (sum over
    # vmtest:spot AND vmtest:od): cell_rank is deterministic Spot-first
    # today, but a `cell="vmtest:spot"` needle would be blind to an
    # od-side re-mint if cover_deficit ever routed there (spot
    # ICE-masked, or a cost-explore tweak) — the regression this
    # scenario guards would pass silently. The `vmtest` hwClass omits
    # capacityTypes → both cells are in all_cells().
    created_a = series_sum(
        snap_a, "rio_controller_nodeclaim_created_total"
    )
    reaped_idle_a = series_sum(
        snap_a, "rio_controller_nodeclaim_reaped_total", 'reason="idle"'
    )
    print(f"=== Phase A: created={created_a} reaped_idle={reaped_idle_a} ===")

    # ── Phase B: kill wide → submit tails (1 ready + 8 Queued) ───────
    # Tight kill→resubmit so the controller never observes a tick with
    # pending_by_system=0. The gap is sub-second vs a 10s controller
    # tick; if a tick DOES land in the gap with floor=0 it can reap
    # 0..8 (not 1), so a slack=1 on the Phase-B reaped_idle assert
    # would not discriminate gap from regression. The "slack=1" below
    # is on Phase C's created_total delta, not this assert.
    # TODO: structural close — submit tails BEFORE killing wide so
    # pending_by_system never drops below 8 across the transition.
    client.succeed(
        "kill $(cat /tmp/wide.pid) 2>/dev/null || true; "
        "nix-build ${drvs.hourglass} -A tails "
        "--arg busybox '(builtins.storePath ${common.busybox})' "
        "--no-out-link --store 'ssh-ng://k3s-server:32222' "
        ">/tmp/tails.log 2>&1 & echo $! >/tmp/tails.pid"
    )

    # Poll warm_floor ≥ 8 — NOT wait_until_succeeds (that takes shell;
    # this needs the parsed-metrics helper). 12×5s = 60s budget.
    # Cell-agnostic (Σ over vmtest:spot + vmtest:od) like the
    # surrounding created/reaped/suppressed asserts: warm_floor[cell] =
    # min(raw_pending, ⌈remaining[cell]×ratio⌉), so if cover_deficit
    # routed to od (spot ICE-masked or a cost-explore tweak)
    # warm_floor[spot]=0 while warm_floor[od]=8 and a spot-pinned poll
    # times out with the misleading "not flowing" message even though
    # the floor is working.
    for _ in range(12):
        if series_sum(
            ctrl_metrics(), "rio_controller_nodeclaim_warm_floor"
        ) >= 8:
            break
        time.sleep(5)
    else:
        dump_ctrl()
        raise Exception(
            "Σ warm_floor never reached 8 — pending_by_system not "
            "flowing or backlogFloorCapRatio not loaded; check "
            "controller log + ConfigMap"
        )

    # 8 controller ticks (10s each) of exposure. na_threshold =
    # max(boot_median/2, minConsolidationTime=30) where boot_median is
    # the DDSketch p50 over N_SEED=10 copies of leadTimeSeed=130 plus
    # ~8 real ~5s KWOK boots → ≈130, so the threshold floor is ≈65s
    # (NOT 30s — the seed dominates the median until >10 real boots).
    # prev_idle was seeded at the first registered-idle observation
    # (Phase A); 80s here puts idle comfortably past 65.
    time.sleep(80)

    snap_b = ctrl_metrics()
    with subtest("Phase B: warm_floor holds 8, zero idle-reap, suppressed>0"):
        floor_b = series_sum(
            snap_b, "rio_controller_nodeclaim_warm_floor"
        )
        reaped_idle_b = series_sum(
            snap_b, "rio_controller_nodeclaim_reaped_total", 'reason="idle"'
        )
        suppressed_b = series_sum(
            snap_b,
            "rio_controller_nodeclaim_reap_suppressed_total",
            'reason="backlog_floor"',
        )
        # Cell-agnostic max (NOT spot-pinned): if cover_deficit ever
        # routes Phase-A to vmtest:od (the l.249/l.289 ICE-mask /
        # cost-explore guard), a spot needle would read the Phase-0
        # zero-write and the failure message would print
        # `threshold=0.0` — misdirecting triage toward "leadTimeSeed
        # not loaded" when the real od-side threshold is fine.
        threshold_b = max(
            (v for lbl, v in snap_b.get(
                "rio_controller_nodeclaim_consolidate_threshold_seconds", {}
            ).items() if 'cell="vmtest:' in lbl),
            default=0.0,
        )
        nclaims = int(k3s_server.succeed(
            "k3s kubectl get nodeclaims "
            "-l rio.build/nodeclaim-pool=builder -o name | wc -l"
        ).strip())
        print(
            f"=== Phase B: floor={floor_b} reaped_idle={reaped_idle_b} "
            f"suppressed={suppressed_b} threshold={threshold_b} "
            f"nodeclaims={nclaims} ==="
        )
        # try/except → dump_ctrl() on every Phase-B regression assert,
        # not just the suppressed_b==0 flake-signature path: a
        # floor-gate regression (the assert whose own message says
        # "the regression this scenario guards") needs the controller
        # log tail + NodeClaim listing in `nix log <drv>` so triage
        # does not have to reproduce interactively.
        try:
            assert floor_b >= 8, (
                f"warm_floor={floor_b}, expected >=8 — floor decayed "
                "during exposure (pending_by_system stale or capRatio "
                "not 1.0)"
            )
            assert reaped_idle_b - reaped_idle_a == 0, (
                f"reaped_total{{reason=idle}} delta="
                f"{reaped_idle_b - reaped_idle_a}, expected 0 — floor "
                "wired but not honoured (the regression this scenario "
                "guards)"
            )
            assert suppressed_b > 0, (
                f"reap_suppressed_total={suppressed_b}, expected >0 — "
                "nodes never became reap-candidates "
                f"(idle<=threshold={threshold_b}; leadTimeSeed=130 → "
                "seed-dominated boot_median → ~65s floor; if "
                "threshold≈60 and this fired, the 80s exposure was "
                "eaten by builder-load skew — widen, do not lower "
                "the seed)"
            )
            assert nclaims >= 8, f"live NodeClaims={nclaims}, expected >=8"
        except Exception:
            dump_ctrl()
            raise

    # ── Phase C: cover_deficit did not re-mint ───────────────────────
    with subtest("Phase C: created_total grew by <=1 vs Phase A"):
        created_c = series_sum(
            snap_b, "rio_controller_nodeclaim_created_total"
        )
        delta = created_c - created_a
        assert delta <= 1, (
            f"created_total delta={delta} (A={created_a} C={created_c}), "
            "expected <=1 — floor failed to hold and cover_deficit "
            "re-minted for the tails fan-out"
        )

    # Teardown (best-effort, NOT exception-safe): the tails build (neck
    # sleeps 600s) holds a live ssh-ng session and a 9-node DAG through
    # collectCoverage — the open-stream-during-graceful-shutdown
    # profraw-loss hazard common.nix documents. wide.pid was killed at
    # Phase B; tails.pid was written but never consumed. Straight-line,
    # not try/finally: a Phase B/C assertion failure or the warm_floor
    # poll's `else: raise` skips this and leaks the session into VM
    # shutdown — accepted (the failure path's diagnostic value
    # outweighs the coverage-mode profraw loss; non-coverage VM
    # shutdown reaps it anyway).
    client.succeed("kill $(cat /tmp/tails.pid) 2>/dev/null || true")

    ${common.collectCoverage fixture.pyNodeVars}
  '';
}
