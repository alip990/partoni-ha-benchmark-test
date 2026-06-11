#!/usr/bin/env python3
"""
run_scenarios.py — run all HA benchmark scenarios via the Locust REST API
and generate a self-contained HTML report + raw JSON results.

Usage:
    python run_scenarios.py [--output reports/benchmark.html] [--scenarios id ...]

Hardening (v2):
  * Health gate before EVERY scenario: 1 leader + 2 streaming replicas
    (Patroni REST :8008/cluster) AND a real write through PgPool.
  * Kill targets resolved at runtime: {leader} / {replica} placeholders.
    leader_crash first moves the leader OFF the proxy node (the node whose
    PgPool the benchmark connects to) so we measure DB failover, not proxy death.
  * Timeline = per-interval deltas of num_requests/num_failures (not Locust's
    smoothed current_rps), so outage windows line up with wall-clock.
  * RTO = first failing/stalled sample after injection → first of 2 consecutive
    clean samples. Reported as "not recovered" if the window never closes.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

import psycopg2
import requests

import config

# ── scenario definitions ─────────────────────────────────────────────────────
# inject.command / inject.recovery may contain {leader} / {replica} / {target}:
#   {leader}  → current Patroni leader at injection time
#   {replica} → a streaming replica that is NOT the proxy node
#   {target}  → whatever the command killed (for recovery)

SCENARIOS = [
    {
        "id":          "baseline_mixed",
        "name":        "Baseline — Mixed Load",
        "description": "Balanced 5:1 read/write, no failures. Establishes TPS baseline.",
        "user_class":  "MixedUser",
        "users":       10,
        "spawn_rate":  2,
        "duration":    90,
        "inject":      None,
    },
    {
        "id":          "baseline_read_heavy",
        "name":        "Baseline — Read Heavy",
        "description": "20:1 read/write with 8-table JOIN. Tests standby read scalability.",
        "user_class":  "ReadHeavyUser",
        "users":       20,
        "spawn_rate":  4,
        "duration":    90,
        "inject":      None,
    },
    {
        "id":          "baseline_write_heavy",
        "name":        "Baseline — Write Heavy",
        "description": "5:1 write/read CTE chains. Drives the primary's write path.",
        "user_class":  "WriteHeavyUser",
        "users":       10,
        "spawn_rate":  2,
        "duration":    90,
        "inject":      None,
    },
    {
        "id":          "failover_leader_crash",
        "name":        "HA — Leader Crash (A1)",
        "description": "SIGKILL the current Patroni leader under probe load. "
                       "Leader is moved off the proxy node first, so this measures "
                       "pure DB failover (etcd TTL expiry → election → promote → PgPool reroute).",
        "user_class":  "FailoverProbe",
        "users":       1,
        "spawn_rate":  1,
        "duration":    150,
        "pre":         "leader_off_proxy",
        "inject": {
            "name":          "leader-crash-inject",        # env: LEADER_CRASH_INJECT_CMD
            "delay":         30,
            "command":       config.LEADER_CRASH_INJECT_CMD,
            "recovery_name": "leader-crash-recovery",      # env: LEADER_CRASH_RECOVERY_CMD
            "recovery":      config.LEADER_CRASH_RECOVERY_CMD,
            "recovery_delay": 90,
            "stable_name":   "leader-crash-stable-check",  # env: LEADER_CRASH_STABLE_CMD
            "stable_cmd":    config.LEADER_CRASH_STABLE_CMD,
        },
    },
    {
        "id":          "failover_planned_switchover",
        "name":        "HA — Planned Switchover (A6)",
        "description": "patronictl switchover (graceful, coordinated demote+promote). "
                       "The HA best case: expects RTO of a few seconds, zero data loss.",
        "user_class":  "FailoverProbe",
        "users":       1,
        "spawn_rate":  1,
        "duration":    120,
        "inject": {
            "name":          "switchover-inject",          # env: SWITCHOVER_INJECT_CMD
            "delay":         45,
            "command":       config.SWITCHOVER_INJECT_CMD,
            "recovery_name": "switchover-recovery",        # env: SWITCHOVER_RECOVERY_CMD
            "recovery":      config.SWITCHOVER_RECOVERY_CMD or None,
            "stable_name":   "switchover-stable-check",    # env: SWITCHOVER_STABLE_CMD
            "stable_cmd":    config.SWITCHOVER_STABLE_CMD,
        },
    },
    {
        "id":          "failover_node_poweroff",
        "name":        "HA — Replica Power-Off (A2)",
        "description": "SIGKILL a streaming replica (not the leader, not the proxy node). "
                       "Writes should be unaffected; PgPool detaches the dead reader.",
        "user_class":  "FailoverProbe",
        "users":       1,
        "spawn_rate":  1,
        "duration":    150,
        "inject": {
            "name":          "replica-poweroff-inject",    # env: REPLICA_POWEROFF_INJECT_CMD
            "delay":         30,
            "command":       config.REPLICA_POWEROFF_INJECT_CMD,
            "recovery_name": "replica-poweroff-recovery",  # env: REPLICA_POWEROFF_RECOVERY_CMD
            "recovery":      config.REPLICA_POWEROFF_RECOVERY_CMD,
            "recovery_delay": 90,
            "stable_name":   "replica-poweroff-stable-check",  # env: REPLICA_POWEROFF_STABLE_CMD
            "stable_cmd":    config.REPLICA_POWEROFF_STABLE_CMD,
        },
    },
]

# ── cluster helpers (Patroni REST + PgPool write check) ──────────────────────

PROXY_NODE = next((n for n, ip in config.LAB_NODES.items() if ip == config.PG_HOST), None)


def get_members():
    """Cluster members from the first reachable Patroni REST API."""
    for ip in config.LAB_NODES.values():
        try:
            r = requests.get(f"http://{ip}:{config.PATRONI_REST_PORT}/cluster", timeout=3)
            if r.status_code == 200:
                return r.json().get("members", [])
        except requests.RequestException:
            continue
    return []


def current_leader(members=None):
    members = members if members is not None else get_members()
    for m in members:
        if m.get("role") == "leader":
            return m.get("name")
    return None


def pick_replica(members=None, avoid=()):
    """A running/streaming replica, preferring one that is not the proxy node."""
    members = members if members is not None else get_members()
    reps = [m["name"] for m in members
            if m.get("role") in ("replica", "sync_standby")
            and m.get("state") in ("running", "streaming")
            and m["name"] not in avoid]
    non_proxy = [r for r in reps if r != PROXY_NODE]
    return (non_proxy or reps or [None])[0]


def write_check():
    """Prove the full write path: client → PgPool → current primary."""
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_WRITE_PORT,
        dbname=config.PG_DATABASE, user=config.PG_USER,
        password=config.PG_PASSWORD, connect_timeout=3,
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS ha_gate(id int PRIMARY KEY, ts timestamptz)')
        cur.execute("INSERT INTO ha_gate VALUES (1, now()) "
                    "ON CONFLICT (id) DO UPDATE SET ts = now()")
        cur.execute("SELECT 1")
        cur.fetchone()
    finally:
        conn.close()


def wait_cluster_healthy(timeout=300, label=""):
    """Block until: 1 leader, >=2 streaming replicas, write via PgPool succeeds."""
    print(f"  [gate{' ' + label if label else ''}] waiting for healthy cluster "
          f"(leader + 2 streaming replicas + write via PgPool) …")
    deadline = time.time() + timeout
    last_err = "no status yet"
    while time.time() < deadline:
        members = get_members()
        leader = current_leader(members)
        streaming = [m["name"] for m in members
                     if m.get("role") in ("replica", "sync_standby")
                     and m.get("state") in ("running", "streaming")]
        if leader and len(streaming) >= 2:
            try:
                write_check()
                print(f"  [gate] healthy: leader={leader} replicas={streaming}")
                return True
            except Exception as exc:
                last_err = f"write check: {exc}"
        else:
            last_err = f"members={[(m.get('name'), m.get('role'), m.get('state')) for m in members]}"
        time.sleep(3)
    print(f"  [gate] TIMEOUT after {timeout}s — last status: {last_err}")
    return False


def cluster_snapshot():
    """Compact cluster state: leader, max timeline, member roles — recorded
    before/after every scenario so the report shows exactly what moved."""
    members = get_members()
    return {
        "leader":   current_leader(members),
        "timeline": max((m.get("timeline") or 0) for m in members) if members else None,
        "members":  [f"{m.get('name')}={m.get('role')}/{m.get('state')}" for m in members],
    }


def max_replica_lag(members=None):
    """Max streaming lag (bytes) across replicas — 0 when fully caught up."""
    members = members if members is not None else get_members()
    lags = [m.get("lag") or 0 for m in members
            if m.get("role") in ("replica", "sync_standby") and isinstance(m.get("lag"), int)]
    return max(lags) if lags else 0


def _pg_query(sql):
    """One-shot query through the PgPool write port; returns list of tuples."""
    conn = psycopg2.connect(
        host=config.PG_HOST, port=config.PG_WRITE_PORT,
        dbname=config.PG_DATABASE, user=config.PG_USER,
        password=config.PG_PASSWORD, connect_timeout=5,
    )
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def _shell_capture(cmd, timeout=20):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception as exc:
        return f"<capture failed: {exc}>"


# every $ is escaped so the LOCAL shell passes it through to the node's shell
_SPECS_REMOTE = ("nproc; "
                 "awk -F': ' '/model name/{print \\$2; exit}' /proc/cpuinfo; "
                 "free -m | awk '/^Mem:/{print \\$2}'; "
                 "uname -r; "
                 ". /etc/os-release; echo \\$PRETTY_NAME")


def _node_specs(shell_prefix):
    # the \$ survives the local shell in both cases; ssh/sh expands it remotely
    out = _shell_capture(f'{shell_prefix} "{_SPECS_REMOTE}"' if shell_prefix
                         else f'sh -c "{_SPECS_REMOTE}"')
    lines = (out.splitlines() + [""] * 5)[:5]
    return {"cpu_cores": lines[0], "cpu_model": lines[1].strip(),
            "ram_mb": lines[2], "kernel": lines[3], "os": lines[4]}


def capture_environment():
    """Snapshot everything the numbers depend on: PostgreSQL version + non-default
    settings (via SQL), Patroni dynamic config, PgPool failure-detection params,
    and CPU/RAM/OS of every node + the load-generator host."""
    print("Capturing environment (PostgreSQL config, node specs) …")
    env = {"captured_at": datetime.now().isoformat(timespec="seconds")}

    try:
        env["postgres_version"] = _pg_query("SELECT version()")[0][0]
        env["database_size"] = _pg_query(
            f"SELECT pg_size_pretty(pg_database_size('{config.PG_DATABASE}'))")[0][0]
        env["pg_settings"] = [
            {"name": n, "setting": s, "unit": u or "", "source": src}
            for n, s, u, src in _pg_query(
                "SELECT name, setting, unit, source FROM pg_settings "
                "WHERE source NOT IN ('default') ORDER BY name")]
    except Exception as exc:
        env["pg_settings_error"] = str(exc)

    env["patroni_dynamic_config"] = _shell_capture(f"{config.PATRONICTL} show-config")
    proxy = PROXY_NODE or next(iter(config.LAB_NODES))
    env["pgpool_params"] = _shell_capture(
        config.NODE_SHELL.format(node=proxy) +
        " \"grep -E '^(sr_check_period|health_check_period|health_check_timeout"
        "|health_check_max_retries|failover_on_backend_error|auto_failback"
        "|search_primary_node_timeout|load_balance_mode|num_init_children|max_pool)'"
        " /etc/pgpool2/pgpool.conf\"")

    env["nodes"] = {}
    for name in config.LAB_NODES:
        env["nodes"][name] = _node_specs(config.NODE_SHELL.format(node=name))
    env["nodes"]["host (load generator)"] = _node_specs("")
    env["cluster"] = cluster_snapshot()
    return env


def pre_leader_off_proxy():
    """If the leader sits on the proxy node (whose PgPool we connect to),
    switch it away first — otherwise killing the leader also kills the proxy
    and the scenario measures proxy death instead of DB failover."""
    leader = current_leader()
    if leader != PROXY_NODE:
        print(f"  [pre] leader={leader}, proxy={PROXY_NODE} — no repositioning needed")
        return
    candidate = pick_replica(avoid=(leader,))
    print(f"  [pre] leader is on proxy node {PROXY_NODE} — switching over to {candidate}")
    _run_command(f"{config.PATRONICTL} switchover pgcluster "
                 f"--leader {leader} --candidate {candidate} --force",
                 name="pre-leader-off-proxy")
    wait_cluster_healthy(timeout=180, label="post-switchover")


PRE_STEPS = {"leader_off_proxy": pre_leader_off_proxy}

# ── Locust API helpers ────────────────────────────────────────────────────────

BASE_URL = f"http://127.0.0.1:{config.LOCUST_WEB_PORT}"


def _api(method, path, **kwargs):
    url = f"{BASE_URL}{path}"
    try:
        return getattr(requests, method)(url, timeout=10, **kwargs)
    except requests.RequestException as exc:
        print(f"  [API] {method.upper()} {path} → {exc}")
        return None


def _wait_locust_ready(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = _api("get", "/stats/requests")
        if r and r.status_code == 200:
            return True
        time.sleep(1)
    return False


def _swarm(user_class, users, spawn_rate):
    return _api("post", "/swarm", data={
        "user_count":   users,
        "spawn_rate":   spawn_rate,
        "user_classes": user_class,
        "host":         f"postgres://{config.PG_HOST}:{config.PG_WRITE_PORT}",
    })


def _stop_and_wait(timeout=20):
    _api("get", "/stop")
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = _get_stats()
        if snap.get("state") in ("stopped", "ready"):
            return True
        time.sleep(1)
    return False


def _reset():
    return _api("get", "/stats/reset")


def _get_stats():
    r = _api("get", "/stats/requests")
    if r and r.status_code == 200:
        return r.json()
    return {}


def _aggregated(snap):
    for s in snap.get("stats", []):
        if s.get("name") == "Aggregated":
            return s
    return None


def _verify_spawned(users, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = _get_stats()
        if snap.get("user_count", 0) >= users and snap.get("state") in ("spawning", "running"):
            return True
        time.sleep(1)
    return False


def _run_command(cmd, name="cmd", block=True):
    if not cmd:
        return
    print(f"  [{name}] {cmd}")
    if block:
        subprocess.run(cmd, shell=True, capture_output=True)
    else:
        # Inject/recovery during a measurement window must not block the
        # stats-polling loop (an ssh switchover takes seconds) — fire and forget.
        subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def wait_custom_stable(cmd, name, timeout=180, interval=5):
    """Retry a scenario-specific stability-check command until it exits 0."""
    print(f"  [{name}] polling until exit 0: {cmd}")
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(cmd, shell=True, capture_output=True)
        if r.returncode == 0:
            print(f"  [{name}] passed")
            return True
        time.sleep(interval)
    print(f"  [{name}] TIMEOUT after {timeout}s")
    return False

# ── scenario runner ───────────────────────────────────────────────────────────

def _resolve_inject(inject):
    """Resolve {leader}/{replica}/{target} placeholders against live cluster state."""
    if not inject:
        return None
    members = get_members()
    leader = current_leader(members)
    replica = pick_replica(members, avoid=(leader,))
    resolved = dict(inject)
    target = None
    cmd = inject["command"]
    if "{leader}" in cmd:
        target = leader
    elif "{replica}" in cmd:
        target = replica
    resolved["command"] = cmd.format(leader=leader, replica=replica)
    resolved["target"] = target
    for key in ("recovery", "stable_cmd"):
        if resolved.get(key):
            resolved[key] = resolved[key].format(
                leader=leader, replica=replica, target=target or "")
    resolved["label"] = f"{inject['name']}: {resolved['command']}"
    return resolved


def run_scenario(scenario: dict) -> dict:
    print(f"\n{'='*60}")
    print(f"  Scenario: {scenario['name']}")
    print(f"  Class: {scenario['user_class']}  users={scenario['users']}  dur={scenario['duration']}s")
    print(f"{'='*60}")

    # 1. health gate — never start a scenario against a broken cluster
    if not wait_cluster_healthy(timeout=300):
        print("  [SKIP] cluster did not become healthy — scenario skipped")
        return {"scenario": scenario, "timeline": [], "summary": {}, "skipped": True}

    # 2. optional pre-step (e.g. move leader off the proxy node)
    if scenario.get("pre"):
        PRE_STEPS[scenario["pre"]]()

    # 3. resolve dynamic kill targets against the live cluster
    inject = _resolve_inject(scenario.get("inject"))
    if inject:
        print(f"  [plan] inject at t={inject['delay']}s: {inject['command']}")

    cluster_before = cluster_snapshot()

    # 4. fresh stats, then swarm
    _reset()
    time.sleep(1)
    resp = _swarm(scenario["user_class"], scenario["users"], scenario["spawn_rate"])
    if not resp or resp.status_code not in (200, 201):
        print(f"  [WARN] swarm start returned {resp} — retrying once")
        time.sleep(2)
        _swarm(scenario["user_class"], scenario["users"], scenario["spawn_rate"])
    if not _verify_spawned(scenario["users"]):
        print("  [WARN] users did not spawn within 15s — results may be empty")

    started_at = datetime.now().isoformat(timespec="seconds")
    start_ts = time.time()
    injected = False
    recovery_done = False
    inject_t = None
    timeline = []
    prev = None  # (t, num_requests, num_failures)

    # 5. poll every 2s; timeline points are per-interval deltas
    while time.time() - start_ts < scenario["duration"]:
        elapsed = time.time() - start_ts

        if inject and not injected and elapsed >= inject["delay"]:
            print(f"  [t={elapsed:.0f}s] injecting failure")
            _run_command(inject["command"], name=inject["name"], block=False)
            injected = True
            inject_t = elapsed

        if inject and injected and not recovery_done and inject.get("recovery"):
            if elapsed >= inject.get("recovery_delay", scenario["duration"] - 20):
                print(f"  [t={elapsed:.0f}s] running recovery")
                _run_command(inject["recovery"], name=inject.get("recovery_name", "recovery"),
                             block=False)
                recovery_done = True

        snap = _get_stats()
        agg = _aggregated(snap)
        if agg:
            req, fail = agg.get("num_requests", 0), agg.get("num_failures", 0)
            pcts = snap.get("current_response_time_percentiles", {}) or {}
            try:
                lag = max_replica_lag()
            except Exception:
                lag = None
            elapsed = time.time() - start_ts   # re-stamp: the poll, not the loop top
            if prev is not None:
                dt = max(elapsed - prev[0], 1e-6)
                timeline.append({
                    "t":        round(elapsed, 1),
                    "rps":      round((req - prev[1]) / dt, 1),
                    "fail_rps": round((fail - prev[2]) / dt, 1),
                    "p50":      pcts.get("response_time_percentile_0.5")
                                or agg.get("median_response_time") or 0,
                    "p95":      pcts.get("response_time_percentile_0.95")
                                or agg.get("response_time_percentile_0.95") or 0,
                    "lag":      lag,
                })
            prev = (elapsed, req, fail)
        time.sleep(2)

    _stop_and_wait()
    ended_at = datetime.now().isoformat(timespec="seconds")

    # 6. final cumulative stats — aggregated + per-operation + error signatures
    final = _get_stats()
    agg = _aggregated(final)
    operations = [
        {"op": s.get("name"), "requests": s.get("num_requests", 0),
         "failures": s.get("num_failures", 0),
         "avg_ms": round(s.get("avg_response_time", 0), 1),
         "p50_ms": s.get("median_response_time", 0),
         "p95_ms": s.get("response_time_percentile_0.95", 0),
         "p99_ms": s.get("response_time_percentile_0.99", 0),
         "max_ms": round(s.get("max_response_time", 0), 1)}
        for s in final.get("stats", []) if s.get("name") != "Aggregated"
    ]
    errors = [
        {"op": e.get("name"), "error": e.get("error"),
         "occurrences": e.get("occurrences", 0)}
        for e in final.get("errors", [])
    ]
    result = {"scenario": {k: v for k, v in scenario.items() if k != "inject"},
              "inject_resolved": inject,
              "started_at": started_at,
              "ended_at": ended_at,
              "cluster_before": cluster_before,
              "operations": operations,
              "errors": errors,
              "timeline": timeline}

    if agg:
        duration = scenario["duration"]
        result["summary"] = {
            "total_requests": agg.get("num_requests", 0),
            "failures":       agg.get("num_failures", 0),
            "avg_ms":         round(agg.get("avg_response_time", 0), 1),
            "p50_ms":         agg.get("median_response_time", 0),
            "p95_ms":         agg.get("response_time_percentile_0.95", 0),
            "p99_ms":         agg.get("response_time_percentile_0.99", 0),
            "rps":            round(agg.get("num_requests", 0) / duration, 1),
            "fail_pct":       round(100 * agg.get("num_failures", 0) /
                                    max(agg.get("num_requests", 1), 1), 2),
        }
        if inject and inject_t is not None and timeline:
            rto, detect_lag, recovered = _estimate_rto(timeline, inject_t)
            result["summary"]["rto_s"] = rto
            result["summary"]["detect_lag_s"] = detect_lag
            result["summary"]["recovered"] = recovered
    else:
        result["summary"] = {}

    # 7. post-scenario stability check: built-in gate + optional scenario command.
    #    Measures how long the cluster needs to return to FULL health
    #    (all members back, write path proven) after the injected failure.
    if inject:
        t0 = time.time()
        stable = wait_cluster_healthy(timeout=300, label=f"stability:{scenario['id']}")
        if stable and inject.get("stable_cmd"):
            stable = wait_custom_stable(inject["stable_cmd"],
                                        inject.get("stable_name", "stable-check"))
        result["summary"]["cluster_stable"] = stable
        result["summary"]["stabilize_s"] = round(time.time() - t0, 1)
        print(f"  [stability] cluster {'STABLE' if stable else 'NOT STABLE'} "
              f"after {result['summary']['stabilize_s']}s")

    result["cluster_after"] = cluster_snapshot()
    moved = (cluster_before.get("leader"), result["cluster_after"].get("leader"))
    if moved[0] != moved[1]:
        print(f"  [cluster] leader moved: {moved[0]} → {moved[1]} "
              f"(timeline {cluster_before.get('timeline')} → "
              f"{result['cluster_after'].get('timeline')})")

    return result


def _estimate_rto(timeline, inject_t):
    """Outage = first sample after injection with failures or zero throughput,
    until the first of >=2 consecutive clean samples. Returns (rto_s,
    detection_lag_s, recovered)."""
    # epsilon guards the rounded sample t (e.g. 46.0) vs raw inject_t (46.04)
    post = [pt for pt in timeline if pt["t"] >= inject_t - 0.25]
    outage_start = None
    for pt in post:
        if pt["fail_rps"] > 0 or pt["rps"] == 0:
            outage_start = pt["t"]
            break
    if outage_start is None:
        return 0, None, True       # injection caused no observable outage

    after = [pt for pt in post if pt["t"] > outage_start]
    for i, pt in enumerate(after):
        clean = pt["rps"] > 0 and pt["fail_rps"] == 0
        nxt = after[i + 1] if i + 1 < len(after) else None
        if clean and (nxt is None or (nxt["rps"] > 0 and nxt["fail_rps"] == 0)):
            return round(pt["t"] - outage_start, 1), round(outage_start - inject_t, 1), True
    last_t = post[-1]["t"] if post else outage_start
    return round(last_t - outage_start, 1), round(outage_start - inject_t, 1), False

# ── report generation ─────────────────────────────────────────────────────────

_HTML_TMPL = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>HA Benchmark Report — {generated}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {{ --bg:#0f1117; --panel:#1a1d27; --accent:#6c63ff; --green:#00d085; --red:#ff4757; --text:#e2e8f0; --muted:#94a3b8; }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ background:var(--bg); color:var(--text); font-family:'Segoe UI',system-ui,sans-serif; padding:2rem; }}
  h1 {{ font-size:1.8rem; margin-bottom:.25rem; }}
  .sub {{ color:var(--muted); font-size:.9rem; margin-bottom:2rem; }}
  table {{ width:100%; border-collapse:collapse; margin-top:1rem; }}
  th {{ background:#2d3348; padding:.65rem 1rem; text-align:left; font-size:.8rem; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; }}
  td {{ padding:.6rem 1rem; border-bottom:1px solid #2d3348; font-size:.875rem; }}
  tr:hover td {{ background:#20243a; }}
  .ok  {{ color:var(--green); }}
  .bad {{ color:var(--red);   }}
  .warn {{ color:#f6c90e; }}
  .chart-wrap {{ background:var(--panel); border-radius:12px; padding:1.25rem; border:1px solid #2d3348; margin-bottom:1.5rem; }}
  .chart-wrap h2 {{ font-size:1rem; color:var(--accent); margin-bottom:.35rem; }}
  .chart-wrap p {{ font-size:.8rem; color:var(--muted); margin-bottom:1rem; }}
  .section {{ margin-bottom:2.5rem; }}
  .tag {{ display:inline-block; padding:.15rem .5rem; border-radius:4px; font-size:.75rem; margin-left:.5rem; }}
  .tag-ha {{ background:#4c1d95; color:#c4b5fd; }}
  .tag-base {{ background:#1e3a2e; color:#6ee7b7; }}
</style>
</head>
<body>
<h1>PostgreSQL HA Benchmark Report</h1>
<p class="sub">Generated {generated} &nbsp;|&nbsp; Run window: {run_window} &nbsp;|&nbsp; Entry point: PgPool {pg_host}:{pg_write_port} &nbsp;|&nbsp; Scenarios: {num_scenarios}</p>

{environment_section}

<div class="section">
<h2 style="margin-bottom:1rem;font-size:1.1rem;">Summary</h2>
<table>
<thead><tr>
  <th>Scenario</th><th>Users</th><th>Requests</th><th>Fail%</th>
  <th>Avg ms</th><th>P50 ms</th><th>P95 ms</th><th>P99 ms</th><th>RPS</th>
  <th>RTO</th><th>Recovered</th><th>Cluster stable</th>
</tr></thead>
<tbody>
{summary_rows}
</tbody>
</table>
<p style="color:var(--muted);font-size:.8rem;margin-top:.5rem;">
RTO = duration of the observed outage window (first failing/stalled 2&nbsp;s sample after
injection → first of two consecutive clean samples). Timeline charts plot per-interval
deltas, not cumulative averages.</p>
</div>

<div class="section">
<h2 style="margin-bottom:1rem;font-size:1.1rem;">Throughput Over Time (RPS, 2s deltas)</h2>
{rps_charts}
</div>

<div class="section">
<h2 style="margin-bottom:1rem;font-size:1.1rem;">Latency Over Time (P50 / P95 ms)</h2>
{latency_charts}
</div>

<div class="section">
<h2 style="margin-bottom:1rem;font-size:1.1rem;">Replication Lag Over Time (MB behind primary)</h2>
{lag_charts}
</div>

<div class="section">
<h2 style="margin-bottom:1rem;font-size:1.1rem;">Per-Operation Breakdown &amp; Errors</h2>
{ops_sections}
</div>

<script>
const chartDefaults = {{
  responsive: true,
  plugins: {{ legend: {{ labels: {{ color: '#94a3b8' }} }} }},
  scales: {{
    x: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: '#2d3348' }} }},
    y: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: '#2d3348' }} }}
  }}
}};
{chart_scripts}
</script>
</body>
</html>
"""


def _color(val, good_below=None, bad_above=None):
    if good_below is not None and val <= good_below:
        return "ok"
    if bad_above is not None and val >= bad_above:
        return "bad"
    return "warn"


def _environment_html(env):
    if not env:
        return ""
    node_rows = "".join(
        f"<tr><td>{name}</td><td>{sp.get('cpu_model','?')}</td>"
        f"<td>{sp.get('cpu_cores','?')}</td><td>{sp.get('ram_mb','?')} MB</td>"
        f"<td>{sp.get('kernel','?')}</td><td>{sp.get('os','?')}</td></tr>"
        for name, sp in env.get("nodes", {}).items())
    settings_rows = "".join(
        f"<tr><td>{s['name']}</td><td>{s['setting']}{(' ' + s['unit']) if s['unit'] else ''}</td>"
        f"<td>{s['source']}</td></tr>"
        for s in env.get("pg_settings", []))
    cluster = env.get("cluster", {})
    return f"""
<div class="section">
<h2 style="margin-bottom:1rem;font-size:1.1rem;">Environment</h2>
<div class="chart-wrap">
  <p><b>PostgreSQL:</b> {env.get('postgres_version', '?')}<br>
     <b>Database size:</b> {env.get('database_size', '?')} &nbsp;•&nbsp;
     <b>Cluster at start:</b> leader={cluster.get('leader')}, timeline={cluster.get('timeline')},
     members: {', '.join(cluster.get('members', []))}<br>
     <b>Captured:</b> {env.get('captured_at', '?')}</p>
  <table>
    <thead><tr><th>Node</th><th>CPU</th><th>Cores</th><th>RAM</th><th>Kernel</th><th>OS</th></tr></thead>
    <tbody>{node_rows}</tbody>
  </table>
  <details style="margin-top:1rem;"><summary style="cursor:pointer;color:var(--accent);">
    PostgreSQL non-default settings ({len(env.get('pg_settings', []))} — from pg_settings via SQL)</summary>
    <table><thead><tr><th>Name</th><th>Value</th><th>Source</th></tr></thead>
    <tbody>{settings_rows}</tbody></table>
  </details>
  <details style="margin-top:.5rem;"><summary style="cursor:pointer;color:var(--accent);">
    Patroni dynamic config (patronictl show-config)</summary>
    <pre style="padding:1rem;background:#0f1117;border-radius:8px;overflow-x:auto;">{env.get('patroni_dynamic_config', '')}</pre>
  </details>
  <details style="margin-top:.5rem;"><summary style="cursor:pointer;color:var(--accent);">
    PgPool failure-detection parameters</summary>
    <pre style="padding:1rem;background:#0f1117;border-radius:8px;overflow-x:auto;">{env.get('pgpool_params', '')}</pre>
  </details>
</div>
</div>"""


def generate_report(results: list, output_path: str, environment=None, run_window=""):
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    summary_rows, rps_charts, latency_charts, chart_scripts = [], [], [], []
    lag_charts, ops_sections = [], []

    for i, res in enumerate(results):
        sc = res["scenario"]
        sm = res.get("summary", {})
        tl = res.get("timeline", [])
        inject = res.get("inject_resolved")
        tag_html = ('<span class="tag tag-ha">HA</span>' if inject
                    else '<span class="tag tag-base">Baseline</span>')

        fail_pct = sm.get("fail_pct", 0)
        rto = sm.get("rto_s")
        recovered = sm.get("recovered")
        if rto is None:
            rto_str, rto_cls = "—", ""
            rec_str, rec_cls = "—", ""
        else:
            rto_str = f"{rto}s"
            rto_cls = "ok" if rto <= 10 else ("warn" if rto <= 40 else "bad")
            rec_str = "yes" if recovered else "NO"
            rec_cls = "ok" if recovered else "bad"
        stable = sm.get("cluster_stable")
        if stable is None:
            stable_str, stable_cls = "—", ""
        else:
            stable_str = (f"yes ({sm.get('stabilize_s', '?')}s)" if stable
                          else f"NO (>{sm.get('stabilize_s', '?')}s)")
            stable_cls = "ok" if stable else "bad"

        summary_rows.append(f"""
<tr>
  <td>{sc['name']}{tag_html}</td>
  <td>{sc['users']}</td>
  <td>{sm.get('total_requests', '—')}</td>
  <td class="{_color(fail_pct, 1, 5)}">{fail_pct}%</td>
  <td>{sm.get('avg_ms', '—')}</td>
  <td>{sm.get('p50_ms', '—')}</td>
  <td>{sm.get('p95_ms', '—')}</td>
  <td>{sm.get('p99_ms', '—')}</td>
  <td>{sm.get('rps', '—')}</td>
  <td class="{rto_cls}">{rto_str}</td>
  <td class="{rec_cls}">{rec_str}</td>
  <td class="{stable_cls}">{stable_str}</td>
</tr>""")

        if tl:
            cid = f"chart_{i}"
            labels = json.dumps([f"{pt['t']:.0f}s" for pt in tl])
            rps_data = json.dumps([pt["rps"] for pt in tl])
            fail_data = json.dumps([pt["fail_rps"] for pt in tl])
            p50_data = json.dumps([pt["p50"] for pt in tl])
            p95_data = json.dumps([pt["p95"] for pt in tl])
            lag_data = json.dumps([round((pt.get("lag") or 0) / 1048576, 2) for pt in tl])
            parts = []
            if res.get("started_at"):
                parts.append(f"run {res['started_at']} → {res.get('ended_at', '?')}")
            before, after = res.get("cluster_before", {}), res.get("cluster_after", {})
            if before or after:
                if before.get("leader") != after.get("leader") or \
                   before.get("timeline") != after.get("timeline"):
                    parts.append(f"leader <b>{before.get('leader')}</b> → "
                                 f"<b>{after.get('leader')}</b> "
                                 f"(timeline {before.get('timeline')} → {after.get('timeline')})")
                else:
                    parts.append(f"leader {before.get('leader')} unchanged "
                                 f"(timeline {before.get('timeline')})")
            if inject:
                parts.append(f"<b>{inject['name']}</b> at t={inject['delay']}s: "
                             f"<code>{inject['command']}</code>")
                if inject.get("recovery"):
                    parts.append(f"<b>{inject.get('recovery_name', 'recovery')}</b> "
                                 f"at t={inject.get('recovery_delay')}s: "
                                 f"<code>{inject['recovery']}</code>")
                if inject.get("stable_cmd"):
                    parts.append(f"<b>{inject.get('stable_name', 'stable-check')}</b>: "
                                 f"<code>{inject['stable_cmd']}</code>")
                if sm.get("stabilize_s") is not None:
                    parts.append(f"cluster stable after {sm['stabilize_s']}s")
            meta = " &nbsp;•&nbsp; ".join(parts)

            rps_charts.append(f"""
<div class="chart-wrap">
  <h2>{sc['name']}</h2>
  <p>{meta}</p>
  <canvas id="{cid}_rps" height="80"></canvas>
</div>""")
            latency_charts.append(f"""
<div class="chart-wrap">
  <h2>{sc['name']}</h2>
  <canvas id="{cid}_lat" height="80"></canvas>
</div>""")
            lag_charts.append(f"""
<div class="chart-wrap">
  <h2>{sc['name']}</h2>
  <canvas id="{cid}_lag" height="60"></canvas>
</div>""")

            ops = res.get("operations", [])
            errs = res.get("errors", [])
            ops_rows = "".join(
                f"<tr><td>{o['op']}</td><td>{o['requests']}</td>"
                f"<td class=\"{'bad' if o['failures'] else 'ok'}\">{o['failures']}</td>"
                f"<td>{o['avg_ms']}</td><td>{o['p50_ms']}</td><td>{o['p95_ms']}</td>"
                f"<td>{o['p99_ms']}</td><td>{o['max_ms']}</td></tr>"
                for o in ops)
            err_html = ""
            if errs:
                err_rows = "".join(
                    f"<tr><td>{e['op']}</td><td><code>{e['error']}</code></td>"
                    f"<td>{e['occurrences']}</td></tr>" for e in errs)
                err_html = (f"<p style='margin-top:1rem;color:var(--red);font-size:.85rem;'>"
                            f"Error signatures:</p><table><thead><tr><th>Operation</th>"
                            f"<th>Error</th><th>Count</th></tr></thead>"
                            f"<tbody>{err_rows}</tbody></table>")
            ops_sections.append(f"""
<div class="chart-wrap">
  <h2>{sc['name']}</h2>
  <table>
    <thead><tr><th>Operation</th><th>Requests</th><th>Failures</th>
    <th>Avg ms</th><th>P50 ms</th><th>P95 ms</th><th>P99 ms</th><th>Max ms</th></tr></thead>
    <tbody>{ops_rows}</tbody>
  </table>
  {err_html}
</div>""")

            chart_scripts.append(f"""
new Chart(document.getElementById('{cid}_rps'), {{
  type:'line', data:{{
    labels:{labels},
    datasets:[
      {{label:'RPS',      data:{rps_data},  borderColor:'#6c63ff', backgroundColor:'rgba(108,99,255,.15)', fill:true, tension:.3, pointRadius:0}},
      {{label:'Fail RPS', data:{fail_data}, borderColor:'#ff4757', backgroundColor:'rgba(255,71,87,.15)',  fill:true, tension:.3, pointRadius:0}}
    ]
  }}, options:chartDefaults
}});
new Chart(document.getElementById('{cid}_lat'), {{
  type:'line', data:{{
    labels:{labels},
    datasets:[
      {{label:'P50 ms', data:{p50_data}, borderColor:'#00d085', tension:.3, pointRadius:0}},
      {{label:'P95 ms', data:{p95_data}, borderColor:'#f6c90e', tension:.3, pointRadius:0}}
    ]
  }}, options:chartDefaults
}});
new Chart(document.getElementById('{cid}_lag'), {{
  type:'line', data:{{
    labels:{labels},
    datasets:[
      {{label:'Max replica lag (MB)', data:{lag_data}, borderColor:'#38bdf8',
        backgroundColor:'rgba(56,189,248,.15)', fill:true, tension:.3, pointRadius:0}}
    ]
  }}, options:chartDefaults
}});""")

    html = _HTML_TMPL.format(
        generated=generated,
        run_window=run_window or generated,
        pg_host=config.PG_HOST,
        pg_write_port=config.PG_WRITE_PORT,
        num_scenarios=len(results),
        environment_section=_environment_html(environment),
        summary_rows="\n".join(summary_rows),
        rps_charts="\n".join(rps_charts) or "<p style='color:var(--muted)'>No timeline data collected.</p>",
        latency_charts="\n".join(latency_charts) or "",
        lag_charts="\n".join(lag_charts) or "",
        ops_sections="\n".join(ops_sections) or "",
        chart_scripts="\n".join(chart_scripts),
    )

    with open(output_path, "w") as fh:
        fh.write(html)
    print(f"\n  Report written → {output_path}")
    return output_path

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run HA benchmark scenarios via Locust API")
    parser.add_argument("--output",   default="",   help="HTML report output path")
    parser.add_argument("--scenarios", nargs="*",   help="Scenario IDs to run (default: all)")
    parser.add_argument("--locust-port", type=int,  default=config.LOCUST_WEB_PORT)
    parser.add_argument("--override-duration", type=int, default=0,
                        help="Cap every scenario duration (smoke testing)")
    parser.add_argument("--dry-run", action="store_true", help="Print scenario list and exit")
    args = parser.parse_args()

    selected = [s for s in SCENARIOS if not args.scenarios or s["id"] in args.scenarios]

    if args.dry_run:
        for s in selected:
            print(f"  {s['id']:<35} {s['name']}")
        return

    if args.override_duration:
        for s in selected:
            s["duration"] = min(s["duration"], args.override_duration)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = args.output or os.path.join(config.REPORTS_DIR, f"benchmark_{ts}.html")
    run_started = datetime.now().isoformat(timespec="seconds")

    print(f"Proxy node (benchmark entry point): {PROXY_NODE} ({config.PG_HOST}:{config.PG_WRITE_PORT})")
    print("Scenario commands (override via the named env vars):")
    for s in selected:
        inj = s.get("inject")
        if not inj:
            continue
        print(f"  {inj['name']:<32} {inj['command']}")
        if inj.get("recovery"):
            print(f"  {inj.get('recovery_name', 'recovery'):<32} {inj['recovery']}")
        if inj.get("stable_cmd"):
            print(f"  {inj.get('stable_name', 'stable-check'):<32} {inj['stable_cmd']}")

    # start locust in standalone web mode — API on /swarm controls start/stop
    locust_cmd = [
        sys.executable, "-m", "locust",
        "-f", os.path.join(os.path.dirname(__file__), "locustfile.py"),
        "--web-host", "127.0.0.1",
        "--web-port", str(args.locust_port),
        "--class-picker",   # REQUIRED: /swarm's user_classes is ignored without it
    ]
    print(f"Starting Locust (standalone) on port {args.locust_port} …")
    proc = subprocess.Popen(locust_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    results = []
    try:
        if not _wait_locust_ready(timeout=30):
            print("ERROR: Locust master did not start in time.")
            proc.terminate()
            sys.exit(1)
        print("Locust master ready.")

        environment = capture_environment()

        for scenario in selected:
            result = run_scenario(scenario)
            results.append(result)
            sm = result.get("summary", {})
            print(f"  → requests={sm.get('total_requests','?')}  "
                  f"fail%={sm.get('fail_pct','?')}  "
                  f"p50={sm.get('p50_ms','?')}ms  p99={sm.get('p99_ms','?')}ms  "
                  f"rto={sm.get('rto_s','—')}s  recovered={sm.get('recovered','—')}  "
                  f"stable={sm.get('cluster_stable','—')} in {sm.get('stabilize_s','—')}s")
            time.sleep(5)

        # leave the cluster healthy after the last scenario
        wait_cluster_healthy(timeout=300, label="final")

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    run_finished = datetime.now().isoformat(timespec="seconds")
    report_path = generate_report(results, output, environment=environment,
                                  run_window=f"{run_started} → {run_finished}")
    json_path = os.path.splitext(report_path)[0] + ".json"
    with open(json_path, "w") as fh:
        json.dump({"run_started": run_started, "run_finished": run_finished,
                   "environment": environment, "results": results},
                  fh, indent=2, default=str)
    print(f"  Raw results   → {json_path}")

    # append to the report registry so every timestamped run stays findable
    index_path = os.path.join(os.path.dirname(report_path) or ".", "INDEX.md")
    new_index = not os.path.exists(index_path)
    headline = "  ".join(
        f"{r['scenario']['id'].replace('failover_', '')}={r['summary'].get('rto_s', '—')}s"
        for r in results if r.get("inject_resolved") and r.get("summary"))
    with open(index_path, "a") as fh:
        if new_index:
            fh.write("# Benchmark report registry\n\n"
                     "| Run started | Report | Scenarios | RTO headline |\n"
                     "|---|---|---|---|\n")
        fh.write(f"| {run_started} | [{os.path.basename(report_path)}]"
                 f"({os.path.basename(report_path)}) | {len(results)} | {headline or '—'} |\n")
    print(f"  Registry      → {index_path}")
    print(f"\nDone. Open the report:\n  xdg-open {report_path}")


if __name__ == "__main__":
    main()
