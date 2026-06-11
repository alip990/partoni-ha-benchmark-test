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


def _run_command(cmd, name="cmd"):
    if not cmd:
        return
    print(f"  [{name}] {cmd}")
    subprocess.run(cmd, shell=True, capture_output=True)


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
            _run_command(inject["command"], name=inject["name"])
            injected = True
            inject_t = elapsed

        if inject and injected and not recovery_done and inject.get("recovery"):
            if elapsed >= inject.get("recovery_delay", scenario["duration"] - 20):
                print(f"  [t={elapsed:.0f}s] running recovery")
                _run_command(inject["recovery"], name=inject.get("recovery_name", "recovery"))
                recovery_done = True

        snap = _get_stats()
        agg = _aggregated(snap)
        if agg:
            req, fail = agg.get("num_requests", 0), agg.get("num_failures", 0)
            pcts = snap.get("current_response_time_percentiles", {}) or {}
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
                })
            prev = (elapsed, req, fail)
        time.sleep(2)

    _stop_and_wait()

    # 6. final cumulative stats
    final = _get_stats()
    agg = _aggregated(final)
    result = {"scenario": {k: v for k, v in scenario.items() if k != "inject"},
              "inject_resolved": inject,
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

    return result


def _estimate_rto(timeline, inject_t):
    """Outage = first sample after injection with failures or zero throughput,
    until the first of >=2 consecutive clean samples. Returns (rto_s,
    detection_lag_s, recovered)."""
    post = [pt for pt in timeline if pt["t"] >= inject_t]
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
<p class="sub">Generated {generated} &nbsp;|&nbsp; Entry point: PgPool {pg_host}:{pg_write_port} &nbsp;|&nbsp; Scenarios: {num_scenarios}</p>

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


def generate_report(results: list, output_path: str):
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    summary_rows, rps_charts, latency_charts, chart_scripts = [], [], [], []

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
            meta = ""
            if inject:
                parts = [f"<b>{inject['name']}</b> at t={inject['delay']}s: "
                         f"<code>{inject['command']}</code>"]
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
}});""")

    html = _HTML_TMPL.format(
        generated=generated,
        pg_host=config.PG_HOST,
        pg_write_port=config.PG_WRITE_PORT,
        num_scenarios=len(results),
        summary_rows="\n".join(summary_rows),
        rps_charts="\n".join(rps_charts) or "<p style='color:var(--muted)'>No timeline data collected.</p>",
        latency_charts="\n".join(latency_charts) or "",
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

    report_path = generate_report(results, output)
    json_path = os.path.splitext(report_path)[0] + ".json"
    with open(json_path, "w") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"  Raw results   → {json_path}")
    print(f"\nDone. Open the report:\n  xdg-open {report_path}")


if __name__ == "__main__":
    main()
