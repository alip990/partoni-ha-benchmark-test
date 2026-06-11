# PostgreSQL HA Benchmark — Full Results (all scenarios)

**Run window:** 2026-06-11 16:28:58 → 16:41:21 (12.5 min, run 3 — final methodology)
**Report:** `reports/benchmark_20260611_162858.html` · **Raw data:** `reports/benchmark_20260611_162858.json` · **Registry:** `reports/INDEX.md`
Every run gets a timestamped report pair plus a row in `INDEX.md`, so results accumulate and stay comparable across configuration experiments.

---

## 1. Environment (captured automatically at run start)

The runner snapshots everything the numbers depend on — via SQL for PostgreSQL, via SSH for the nodes — and embeds it in the HTML report ("Environment" section) and JSON (`environment` key).

### 1.1 Hardware / OS (identical for all 3 nodes + load generator — containers share the host)

| | |
|---|---|
| CPU | 13th Gen Intel Core i7-1355U, 12 threads |
| RAM | 23,656 MB total (shared) |
| Kernel | 6.17.0-29-generic |
| OS | Ubuntu 24.04.4 LTS |
| Nodes | Docker systemd containers `node1/2/3` @ 192.168.88.101–103 (bridge `pglab0`) |

> All three "nodes" are containers on one laptop: no real network latency, shared disk and CPU. Absolute latencies are optimistic vs. real servers; the *relative* differences between scenarios and the failover *mechanics/timings* (TTL, health checks) transfer directly.

### 1.2 Software

| Component | Version / value |
|---|---|
| PostgreSQL | 16.14 (Ubuntu 16.14-1.pgdg24.04+1) |
| Patroni | 4.1.3, etcd3 DCS |
| PgPool-II | 4.x — entry point `node1:9999` for writes *and* reads |
| Locust | 2.x, standalone web mode, driven via REST API (`--class-picker`) |
| Database size | 178 MB at run start |

### 1.3 PostgreSQL configuration (from `pg_settings` via SQL)

50 non-default settings are recorded in the report; the load-relevant core:

```
shared_buffers=1GB            work_mem=16MB              maintenance_work_mem=256MB
effective_cache_size=3GB      max_connections=200        max_wal_size=2GB
random_page_cost=1.1          effective_io_concurrency=16
wal_log_hints=on (pg_rewind)  shared_preload_libraries=pg_stat_statements
pg_stat_statements.track=all  track_io_timing=on
```

Editable live via `db-ha-lab/scripts/pg-config.sh` (`show → edit → diff → apply → restart`); the file `db-ha-lab/postgres/patroni-dynamic-config.yml` is the canonical copy. See `docs/POSTGRES-CONFIG.md`.

### 1.4 HA timing configuration (decides the RTO numbers below)

```
Patroni:  ttl=30  loop_wait=10  retry_timeout=10  maximum_lag_on_failover=1MB
          synchronous_mode=false  use_pg_rewind=true  use_slots=true
PgPool:   sr_check_period=5  health_check_period=5  health_check_timeout=5
          health_check_max_retries=2  auto_failback=on
          failover_on_backend_error=off  search_primary_node_timeout=60
```

---

## 2. Method

- **Probe:** `FailoverProbe` — 1 user, one connection through the write port; each cycle = `INSERT` + `SELECT` (~9 cycles/s ≈ 18 req/s healthy). Lazy reconnect on every error, so it survives outages instead of dying.
- **Timeline:** Locust polled every 2 s; points are **per-interval deltas** (true rates, not smoothed). Each point also samples **max replica lag** from Patroni REST.
- **RTO** = first failing/stalled 2 s sample after injection → first of ≥2 consecutive clean samples. `detect_lag` = injection → first degraded sample.
- **Health gate before every scenario:** 1 leader + 2 streaming replicas (Patroni REST `:8008/cluster`) **and** a real write through PgPool. No scenario starts against a degraded cluster.
- **Stability check after every HA scenario:** the same gate (plus optional per-scenario `*_STABLE_CMD`), with time-to-stable recorded.
- **Cluster movement:** leader + timeline recorded before/after every scenario.
- **Named, env-overridable commands** per scenario, resolved against live cluster state at injection time (`{leader}`, `{replica}`, `{target}`):

| Scenario | Inject | Recovery | Stability |
|---|---|---|---|
| Leader Crash | `LEADER_CRASH_INJECT_CMD` → `docker kill {leader}` | `LEADER_CRASH_RECOVERY_CMD` → `docker start {target}` | `LEADER_CRASH_STABLE_CMD` |
| Switchover | `SWITCHOVER_INJECT_CMD` → `patronictl switchover --force` | — (graceful) | `SWITCHOVER_STABLE_CMD` |
| Replica Power-Off | `REPLICA_POWEROFF_INJECT_CMD` → `docker kill {replica}` | `REPLICA_POWEROFF_RECOVERY_CMD` → `docker start {target}` | `REPLICA_POWEROFF_STABLE_CMD` |

Hetzner: swap to `hcloud server poweroff {leader}` etc. in `.env` — see `.env.example`.

---

## 3. Results summary

| Scenario | Window (local) | Users | Requests | Fail % | P50 | P95 | P99 | RPS | **RTO** | Leader / TL | Stable after |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Baseline — Mixed | 16:29–16:30 | 10 | 7,084 | 0 % | 9 ms | 11 ms | 13 ms | 78.7 | — | node2 (23) | — |
| Baseline — Read Heavy | 16:30–16:32 | 20 | 5,684 | 0 % | 28 ms | 39 ms | 67 ms | 63.2 | — | node2 (23) | — |
| Baseline — Write Heavy | 16:32–16:33 | 10 | 8,755 | 0 % | 2 ms | 13 ms | 17 ms | 97.3 | — | node2 (23) | — |
| **HA — Leader Crash (A1)** | 16:34–16:36 | 1 | 2,074 | 0.10 % | 7 ms | 13 ms | 17 ms | 13.8 | **28.0 s** | node2→node3 (23→24) | 0.0 s |
| **HA — Planned Switchover (A6)** | 16:36–16:38 | 1 | 2,040 | 0.59 % | 7 ms | 13 ms | 17 ms | 17.0 | **4.0 s** | node3→node1 (24→25) | 0.0 s |
| **HA — Replica Power-Off (A2)** | 16:38–16:41 | 1 | 2,589 | **0 %** | 7 ms | 14 ms | 16 ms | 17.3 | **0 s** ¹ | node1 (25) unchanged | 0.0 s |

¹ This run's probe session wasn't pinned to the killed replica — see §5.6; the previous run measured **22.1 s** for the unlucky case. Both outcomes are real.

**Headlines: unplanned failover 28 s · planned switchover 4 s · replica loss 0–22 s depending on session pinning · cluster self-healed to full strength after every failure (stability gate passed instantly each time).**

Cross-run RTO history (`reports/INDEX.md`):

| Run | Leader crash | Switchover | Replica power-off |
|---|---|---|---|
| 2026-06-11 15:35 (run 2) | 34.1 s | 2.0 s | 22.1 s (pinned session) |
| 2026-06-11 16:28 (run 3) | 28.0 s | 4.0 s | 0 s (unpinned session) |

The leader-crash spread (28–34 s) is expected: it depends on where in Patroni's 10 s loop the kill lands relative to the 30 s TTL expiry.

---

## 4. Baseline analysis (true per-class profiles)

> Run 2's baselines were silently **blended**: Locust ignores `/swarm`'s `user_classes` unless started with `--class-picker`, so all four user classes ran in every "baseline". The per-operation breakdown added in this run exposed it (a `WITH` CTE op appeared inside a MixedUser run). Run 3 numbers below are the real labeled profiles.

### 4.1 Mixed Load — 10 × `MixedUser`, 5:1 read/write, 90 s

| Operation | Requests | Fail | P50 | P95 | P99 | Max |
|---|---|---|---|---|---|---|
| SELECT (`read.sql`, 50-row scan) | 5,794 | 0 | 9 ms | 11 ms | 12 ms | 39 ms |
| INSERT (`write.sql`, 1 row) | 1,186 | 0 | 2 ms | 5 ms | 37 ms | 230 ms |
| CONNECT | 20 | 0 | 24 ms | 36 ms | — | 36 ms |

78.7 req/s total; measured ratio 4.9 : 1 (target 5 : 1). Replication lag peaked at **0.04 MB** — replicas effectively in lock-step. Writes are 4× cheaper than the 50-row read at the median.

### 4.2 Read Heavy — 20 × `ReadHeavyUser`, 20:1 read/write, 90 s

| Operation | Requests | Fail | P50 | P95 | P99 | Max |
|---|---|---|---|---|---|---|
| SELECT (`read_heavy.sql`, 8-table JOIN + window fn) | 5,373 | 0 | 28 ms | 39 ms | 67 ms | 222 ms |
| INSERT | 279 | 0 | 4 ms | 26 ms | 51 ms | 170 ms |

63.2 req/s — the heavy join costs **28 ms at p50** (vs 9 ms for the simple read; the blended run had reported a misleading 6 ms). 20 users × ~28 ms/query ≈ the observed throughput: the workload is genuinely query-bound, and PgPool spreads it across both replicas (without the read split, one node would carry all 60 q/s).

### 4.3 Write Heavy — 10 × `WriteHeavyUser`, 5:1 write/read, 90 s

| Operation | Requests | Fail | P50 | P95 | P99 | Max |
|---|---|---|---|---|---|---|
| WITH (`write_heavy.sql`, 5-table CTE chain) | 7,227 | 0 | 2 ms | 12 ms | 17 ms | 213 ms |
| SELECT | 1,508 | 0 | 10 ms | 13 ms | 15 ms | 34 ms |

97.3 req/s — the highest baseline throughput: the 5-table insert chain (Province→City→Address→Manufacturer→Device) commits in 2 ms at the median on this hardware. Replication lag stayed at 0.00 MB even at ~80 write chains/s: the 1 MB `maximum_lag_on_failover` budget is never approached, meaning failover candidates are always eligible.

---

## 5. HA scenario analysis

### 5.4 HA — Leader Crash (A1): `docker kill node2` (the live leader)

**What it simulates.** Sudden leader death — power loss, kernel panic, OOM-kill. The cluster must *detect* (etcd lease expiry), *elect* (Patroni), *promote* (pg_promote) and *reroute* (PgPool) with no human.

**Sequence (from the run log + JSON):**

```
16:34:02  scenario start  (gate: leader=node2, replicas node1+node3, write OK)
t=30s     leader-crash-inject: docker kill node2
t=32s     probe degrades (detect_lag 2.0s) — then full stall, rps=0
t≈60s     node2's etcd lease (ttl=30) expires → election → node3 promotes
          (timeline 23 → 24); PgPool follow-primary callback attaches new primary
t=60.3s   probe clean again at full rate           ← RTO = 28.0 s
t=90s     leader-crash-recovery: docker start node2 — services auto-start,
          Patroni rejoins node2 as replica via pg_rewind
t=150s    scenario end; stability gate passes instantly (3/3 members, write OK)
```

**Per-operation detail (what the new breakdown shows):**

| Operation | Requests | Fail | P50 | P99 | **Max** |
|---|---|---|---|---|---|
| INSERT | 1,037 | 2 | 4 ms | 30 ms | **21,399 ms** |
| SELECT | 1,035 | 0 | 10 ms | 14 ms | 1,013 ms |
| CONNECT | 2 | 0 | — | — | 6,952 ms |

The 21.4 s INSERT max *is* the outage as one request experienced it: the in-flight INSERT hung on the dead leader's socket until PgPool tore the session down. Error signatures captured:
`FATAL: unable to read data from DB node 1` (×1), `server closed the connection unexpectedly` (×2 total hard failures — everything else stalled rather than erred).

**Interpretation.** RTO 28.0 s ≈ TTL (30 s) dominated, exactly the theory. Only 0.10 % of requests hard-failed; the practical damage is *hanging connections*, which is why client-side timeouts matter as much as server HA. Tuning `ttl=15/loop_wait=5/retry_timeout=5` (one `pg-config.sh apply`) should land RTO near 15 s — re-run `--scenarios failover_leader_crash` to verify and the registry keeps both results.

### 5.5 HA — Planned Switchover (A6): `patronictl switchover --force`

**What it simulates.** Operator-initiated leader move — the procedure before node maintenance, kernel updates, hardware swaps.

**Sequence.** Leader node3 → node1 (timeline 24 → 25), coordinated: Patroni checkpoints the old leader, waits for replica catch-up, demotes and promotes atomically. PgPool drops sessions to the demoted node; the probe reconnects to the new primary.

| Operation | Requests | Fail | P50 | P99 | Max |
|---|---|---|---|---|---|
| INSERT | 1,025 | 12 | 4 ms | 24 ms | 2,079 ms |
| SELECT | 1,013 | 0 | 10 ms | 15 ms | 19 ms |

**RTO 4.0 s**, 12 failed INSERTs (0.59 %), all in the promote window, with precise signatures:
`FATAL: failed to create a backend 2 connection` (×11 — PgPool refusing new backend connections mid-promote), `unable to read data from DB node 2` (×1). Reads never failed — replicas kept serving throughout.

**Interpretation.** 4 s of write disruption, zero read disruption, zero data loss (replication drained before promote). **7× cheaper than the crash path (4 s vs 28 s)** — the case for always draining via switchover before maintenance.

### 5.6 HA — Replica Power-Off (A2): `docker kill node2` (a streaming replica)

**What it simulates.** Loss of a non-leader node — disk failure, accidental shutdown of a standby.

**This run:** RTO **0 s, zero failures**, INSERT max 212 ms, SELECT max 21 ms. The probe never noticed a node die.
**Previous run (15:35):** RTO **22.1 s** — full probe stall.

**Both are correct.** PgPool pins each session's read load-balancing to one backend at connect time (`lb_weight 0.33` each). If your session's reader is the node that dies, your reads hang until PgPool's health-check budget expires (`health_check_period 5s + (timeout 5s + retry) × 2 ≈ 15–20 s`), PgPool detaches the backend and kills the pinned sessions; reconnects then land on survivors. If your session was pinned elsewhere — this run — replica loss is invisible. Expected impact for a session population: ~⅓ of sessions stall ~20 s, ⅔ see nothing; **writes are never affected** (the leader was untouched; lag stayed ≤ 0.05 MB).

**Interpretation.** "Replica loss is free" is only true per-session-by-luck. The PgPool health-check budget is the read-path analogue of Patroni's TTL and deserves the same tuning attention (e.g. `period=2/timeout=2/retries=1` ≈ 6 s worst case). The cluster itself healed identically in both runs: node2 rebooted, services auto-started, rejoined as a streaming replica before scenario end.

---

## 6. Key findings

1. **Unplanned RTO is configuration, not fate: 28–34 s measured, dominated by `ttl=30`.** The spread across runs comes from where the crash lands in the `loop_wait` cycle. Halve the TTL → halve the RTO (at the cost of failover sensitivity).
2. **Planned switchover: 2–4 s, writes only, zero data loss.** 7–17× cheaper than crash failover. Drain before maintenance, always.
3. **Replica loss is a lottery ticket per session:** ~⅓ of sessions (those read-pinned to the dead node) stall for the PgPool health-check budget (~20 s); the rest see nothing; writes never suffer. Tune `health_check_*` like you tune `ttl`.
4. **Failures are stalls, not errors.** 0–12 hard failures per scenario; the real exposure is hung requests (one INSERT waited 21.4 s). Client-side timeouts (`connect_timeout`, `statement_timeout`) define how outages *feel*.
5. **The error signatures differentiate the failure modes precisely:** crash → `unable to read data from DB node` + `server closed the connection`; switchover → `failed to create a backend connection` (refused during promote). Useful for alerting rules: the strings tell you *which* HA event you're in.
6. **Self-healing is real but only with boot-enabled services.** Every killed node returned to `streaming` before its scenario ended (pg_rewind for the ex-leader); the post-scenario stability gate passed in 0.0 s all six times. Run 1 proved the opposite case: without `systemctl enable`, a "recovered" node is an empty shell and the cluster degrades to quorum loss.
7. **The true workload profiles matter:** the heavy 8-table join costs 28 ms p50 (not the 6 ms a blended run suggested); the 5-table CTE write chain commits in 2 ms; replication lag never exceeded 0.05 MB. And methodologically — **per-operation breakdowns caught a class-selection bug** (`--class-picker`) **that aggregate numbers hid completely.**

---

## 7. Reproducing & extending

```bash
cd ~/Downloads/partoni-ha-benchmark-test

# Full suite → timestamped report + JSON + INDEX.md row
PG_HOST=192.168.88.101 PG_DATABASE=postgres PG_USER=postgres \
PG_PASSWORD=ChangeMe_Postgres1 PG_WRITE_PORT=9999 PG_READ_PORT=9999 \
PATRONICTL="ssh node1 sudo patronictl -c /etc/patroni/config.yml" \
DOCKER_KILL="docker kill" DOCKER_START="docker start" \
.venv-host/bin/python run_scenarios.py

# One scenario / smoke
.venv-host/bin/python run_scenarios.py --scenarios failover_leader_crash
.venv-host/bin/python run_scenarios.py --scenarios baseline_mixed --override-duration 20

# Config experiment workflow (thesis):
cd ~/Downloads/db-ha-lab
./scripts/pg-config.sh edit      # e.g. ttl: 15, loop_wait: 5, retry_timeout: 5
./scripts/pg-config.sh apply     # live, no restart needed for these
cd - && .venv-host/bin/python run_scenarios.py --scenarios failover_leader_crash
# → compare rows in reports/INDEX.md
```

On node1 the same kit is deployed at `/opt/locust-bench` (`ansible-playbook site-locust.yml`), web UI `http://192.168.88.101:8090`.

## 8. Artifacts

| File | Content |
|---|---|
| `reports/benchmark_20260611_162858.html` | This run: summary, environment, RPS/latency/lag charts, per-op tables, error signatures |
| `reports/benchmark_20260611_162858.json` | `{run_started, run_finished, environment, results[]}` — every timeline point, snapshot and error |
| `reports/INDEX.md` | Append-only registry of all runs with RTO headlines |
| `reports/benchmark_full.{html,json}`, `switchover_rerun.{html,json}` | Run 2 artifacts (blended baselines; A2 pinned-session outcome) |
| `../db-ha-lab/postgres/patroni-dynamic-config.yml` + `scripts/pg-config.sh` | The cluster config as an editable, appliable file |
| `../db-ha-lab/docs/POSTGRES-CONFIG.md` | Config layers, reload-vs-restart reference |
