"""Central configuration — change values here or export env vars.
All other files import from this module; nothing else should hardcode IPs or passwords."""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))

# ── Database connection ──────────────────────────────────────────────────────
# Stack A (Patroni + PgPool): both ports → PgPool :9999 (handles routing)
# Stack B (CNPG NodePort):   both ports → :30432
PG_HOST          = os.getenv("PG_HOST",          "192.168.88.101")
PG_DATABASE      = os.getenv("PG_DATABASE",      "postgres")
PG_USER          = os.getenv("PG_USER",          "postgres")
PG_PASSWORD      = os.getenv("PG_PASSWORD",      "ChangeMe_Postgres1")
PG_WRITE_PORT    = int(os.getenv("PG_WRITE_PORT", "9999"))
PG_READ_PORT     = int(os.getenv("PG_READ_PORT",  "9999"))
PG_CONNECT_TIMEOUT = int(os.getenv("PG_CONNECT_TIMEOUT", "3"))

# ── SQL files ────────────────────────────────────────────────────────────────
SQL_DIR = os.getenv("SQL_DIR", os.path.join(_HERE, "sql"))

# ── Locust web server ────────────────────────────────────────────────────────
LOCUST_WEB_HOST = os.getenv("LOCUST_WEB_HOST", "0.0.0.0")
LOCUST_WEB_PORT = int(os.getenv("LOCUST_WEB_PORT", "8089"))

# ── Report output ────────────────────────────────────────────────────────────
REPORTS_DIR = os.getenv("REPORTS_DIR", os.path.join(_HERE, "reports"))

# ── Standalone mode ──────────────────────────────────────────────────────────
# STANDALONE=1 → benchmark ANY PostgreSQL (a single server, RDS, another
# cluster) with nothing but PG_* settings: disables Patroni REST health gates,
# node SSH capture and the HA failover scenarios (baselines + reports work).
# The write-path gate still runs: it creates/updates a tiny `ha_gate` table.
STANDALONE = os.getenv("STANDALONE", "").lower() in ("1", "true", "yes")

# ── HA lab: failure injection (used by run_scenarios.py) ────────────────────
# These commands are run from the host running run_scenarios.py.
# Adjust if you're running from inside a node or against Hetzner VMs.
LAB_NODE1_IP  = os.getenv("LAB_NODE1_IP",  "192.168.88.101")
LAB_NODE2_IP  = os.getenv("LAB_NODE2_IP",  "192.168.88.102")
LAB_NODE3_IP  = os.getenv("LAB_NODE3_IP",  "192.168.88.103")
PATRONICTL    = os.getenv("PATRONICTL",    "ssh node1 sudo patronictl -c /etc/patroni/config.yml")
DOCKER_KILL   = os.getenv("DOCKER_KILL",   "docker kill")
DOCKER_START  = os.getenv("DOCKER_START",  "docker start")

# Patroni REST API (used by run_scenarios.py for health gates + leader detection)
PATRONI_REST_PORT = int(os.getenv("PATRONI_REST_PORT", "8008"))

# How to run a shell command on a node (environment capture: CPU/RAM/OS specs).
# {node} = patroni member name. Hetzner: NODE_SHELL="ssh ubuntu@{node}".
NODE_SHELL = os.getenv("NODE_SHELL", "ssh {node}")
LAB_NODES = {                      # patroni member name → IP (must match container names)
    "node1": LAB_NODE1_IP,
    "node2": LAB_NODE2_IP,
    "node3": LAB_NODE3_IP,
}

# ── Per-scenario commands (override any of these per environment) ────────────
# Each HA scenario has its OWN named inject / recovery / stability-check command.
# Placeholders resolved at injection time against live cluster state:
#   {leader}  → current Patroni leader        {replica} → a streaming replica
#   {target}  → the node the inject command hit (for recovery)
#
# Docker lab defaults below. Hetzner example:
#   LEADER_CRASH_INJECT_CMD="hcloud server poweroff {leader}"
#   LEADER_CRASH_RECOVERY_CMD="hcloud server poweron {target}"
#   LEADER_CRASH_STABLE_CMD="ssh {target} sudo patronictl -c /etc/patroni/config.yml list | grep -q streaming"
#
# *_STABLE_CMD is an OPTIONAL extra shell check run after recovery: the runner
# retries it until exit code 0 (or timeout). The built-in stability gate
# (1 leader + 2 streaming replicas via Patroni REST + a real write through
# PgPool) always runs first — the custom command is on top of that.

LEADER_CRASH_INJECT_CMD       = os.getenv("LEADER_CRASH_INJECT_CMD",       f"{DOCKER_KILL} {{leader}}")
LEADER_CRASH_RECOVERY_CMD     = os.getenv("LEADER_CRASH_RECOVERY_CMD",     f"{DOCKER_START} {{target}}")
LEADER_CRASH_STABLE_CMD       = os.getenv("LEADER_CRASH_STABLE_CMD",       "")

SWITCHOVER_INJECT_CMD         = os.getenv("SWITCHOVER_INJECT_CMD",         f"{PATRONICTL} switchover pgcluster --force")
SWITCHOVER_RECOVERY_CMD       = os.getenv("SWITCHOVER_RECOVERY_CMD",       "")   # graceful op — nothing to recover
SWITCHOVER_STABLE_CMD         = os.getenv("SWITCHOVER_STABLE_CMD",         "")

REPLICA_POWEROFF_INJECT_CMD   = os.getenv("REPLICA_POWEROFF_INJECT_CMD",   f"{DOCKER_KILL} {{replica}}")
REPLICA_POWEROFF_RECOVERY_CMD = os.getenv("REPLICA_POWEROFF_RECOVERY_CMD", f"{DOCKER_START} {{target}}")
REPLICA_POWEROFF_STABLE_CMD   = os.getenv("REPLICA_POWEROFF_STABLE_CMD",   "")
