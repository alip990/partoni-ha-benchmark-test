"""Locust benchmark for Patroni/CNPG HA lab.

User classes (select via --class-picker in the Web UI or LOCUST_USER_CLASSES env):
  MixedUser       — balanced 5:1 read/write (default)
  ReadHeavyUser   — 20:1 read/write, uses read_heavy.sql JOIN query
  WriteHeavyUser  — 1:5 read/write, uses write_heavy.sql CTE chain
  FailoverProbe   — 10 probes/s single INSERT+SELECT; measures HA RTO precisely

All connection details come from config.py / environment variables.
SQL queries come from sql/*.sql files — edit those without touching Python.
"""
import logging
import time

from locust import User, between, events, tag, task

import config
from db_tasks import read_heavy, read_simple, run_migration, run_seed, write_heavy, write_simple
from postgres_session import PostgresSession

logger = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_clients(self):
    kwargs = dict(
        host=config.PG_HOST,
        database=config.PG_DATABASE,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        request_event=self.environment.events.request,
    )
    self.write_client = PostgresSession(port=config.PG_WRITE_PORT, **kwargs)
    self.read_client  = PostgresSession(port=config.PG_READ_PORT,  **kwargs)


def _close_clients(self):
    for c in (getattr(self, "write_client", None), getattr(self, "read_client", None)):
        if c:
            try:
                c.close()
            except Exception:
                pass


def _bootstrap(write_client, attempts=5, delay=2):
    """Run migration + seed (idempotent). Retries so a user spawned during a
    brief cluster hiccup still bootstraps; never raises — an on_start exception
    would permanently kill the user before it runs a single task."""
    for i in range(attempts):
        try:
            run_migration(write_client)
            run_seed(write_client)
            return
        except Exception as exc:
            logger.warning("Bootstrap attempt %d/%d failed: %s", i + 1, attempts, exc)
            time.sleep(delay)
    logger.warning("Bootstrap gave up after %d attempts — schema may already exist.", attempts)


# ── User classes ─────────────────────────────────────────────────────────────

class MixedUser(User):
    """Balanced workload: 5 reads per 1 write — typical OLTP mix.
    Uses simple read.sql and write.sql queries.
    Write connections go to PG_WRITE_PORT (primary via PgPool).
    Read connections go to PG_READ_PORT (load-balanced by PgPool)."""

    wait_time = between(0.05, 0.2)

    def on_start(self):
        _make_clients(self)
        _bootstrap(self.write_client)

    def on_stop(self):
        _close_clients(self)

    @tag("write")
    @task(1)
    def task_write(self):
        result = write_simple(self.write_client)
        if not result.success:
            raise Exception("write_simple failed")

    @tag("read")
    @task(5)
    def task_read(self):
        result = read_simple(self.read_client)
        if not result.success:
            raise Exception("read_simple failed")


class ReadHeavyUser(User):
    """Read-dominated workload: 20 complex reads per 1 write.
    Uses read_heavy.sql (8-table JOIN with window aggregates) for reads.
    Models a reporting / analytics workload — good for testing
    PgPool's read load-balancing across standbys."""

    wait_time = between(0.05, 0.3)

    def on_start(self):
        _make_clients(self)
        _bootstrap(self.write_client)

    def on_stop(self):
        _close_clients(self)

    @tag("write")
    @task(1)
    def task_write(self):
        result = write_simple(self.write_client)
        if not result.success:
            raise Exception("write_simple failed")

    @tag("read")
    @task(20)
    def task_read_heavy(self):
        result = read_heavy(self.read_client)
        if not result.success:
            raise Exception("read_heavy failed")


class WriteHeavyUser(User):
    """Write-dominated workload: 5 write chains per 1 read.
    Uses write_heavy.sql (5-table CTE: Province→City→Address→Manufacturer→Device).
    Models an ingestion / ETL workload — drives replication lag on standbys."""

    wait_time = between(0.05, 0.15)

    def on_start(self):
        _make_clients(self)
        _bootstrap(self.write_client)

    def on_stop(self):
        _close_clients(self)

    @tag("write")
    @task(5)
    def task_write_heavy(self):
        result = write_heavy(self.write_client)
        if not result.success:
            raise Exception("write_heavy failed")

    @tag("read")
    @task(1)
    def task_read(self):
        result = read_simple(self.read_client)
        if not result.success:
            raise Exception("read_simple failed")


class FailoverProbe(User):
    """High-frequency HA probe: one INSERT + one SELECT per 100ms tick.
    Measures RTO precisely — each failed request = ~100ms of downtime.
    Uses a SINGLE write+read connection through the HA endpoint.
    Fire this class alone with 1 user while triggering failover scenarios."""

    wait_time = between(0.08, 0.12)   # ~10 probes/s

    def on_start(self):
        kwargs = dict(
            host=config.PG_HOST,
            database=config.PG_DATABASE,
            user=config.PG_USER,
            password=config.PG_PASSWORD,
            request_event=self.environment.events.request,
        )
        self.client = PostgresSession(port=config.PG_WRITE_PORT, **kwargs)
        _bootstrap(self.client)

    def on_stop(self):
        client = getattr(self, "client", None)
        if client:
            try:
                client.close()
            except Exception:
                pass

    @task
    def probe(self):
        # Write: insert a device, capture its ID
        write_result = write_simple(self.client)
        if not write_result.success:
            raise Exception("probe_write failed")

        # Read: read it back (confirms primary accepted the write and is readable)
        read_result = read_simple(self.client)
        if not read_result.success:
            raise Exception("probe_read failed")


# ── Startup event ─────────────────────────────────────────────────────────────

@events.init_command_line_parser.add_listener
def add_custom_args(parser, **_kw):
    parser.add_argument("--skip-bootstrap", action="store_true",
                        help="Skip migration/seed on first user start")
