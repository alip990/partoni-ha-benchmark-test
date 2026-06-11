"""PostgreSQL session with Locust request event integration.
Uses psycogreen for gevent-compatible async I/O."""
import logging
import time
from typing import Any, List, Optional

import psycopg2
from psycopg2 import DatabaseError, OperationalError

import psycogreen.gevent
psycogreen.gevent.patch_psycopg()

import config as _cfg

logger = logging.getLogger(__name__)


class PostgresResponse:
    def __init__(self, success: bool, response_time: float,
                 exception: Optional[Exception], response_length: int,
                 result: Optional[List[Any]] = None):
        self.success = success
        self.response_time = response_time
        self.exception = exception
        self.response_length = response_length
        self.result = result or []

    def __str__(self):
        return f"success={self.success} rows={self.response_length} exc={self.exception}"


class PostgresSession:
    def __init__(self, host: str, port: int, database: str,
                 user: str, password: str, request_event):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.request_event = request_event
        self.connection = None
        self._cursor = None
        # Connection is LAZY: first execute_query() connects. A constructor that
        # raises would kill the Locust user in on_start during an outage —
        # exactly when the FailoverProbe must stay alive to measure RTO.

    # ── connection management ─────────────────────────────────────────────────

    def _connect(self):
        start = time.time()
        try:
            self.connection = psycopg2.connect(
                host=self.host, port=self.port,
                dbname=self.database, user=self.user, password=self.password,
                connect_timeout=_cfg.PG_CONNECT_TIMEOUT,
            )
            self.connection.autocommit = True
            elapsed = (time.time() - start) * 1000
            self.request_event.fire(request_type="PG", name="CONNECT",
                                    response_time=elapsed, response_length=0)
        except Exception:
            # Do NOT fire a failure event here: execute_query() records the
            # failure for the query that triggered the reconnect. Firing both
            # would double-count every probe failure (CONNECT + PG_QUERY).
            self.connection = None
            raise

    def _cursor_obj(self):
        if not self._cursor or self._cursor.closed:
            if not self.connection:
                self._connect()
            self._cursor = self.connection.cursor()
        return self._cursor

    def reset(self):
        self.close()
        self._connect()

    def close(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            pass
        finally:
            self.connection = None
            self._cursor = None

    # ── query execution ───────────────────────────────────────────────────────

    def execute_query(self, query: str,
                      params: Optional[tuple] = None) -> PostgresResponse:
        start = time.time()
        op = query.split()[0].upper() if query.split() else "QUERY"
        try:
            cur = self._cursor_obj()
            cur.execute(query, params)
            if cur.description:
                rows = cur.fetchall()
                length = len(rows)
            else:
                rows = []
                length = cur.rowcount if cur.rowcount >= 0 else 0
            elapsed = (time.time() - start) * 1000
            self.request_event.fire(request_type="PG_QUERY", name=op,
                                    response_time=elapsed, response_length=length)
            return PostgresResponse(True, elapsed, None, length, rows)
        except (OperationalError, DatabaseError, Exception) as exc:
            elapsed = (time.time() - start) * 1000
            self.request_event.fire(request_type="PG_QUERY", name=op,
                                    response_time=elapsed, response_length=0,
                                    exception=exc)
            # On connection-level errors, reset so the next call reconnects
            if isinstance(exc, (OperationalError, psycopg2.InterfaceError)):
                self.close()
            raise
