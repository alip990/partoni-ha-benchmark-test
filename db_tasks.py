"""Database task functions — load SQL from sql/ files.
To change a query: edit the corresponding sql/*.sql file, no Python change needed."""
import logging
import os

from config import SQL_DIR

logger = logging.getLogger(__name__)

_sql_cache: dict = {}


def _sql(name: str) -> str:
    if name not in _sql_cache:
        path = os.path.join(SQL_DIR, f"{name}.sql")
        with open(path) as fh:
            _sql_cache[name] = fh.read()
    return _sql_cache[name]


def _split_sql(text: str):
    """Split SQL text into statements, respecting dollar-quoting, single-quotes,
    and -- line comments (semicolons inside comments are not statement terminators)."""
    stmts, cur, i = [], [], 0
    in_dollar = None   # dollar-quote tag currently open, e.g. '$$' or '$EF$'
    in_string = False
    while i < len(text):
        ch = text[i]
        # Skip rest of line for -- comments (only when not inside quoted context)
        if not in_string and not in_dollar and ch == '-' and text[i:i+2] == '--':
            eol = text.find('\n', i)
            end = eol if eol != -1 else len(text)
            cur.append(text[i:end])
            i = end
            continue
        # Dollar-quoting open
        if not in_string and not in_dollar and ch == '$':
            j = text.find('$', i + 1)
            if j != -1:
                tag = text[i:j + 1]
                cur.append(tag)
                in_dollar = tag
                i = j + 1
                continue
        # Dollar-quoting close (scan for the closing tag)
        if in_dollar:
            j = text.find(in_dollar, i)
            if j != -1:
                cur.append(text[i:j + len(in_dollar)])
                i = j + len(in_dollar)
                in_dollar = None
            else:
                cur.append(text[i:])
                break
            continue
        # Single-quoted strings
        if ch == "'" and not in_dollar:
            in_string = not in_string
        # Statement terminator
        if not in_string and ch == ';':
            stmt = ''.join(cur).strip()
            code = '\n'.join(l for l in stmt.splitlines() if not l.strip().startswith('--'))
            if code.strip():
                stmts.append(stmt)
            cur = []
        else:
            cur.append(ch)
        i += 1
    return stmts


def run_migration(client) -> None:
    """Apply full DM schema (idempotent — safe to re-run)."""
    for stmt in _split_sql(_sql("migration")):
        try:
            client.execute_query(stmt)
        except Exception as exc:
            logger.error("Migration statement failed: %s\n%s", exc, stmt[:120])
            raise


def run_seed(client) -> None:
    """Insert reference data (idempotent — ON CONFLICT DO NOTHING)."""
    for stmt in _split_sql(_sql("seed")):
        try:
            client.execute_query(stmt)
        except Exception as exc:
            logger.error("Seed statement failed: %s\n%s", exc, stmt[:120])
            raise


# ── Read tasks ───────────────────────────────────────────────────────────────

def read_simple(client):
    """Single-table read: recent devices."""
    return client.execute_query(_sql("read"))


def read_heavy(client):
    """8-table join with window aggregates."""
    return client.execute_query(_sql("read_heavy"))


# ── Write tasks ──────────────────────────────────────────────────────────────

def write_simple(client):
    """Single-row device insert."""
    return client.execute_query(_sql("write"))


def write_heavy(client):
    """5-table CTE write chain (Province→City→Address→Manufacturer→Device)."""
    return client.execute_query(_sql("write_heavy"))
