#!/usr/bin/env python3
"""agent2agent — decentralized agent-to-agent communication.

One agent publishes a named result; another blocks until it appears.

  Agent A (producer):   agent_publish(name="researcher", result="…")
  Agent B (consumer):   agent_wait(name="researcher")   ← blocks until A publishes

Backend is selected by AGENT_BUS_DB:
  ~/.agent_bus.db            → SQLite (default, local)
  /path/to/file.db           → SQLite at that path
  postgresql://user:pw@host/db → Postgres (remote, push via LISTEN/NOTIFY)
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

from mcp.server.fastmcp import FastMCP

DB_URL = os.environ.get("AGENT_BUS_DB", str(Path.home() / ".agent_bus.db"))
_USE_POSTGRES = DB_URL.startswith(("postgresql://", "postgres://"))

mcp = FastMCP("agent2agent")


# ─── SQLite backend ───────────────────────────────────────────────────────────

def _sqlite_init(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            name        TEXT PRIMARY KEY,
            task        TEXT,
            result      TEXT,
            published   INTEGER,
            started_at  INTEGER NOT NULL
        )
    """)
    conn.commit()


@contextmanager
def _sqlite():
    conn = sqlite3.connect(DB_URL, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _sqlite_init(conn)
    try:
        yield conn
    finally:
        conn.close()


# ─── Postgres backend ─────────────────────────────────────────────────────────

def _pg():
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as e:
        raise RuntimeError(
            "psycopg2 is required for Postgres backend: pip install psycopg2-binary"
        ) from e
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            name        TEXT PRIMARY KEY,
            task        TEXT,
            result      TEXT,
            published   BIGINT,
            started_at  BIGINT NOT NULL
        )
    """)
    cur.execute("""
        CREATE OR REPLACE FUNCTION notify_agent_published()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_notify('agent_published', NEW.name);
            RETURN NEW;
        END;
        $$
    """)
    cur.execute("""
        DROP TRIGGER IF EXISTS trg_agent_published ON agents
    """)
    cur.execute("""
        CREATE TRIGGER trg_agent_published
        AFTER INSERT OR UPDATE OF published ON agents
        FOR EACH ROW WHEN (NEW.published IS NOT NULL)
        EXECUTE FUNCTION notify_agent_published()
    """)
    conn.commit()
    return conn


# ─── unified ops ─────────────────────────────────────────────────────────────

def _upsert_start(name: str, task: str, now: int) -> None:
    if _USE_POSTGRES:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agents (name, task, result, published, started_at)
            VALUES (%s, %s, NULL, NULL, %s)
            ON CONFLICT (name) DO UPDATE SET
                task = EXCLUDED.task, result = NULL,
                published = NULL, started_at = EXCLUDED.started_at
        """, (name, task, now))
        conn.commit(); conn.close()
    else:
        with _sqlite() as conn:
            conn.execute("""
                INSERT INTO agents (name, task, result, published, started_at)
                VALUES (?, ?, NULL, NULL, ?)
                ON CONFLICT(name) DO UPDATE SET
                    task = excluded.task, result = NULL,
                    published = NULL, started_at = excluded.started_at
            """, (name, task, now))
            conn.commit()


def _upsert_publish(name: str, result: str, now: int) -> None:
    if _USE_POSTGRES:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agents (name, task, result, published, started_at)
            VALUES (%s, '', %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET
                result = EXCLUDED.result, published = EXCLUDED.published
        """, (name, result, now, now))
        conn.commit(); conn.close()
    else:
        with _sqlite() as conn:
            conn.execute("""
                INSERT INTO agents (name, task, result, published, started_at)
                VALUES (?, '', ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    result = excluded.result, published = excluded.published
            """, (name, result, now, now))
            conn.commit()


def _fetch_published(name: str) -> dict | None:
    if _USE_POSTGRES:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("SELECT result, published FROM agents WHERE name = %s", (name,))
        row = cur.fetchone(); conn.close()
        if row and row["published"] is not None:
            return {"result": row["result"], "published": row["published"]}
    else:
        with _sqlite() as conn:
            row = conn.execute(
                "SELECT result, published FROM agents WHERE name = ?", (name,)
            ).fetchone()
            if row and row["published"] is not None:
                return {"result": row["result"], "published": row["published"]}
    return None


def _fetch_all() -> list[dict]:
    if _USE_POSTGRES:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT name, task, result, published, started_at FROM agents ORDER BY started_at DESC"
        )
        rows = cur.fetchall(); conn.close()
        return [dict(r) for r in rows]
    else:
        with _sqlite() as conn:
            rows = conn.execute(
                "SELECT name, task, result, published, started_at FROM agents ORDER BY started_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]


def _delete_agent(name: str) -> int:
    if _USE_POSTGRES:
        conn = _pg()
        cur = conn.cursor()
        cur.execute("DELETE FROM agents WHERE name = %s", (name,))
        n = cur.rowcount; conn.commit(); conn.close()
        return n
    else:
        with _sqlite() as conn:
            n = conn.execute("DELETE FROM agents WHERE name = ?", (name,)).rowcount
            conn.commit()
            return n


async def _wait_for(name: str, timeout: int) -> dict | None:
    """Return published row dict or None on timeout."""
    deadline = time.time() + timeout

    if _USE_POSTGRES:
        # Use LISTEN/NOTIFY for push — no polling needed.
        import psycopg2
        listen_conn = psycopg2.connect(DB_URL)
        listen_conn.autocommit = True
        listen_cur = listen_conn.cursor()
        listen_cur.execute("LISTEN agent_published")
        try:
            # Check once before blocking in case it's already done.
            row = _fetch_published(name)
            if row:
                return row
            loop = asyncio.get_event_loop()
            while time.time() < deadline:
                remaining = max(0.0, deadline - time.time())
                # Wait up to 1 s at a time so we can check the deadline.
                ready = await loop.run_in_executor(
                    None,
                    lambda: _pg_poll(listen_conn, min(remaining, 1.0)),
                )
                if ready:
                    row = _fetch_published(name)
                    if row:
                        return row
        finally:
            listen_conn.close()
    else:
        while time.time() < deadline:
            row = _fetch_published(name)
            if row:
                return row
            await asyncio.sleep(0.5)

    return None


def _pg_poll(conn, timeout_secs: float) -> bool:
    import select
    r, _, _ = select.select([conn], [], [], timeout_secs)
    if r:
        conn.poll()
        return bool(conn.notifies)
    return False


# ─── tools ───────────────────────────────────────────────────────────────────

@mcp.tool()
async def agent_start(name: str, task: str = "") -> str:
    """Register this agent so others can see it's running.

    Call this at the START of your work. Optional but makes agent_status
    useful — other agents can see you exist before you publish.

    Args:
        name: Short unique name for this agent (e.g. "researcher", "writer").
        task: One-line description of what you're doing (shown in status).
    """
    now = int(time.time())
    _upsert_start(name, task, now)
    return json.dumps({
        "ok": True,
        "name": name,
        "task": task,
        "message": f"Registered as '{name}'. Call agent_publish(name='{name}', result=...) when done.",
    }, indent=2)


@mcp.tool()
async def agent_publish(name: str, result: str) -> str:
    """Publish your result so any agent waiting on you can proceed.

    Call this as your LAST action. Any agent blocked in agent_wait(name)
    will immediately unblock and receive result.

    Args:
        name:   Your agent name — must match what the waiting agent passes to agent_wait.
        result: Your output. Plain text or a JSON string.
    """
    now = int(time.time())
    _upsert_publish(name, result, now)
    return json.dumps({
        "ok": True,
        "name": name,
        "published_at": now,
        "message": f"Result published. Any agent waiting on '{name}' will now unblock.",
    }, indent=2)


@mcp.tool()
async def agent_wait(name: str, timeout: int = 300) -> str:
    """Block until the named agent publishes its result, then return it.

    Call this BEFORE doing any work that depends on another agent's output.
    With SQLite: polls every 0.5s (~250ms average latency).
    With Postgres: uses LISTEN/NOTIFY for instant wakeup (~0ms latency).

    Args:
        name:    The agent name to wait for (must match the name in agent_publish).
        timeout: Max seconds to wait before giving up (default 300 = 5 min).
    """
    row = await _wait_for(name, timeout)
    if row:
        return json.dumps({
            "ok": True,
            "name": name,
            "result": row["result"],
            "published_at": row["published"],
            "message": f"Agent '{name}' is done. Use the result above to continue your work.",
        }, indent=2)
    return json.dumps({
        "ok": False,
        "name": name,
        "error": "timeout",
        "waited_secs": timeout,
        "message": (
            f"Agent '{name}' did not publish within {timeout}s. "
            "Check agent_status() to see if it's still running, "
            "or call agent_wait again with a longer timeout."
        ),
    }, indent=2)


@mcp.tool()
async def agent_status() -> str:
    """List all agents — running, done, or timed out.

    Shows each agent's name, task description, whether it has published,
    and a preview of its result. Use this to check what's happening or
    to debug a stuck wait.
    """
    rows = _fetch_all()
    agents = []
    for r in rows:
        preview = (r.get("result") or "")
        if len(preview) > 150:
            preview = preview[:150] + "…"
        agents.append({
            "name": r["name"],
            "task": r.get("task") or "",
            "status": "done" if r["published"] else "running",
            "started_at": r["started_at"],
            "published_at": r["published"],
            "result_preview": preview,
        })
    return json.dumps({
        "agents": agents,
        "total": len(agents),
        "done": sum(1 for a in agents if a["status"] == "done"),
        "running": sum(1 for a in agents if a["status"] == "running"),
    }, indent=2)


@mcp.tool()
async def agent_clear(name: str) -> str:
    """Delete an agent's record so the name can be reused.

    Use before re-running a task with the same agent name, or to clean
    up stale records from previous sessions.
    """
    n = _delete_agent(name)
    if n:
        return json.dumps({"ok": True, "name": name, "message": f"Agent '{name}' cleared."})
    return json.dumps({"ok": False, "name": name, "message": f"No record found for '{name}'."})


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
