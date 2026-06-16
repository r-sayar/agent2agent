#!/usr/bin/env python3
"""agent2agent — decentralized agent-to-agent communication.

One agent publishes a named result; another blocks until it appears.

  Agent A (producer):   agent_publish(name="researcher", result="…")
  Agent B (consumer):   agent_wait(name="researcher")   ← blocks until A publishes

For broadcast messaging (all agents see the same messages):

  Any agent:            agent_send(channel="global", message="…")
  Any agent:            agent_recv(channel="global")   ← returns all new messages since last check

Backend is selected by AGENT_BUS_DB:
  ~/.agent_bus.db              → SQLite (default, local)
  /path/to/file.db             → SQLite at that path
  postgresql://user:pw@host/db → Postgres (remote, push via LISTEN/NOTIFY)
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from contextlib import contextmanager
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            channel     TEXT NOT NULL,
            sender      TEXT NOT NULL DEFAULT '',
            content     TEXT NOT NULL,
            created_at  INTEGER NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel, id)")
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
        CREATE TABLE IF NOT EXISTS messages (
            id          BIGSERIAL PRIMARY KEY,
            channel     TEXT NOT NULL,
            sender      TEXT NOT NULL DEFAULT '',
            content     TEXT NOT NULL,
            created_at  BIGINT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel, id)")
    cur.execute("""
        CREATE OR REPLACE FUNCTION notify_agent_published()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_notify('agent_published', NEW.name);
            RETURN NEW;
        END;
        $$
    """)
    cur.execute("DROP TRIGGER IF EXISTS trg_agent_published ON agents")
    cur.execute("""
        CREATE TRIGGER trg_agent_published
        AFTER INSERT OR UPDATE OF published ON agents
        FOR EACH ROW WHEN (NEW.published IS NOT NULL)
        EXECUTE FUNCTION notify_agent_published()
    """)
    conn.commit()
    return conn


# ─── unified ops — agents ─────────────────────────────────────────────────────

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
    deadline = time.time() + timeout
    if _USE_POSTGRES:
        import psycopg2
        listen_conn = psycopg2.connect(DB_URL)
        listen_conn.autocommit = True
        listen_cur = listen_conn.cursor()
        listen_cur.execute("LISTEN agent_published")
        try:
            row = _fetch_published(name)
            if row:
                return row
            loop = asyncio.get_event_loop()
            while time.time() < deadline:
                remaining = max(0.0, deadline - time.time())
                ready = await loop.run_in_executor(
                    None, lambda: _pg_poll(listen_conn, min(remaining, 1.0))
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


# ─── unified ops — messages ───────────────────────────────────────────────────

def _insert_message(channel: str, sender: str, content: str, now: int) -> int:
    if _USE_POSTGRES:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO messages (channel, sender, content, created_at) VALUES (%s, %s, %s, %s) RETURNING id",
            (channel, sender, content, now),
        )
        msg_id = cur.fetchone()["id"]
        conn.commit(); conn.close()
        return msg_id
    else:
        with _sqlite() as conn:
            cur = conn.execute(
                "INSERT INTO messages (channel, sender, content, created_at) VALUES (?, ?, ?, ?)",
                (channel, sender, content, now),
            )
            conn.commit()
            return cur.lastrowid


def _fetch_messages(channel: str, since_id: int) -> list[dict]:
    if _USE_POSTGRES:
        conn = _pg()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, sender, content, created_at FROM messages WHERE channel = %s AND id > %s ORDER BY id",
            (channel, since_id),
        )
        rows = cur.fetchall(); conn.close()
        return [dict(r) for r in rows]
    else:
        with _sqlite() as conn:
            rows = conn.execute(
                "SELECT id, sender, content, created_at FROM messages WHERE channel = ? AND id > ? ORDER BY id",
                (channel, since_id),
            ).fetchall()
            return [dict(r) for r in rows]


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


@mcp.tool()
async def agent_send(channel: str, message: str, sender: str = "") -> str:
    """Broadcast a message to a channel. All agents reading that channel will see it.

    Unlike agent_publish (one slot, one result), messages accumulate — every
    agent that calls agent_recv gets ALL messages it hasn't seen yet.

    Use channel="global" for messages meant for every agent.

    Args:
        channel: Channel name (e.g. "global", "team-a", "alerts").
        message: The message content. Plain text or JSON string.
        sender:  Optional name identifying who sent this (e.g. your agent name).
    """
    now = int(time.time())
    msg_id = _insert_message(channel, sender, message, now)
    return json.dumps({
        "ok": True,
        "id": msg_id,
        "channel": channel,
        "message": f"Message #{msg_id} sent to channel '{channel}'. Agents calling agent_recv(channel='{channel}') will receive it.",
    }, indent=2)


@mcp.tool()
async def agent_recv(channel: str, since_id: int = 0) -> str:
    """Fetch all new messages on a channel since a given message ID.

    Non-blocking. Returns immediately with whatever is there.
    Call this at the start of each turn to check for messages from other agents.

    To follow a channel across turns: save the highest 'id' returned and pass
    it as since_id next time. Start with since_id=0 to get all history.

    Args:
        channel:  Channel to read (e.g. "global").
        since_id: Only return messages with id > this value (default 0 = all).
    """
    rows = _fetch_messages(channel, since_id)
    return json.dumps({
        "ok": True,
        "channel": channel,
        "messages": [
            {
                "id": r["id"],
                "sender": r["sender"],
                "content": r["content"],
                "created_at": r["created_at"],
            }
            for r in rows
        ],
        "count": len(rows),
        "next_since_id": rows[-1]["id"] if rows else since_id,
        "tip": f"Pass since_id={rows[-1]['id'] if rows else since_id} next call to get only new messages.",
    }, indent=2)


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
