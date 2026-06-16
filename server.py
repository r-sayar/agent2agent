#!/usr/bin/env python3
"""agent_bus — lets Claude agents signal completion and wait for each other.

One agent publishes a named result; another blocks until it appears.

  Agent A (producer):   agent_publish(name="researcher", result="…")
  Agent B (consumer):   agent_wait(name="researcher")   ← blocks until A publishes

The DB lives at AGENT_BUS_DB (default: ~/.agent_bus.db) so state persists
across restarts and is visible to all agents on the machine.
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

DB_PATH = os.environ.get("AGENT_BUS_DB", str(Path.home() / ".agent_bus.db"))

mcp = FastMCP("agent-bus")


# ─── store ───────────────────────────────────────────────────────────────────

def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            name        TEXT PRIMARY KEY,
            task        TEXT,
            result      TEXT,
            published   INTEGER,   -- unix timestamp when result was published; NULL = still running
            started_at  INTEGER NOT NULL
        )
    """)
    conn.commit()


@contextmanager
def _db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _init_db(conn)
    try:
        yield conn
    finally:
        conn.close()


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
    with _db() as conn:
        conn.execute("""
            INSERT INTO agents (name, task, result, published, started_at)
            VALUES (?, ?, NULL, NULL, ?)
            ON CONFLICT(name) DO UPDATE SET
                task       = excluded.task,
                result     = NULL,
                published  = NULL,
                started_at = excluded.started_at
        """, (name, task, now))
        conn.commit()
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
    with _db() as conn:
        conn.execute("""
            INSERT INTO agents (name, task, result, published, started_at)
            VALUES (?, '', ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                result    = excluded.result,
                published = excluded.published
        """, (name, result, now, now))
        conn.commit()
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
    Polls internally every 0.5 s — from your perspective it's a single
    blocking call that returns the moment the other agent is done.

    Args:
        name:    The agent name to wait for (must match the name in agent_publish).
        timeout: Max seconds to wait before giving up (default 300 = 5 min).
    """
    deadline = time.time() + timeout
    while True:
        with _db() as conn:
            row = conn.execute(
                "SELECT result, published FROM agents WHERE name = ?", (name,)
            ).fetchone()
        if row and row["published"] is not None:
            return json.dumps({
                "ok": True,
                "name": name,
                "result": row["result"],
                "published_at": row["published"],
                "message": f"Agent '{name}' is done. Use the result above to continue your work.",
            }, indent=2)
        if time.time() >= deadline:
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
        await asyncio.sleep(0.5)


@mcp.tool()
async def agent_status() -> str:
    """List all agents — running, done, or timed out.

    Shows each agent's name, task description, whether it has published,
    and a preview of its result. Use this to check what's happening or
    to debug a stuck wait.
    """
    with _db() as conn:
        rows = conn.execute(
            "SELECT name, task, result, published, started_at FROM agents ORDER BY started_at DESC"
        ).fetchall()
    agents = []
    for r in rows:
        preview = (r["result"] or "")
        if len(preview) > 150:
            preview = preview[:150] + "…"
        agents.append({
            "name": r["name"],
            "task": r["task"] or "",
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
    with _db() as conn:
        n = conn.execute("DELETE FROM agents WHERE name = ?", (name,)).rowcount
        conn.commit()
    if n:
        return json.dumps({"ok": True, "name": name, "message": f"Agent '{name}' cleared."})
    return json.dumps({"ok": False, "name": name, "message": f"No record found for '{name}'."})


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
