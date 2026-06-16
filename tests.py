#!/usr/bin/env python3
"""agent2agent test suite."""
import asyncio
import json
import os
import sys
import tempfile
import time

_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["AGENT_BUS_DB"] = _tmp.name

sys.path.insert(0, os.path.dirname(__file__))
from server import (
    agent_clear, agent_publish, agent_recv, agent_send,
    agent_start, agent_status, agent_wait,
)

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
results = []

def check(name, cond, detail=""):
    status = PASS if cond else FAIL
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
    results.append(cond)

def j(coro):
    return json.loads(asyncio.run(coro))

# ─── 1. agent_start ──────────────────────────────────────────────────────────
print("\n1. agent_start")
r = j(agent_start(name="alpha", task="testing"))
check("ok=True", r["ok"] is True)
check("name echoed", r["name"] == "alpha")
asyncio.run(agent_publish(name="alpha", result="old"))
r2 = j(agent_start(name="alpha", task="retrying"))
check("re-register clears old result", r2["ok"] is True)

# ─── 2. agent_publish ────────────────────────────────────────────────────────
print("\n2. agent_publish")
r = j(agent_publish(name="beta", result="hello world"))
check("ok=True", r["ok"] is True)
check("name echoed", r["name"] == "beta")
check("published_at set", isinstance(r["published_at"], int))

# ─── 3. agent_wait — already published ───────────────────────────────────────
print("\n3. agent_wait (already published)")
t0 = time.time()
r = j(agent_wait(name="beta", timeout=5))
elapsed = time.time() - t0
check("ok=True", r["ok"] is True)
check("result correct", r["result"] == "hello world")
check("returns fast (<1s)", elapsed < 1.0, f"{elapsed:.2f}s")

# ─── 4. agent_wait — blocks until publish ────────────────────────────────────
print("\n4. agent_wait (blocks, then unblocks)")

async def _block_test():
    async def producer():
        await asyncio.sleep(0.9)
        await agent_publish(name="gamma", result="unblocked!")
    async def consumer():
        t0 = time.time()
        r = json.loads(await agent_wait(name="gamma", timeout=5))
        return r, time.time() - t0
    consumer_task = asyncio.create_task(consumer())
    await producer()
    return await consumer_task

r, elapsed = asyncio.run(_block_test())
check("ok=True", r["ok"] is True)
check("result correct", r["result"] == "unblocked!")
check("waited ~0.9s (0.7–1.5)", 0.7 < elapsed < 1.5, f"{elapsed:.2f}s")

# ─── 5. agent_wait — timeout ─────────────────────────────────────────────────
print("\n5. agent_wait (timeout)")
t0 = time.time()
r = j(agent_wait(name="nonexistent", timeout=1))
elapsed = time.time() - t0
check("ok=False", r["ok"] is False)
check("error=timeout", r["error"] == "timeout")
check("took ~1s", 0.9 < elapsed < 2.0, f"{elapsed:.2f}s")

# ─── 6. agent_status ─────────────────────────────────────────────────────────
print("\n6. agent_status")
r = j(agent_status())
check("returns agents list", isinstance(r["agents"], list))
check("total >= 3", r["total"] >= 3)
names = [a["name"] for a in r["agents"]]
check("beta in agents", "beta" in names)
check("gamma in agents", "gamma" in names)
beta = next(a for a in r["agents"] if a["name"] == "beta")
check("beta status=done", beta["status"] == "done")
alpha = next(a for a in r["agents"] if a["name"] == "alpha")
check("alpha status=running (re-registered)", alpha["status"] == "running")

# ─── 7. agent_clear ──────────────────────────────────────────────────────────
print("\n7. agent_clear")
r = j(agent_clear(name="beta"))
check("ok=True", r["ok"] is True)
r2 = j(agent_clear(name="beta"))
check("clearing missing → ok=False", r2["ok"] is False)
r3 = j(agent_status())
check("beta removed from status", "beta" not in [a["name"] for a in r3["agents"]])

# ─── 8. concurrent producers ─────────────────────────────────────────────────
print("\n8. concurrent producers (fan-in)")

async def _fanin():
    await asyncio.gather(
        agent_publish(name="worker-1", result="result-1"),
        agent_publish(name="worker-2", result="result-2"),
        agent_publish(name="worker-3", result="result-3"),
    )
    r1 = json.loads(await agent_wait(name="worker-1", timeout=2))
    r2 = json.loads(await agent_wait(name="worker-2", timeout=2))
    r3 = json.loads(await agent_wait(name="worker-3", timeout=2))
    return r1, r2, r3

r1, r2, r3 = asyncio.run(_fanin())
check("worker-1 ok", r1["ok"] and r1["result"] == "result-1")
check("worker-2 ok", r2["ok"] and r2["result"] == "result-2")
check("worker-3 ok", r3["ok"] and r3["result"] == "result-3")

# ─── 9. agent_send / agent_recv ──────────────────────────────────────────────
print("\n9. agent_send / agent_recv (broadcast channel)")

r = j(agent_send(channel="global", message="hello everyone", sender="agent-a"))
check("send ok=True", r["ok"] is True)
check("id is int", isinstance(r["id"], int))
first_id = r["id"]

j(agent_send(channel="global", message="second message", sender="agent-b"))
j(agent_send(channel="other",  message="different channel", sender="agent-c"))

r = j(agent_recv(channel="global", since_id=0))
check("recv ok=True", r["ok"] is True)
check("got 2 messages on global", r["count"] == 2)
check("first message content", r["messages"][0]["content"] == "hello everyone")
check("first message sender", r["messages"][0]["sender"] == "agent-a")
check("second message content", r["messages"][1]["content"] == "second message")

r2 = j(agent_recv(channel="global", since_id=first_id))
check("since_id filters correctly (1 new)", r2["count"] == 1)
check("filtered message is second", r2["messages"][0]["content"] == "second message")

r3 = j(agent_recv(channel="global", since_id=r2["next_since_id"]))
check("next_since_id advances correctly (0 new)", r3["count"] == 0)

r4 = j(agent_recv(channel="other", since_id=0))
check("other channel isolated (1 message)", r4["count"] == 1)
check("other channel content correct", r4["messages"][0]["content"] == "different channel")

# ─── 10. multiple agents read same message ───────────────────────────────────
print("\n10. broadcast: multiple readers get same message")
j(agent_send(channel="broadcast-test", message="for everyone", sender="orchestrator"))

ra = j(agent_recv(channel="broadcast-test", since_id=0))
rb = j(agent_recv(channel="broadcast-test", since_id=0))
check("agent-a received broadcast", ra["count"] == 1)
check("agent-b received same broadcast", rb["count"] == 1)
check("same message id", ra["messages"][0]["id"] == rb["messages"][0]["id"])

# ─── 11. AGENT_BUS_DB env var ────────────────────────────────────────────────
print("\n11. AGENT_BUS_DB env var")
check("custom DB path used", os.environ["AGENT_BUS_DB"] == _tmp.name)
check("DB file exists", os.path.exists(_tmp.name))

# ─── summary ─────────────────────────────────────────────────────────────────
passed = sum(results)
total = len(results)
print(f"\n{'='*40}")
if passed == total:
    print(f"\033[32mall {total} checks passed\033[0m")
else:
    print(f"\033[31m{passed}/{total} passed\033[0m")

os.unlink(_tmp.name)
sys.exit(0 if passed == total else 1)
