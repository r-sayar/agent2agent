# agent-bus

A tiny MCP server that lets parallel Claude agents signal completion and wait for each other — on a single machine, with no infrastructure.

```
Agent A  →  agent_publish(name="researcher", result="…")
Agent B  →  agent_wait(name="researcher")   # blocks until A is done, then returns A's result
```

State lives in a local SQLite file (`~/.agent_bus.db`). No Redis, no broker, no cloud service.

---

## Why this exists

Everyone building multi-agent systems is focused on the **cloud orchestration** problem: how do you fan out work across remote workers, managed queues, and hosted APIs? A2A, CrewAI, AutoGen — they're all solving that.

Nobody is focused on the **local parallel** problem.

When you run `claude --dangerously-skip-permissions` in a few terminal tabs, or use Claude Code's sub-agent spawning, you get real parallelism — multiple Claude processes running on your machine at the same time. This is increasingly the default way developers work with agents. But those agents have no way to talk to each other. If Agent B's work depends on Agent A's output, you either:

- Serialize everything (kills the parallelism benefit), or
- Copy-paste results manually between sessions, or
- Build your own ad-hoc coordination layer every time.

**agent-bus** is that coordination layer, kept deliberately minimal. It's a synchronization primitive, not a framework. You get five tools:

| Tool | What it does |
|------|-------------|
| `agent_start(name, task)` | Announce that you're running |
| `agent_publish(name, result)` | Broadcast your output |
| `agent_wait(name, timeout)` | Block until another agent publishes |
| `agent_status()` | See all agents and their states |
| `agent_clear(name)` | Reset a slot for reuse |

---

## Comparison with alternatives

| | agent-bus | A2A | CrewAI / AutoGen | Claude Workflows |
|--|-----------|-----|-----------------|-----------------|
| **Target** | Local parallel agents | Remote agent-to-agent | Orchestrated multi-agent | Hosted cloud |
| **Transport** | SQLite file | HTTP | In-process | Cloud API |
| **Setup** | `uvx agent-bus` | Full server + certs | Python framework install | Subscription |
| **Wait primitive** | `agent_wait()` ✓ | Async callbacks | No | No |
| **Works offline** | ✓ | ✗ | ✓ | ✗ |
| **Lines of code** | ~200 | — | — | — |

The key insight: **A2A and similar protocols assume agents are remote services**. They're designed for service discovery, auth, and async messaging over HTTP. That's the right tool when you're coordinating agents across machines or vendors. But for the common case of "I spawned three Claude tabs and they need to share results," it's massive overkill. agent-bus is a `wait()`/`notify()` for LLM agents on a laptop.

---

## Install

```bash
# Run directly with uvx (no install needed)
uvx agent-bus

# Or install
pip install agent-bus
```

## Add to Claude Code

Add to your `~/.claude/settings.json` (or project `.claude/settings.json`):

```json
{
  "mcpServers": {
    "agent-bus": {
      "command": "uvx",
      "args": ["agent-bus"]
    }
  }
}
```

Or if installed locally:

```json
{
  "mcpServers": {
    "agent-bus": {
      "command": "python",
      "args": ["/path/to/agent-bus/server.py"]
    }
  }
}
```

---

## Usage pattern

**Agent A** (runs first, produces a result):

```
agent_start(name="scraper", task="scraping product pages")
... do work ...
agent_publish(name="scraper", result=json.dumps({"products": [...]}))
```

**Agent B** (depends on A's output):

```
data = agent_wait(name="scraper")   # returns immediately if A already finished
... use data["result"] ...
agent_publish(name="summarizer", result="Done: 42 products found")
```

**You** (watching from the outside):

```
agent_status()   # shows both agents, their tasks, and current state
```

---

## Data model

One SQLite table, five columns:

```sql
CREATE TABLE agents (
    name        TEXT PRIMARY KEY,
    task        TEXT,
    result      TEXT,
    published   INTEGER,   -- unix timestamp; NULL = still running
    started_at  INTEGER NOT NULL
)
```

Default DB path: `~/.agent_bus.db`. Override with `AGENT_BUS_DB=/path/to/file.db`.

---

## Open problems

- **Fan-out**: `agent_wait` blocks on a single producer. A `agent_wait_all(names=[...])` that unblocks when all named agents finish would be useful.
- **Pub/sub channels**: right now each name is a single slot. Multiple consumers for the same result (broadcast) isn't supported.
- **Expiry**: records accumulate indefinitely. A TTL or `agent_clear_all()` would help in long-running setups.
- **Result streaming**: results are published atomically. There's no way to stream partial output from a running agent.

---

## License

MIT
