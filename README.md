# agent2agent

Decentralized agent-to-agent communication. Agents publish results and wait for each other — no broker, no cloud, no orchestrator required.

```
Agent A  →  agent_publish(name="researcher", result="…")
Agent B  →  agent_wait(name="researcher")   # blocks until A is done, then returns A's result
```

Works on a single laptop out of the box. Scales to remote teams by swapping the backend.

---

## Why this exists

The default way developers work with agents today is **parallel** — multiple Claude Code sessions open at once, sub-agents spawned mid-task, background jobs running alongside foreground work. But those agents have no way to talk to each other. If Agent B's work depends on Agent A's output, your options are:

- Serialize everything (kills the parallelism benefit), or
- Copy-paste results manually between sessions, or
- Build your own ad-hoc coordination layer every time.

agent2agent is that coordination layer, kept deliberately minimal. Five tools:

| Tool | What it does |
|------|-------------|
| `agent_start(name, task)` | Announce that you're running |
| `agent_publish(name, result)` | Broadcast your output |
| `agent_wait(name, timeout)` | Block until another agent publishes |
| `agent_status()` | See all agents and their states |
| `agent_clear(name)` | Reset a slot for reuse |

---

## vs. Google's Agent2Agent (A2A) protocol

Google's [A2A protocol](https://google.github.io/A2A/) solves a different problem: **how do remote agent services discover and call each other over HTTP**. It's designed for enterprise environments where agents run as separate hosted services behind auth layers.

agent2agent solves the problem A2A explicitly doesn't cover: **agents already running on the same machine (or sharing a backend) that need to hand off results**.

| | agent2agent | Google A2A |
|--|-------------|------------|
| **Model** | Shared state (publish/wait) | RPC (request/response) |
| **Transport** | SQLite, Postgres, or email | HTTPS + JSON-RPC |
| **Setup** | `uvx agent2agent` | Run agent servers + service discovery |
| **Blocking wait** | `agent_wait()` ✓ | Async callbacks only |
| **Works offline** | ✓ | ✗ |
| **Decentralized** | ✓ (no broker) | ✗ (requires agent card registry) |
| **Lines of code** | ~200 | — |

The key distinction: A2A assumes agents are **remote services**. agent2agent assumes agents are **parallel processes** that share a medium — whether that's a local file, a database, or a mailbox.

---

## Setup

### Option 1 — Local SQLite (default, zero config)

The simplest setup. State lives in `~/.agent_bus.db`. Every agent on the same machine shares it automatically.

```bash
uvx agent2agent           # run the MCP server
# or
pip install agent2agent
```

Add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "agent2agent": {
      "command": "uvx",
      "args": ["agent2agent"]
    }
  }
}
```

Override the DB path with an env var:

```json
{
  "mcpServers": {
    "agent2agent": {
      "command": "uvx",
      "args": ["agent2agent"],
      "env": { "AGENT_BUS_DB": "/shared/volume/agents.db" }
    }
  }
}
```

### Option 2 — Postgres (remote, push-based)

Point `AGENT_BUS_DB` at a Postgres connection string. All agents on any machine with DB credentials become peers. Uses `LISTEN`/`NOTIFY` internally — `agent_wait` wakes up **instantly** when another agent publishes, with near-zero latency instead of the 500ms poll cycle.

```bash
pip install "agent2agent[postgres]"
```

```json
{
  "mcpServers": {
    "agent2agent": {
      "command": "uvx",
      "args": ["agent2agent[postgres]"],
      "env": { "AGENT_BUS_DB": "postgresql://user:pass@host/agents" }
    }
  }
}
```

The schema is one table — easy to self-host on any Postgres instance (Supabase, Railway, your own VPS).

### Option 3 — Email (async, across any network)

For agents that don't share a filesystem or database, email works as a transport. An agent publishes by sending an email; the waiting agent polls its inbox. Latency is seconds rather than milliseconds, but it requires no shared infrastructure — just two email addresses.

Good for long-running async workflows where sub-second latency doesn't matter. Email backend is not yet built into this package; contributions welcome.

---

## Usage pattern

**Agent A** (producer):

```
agent_start(name="scraper", task="scraping product pages")
... do work ...
agent_publish(name="scraper", result=json.dumps({"products": [...]}))
```

**Agent B** (consumer, depends on A):

```
data = agent_wait(name="scraper")   # returns immediately if A already finished
... use data["result"] ...
agent_publish(name="summarizer", result="Done: 42 products found")
```

**You** (observer):

```
agent_status()   # shows all agents, tasks, and current state
```

---

## Data model

One table, five columns:

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

Communication latency: ~250ms average on SQLite (polls every 500ms), near-zero on Postgres (push via `LISTEN`/`NOTIFY`).

---

## Open problems

- **Fan-out**: `agent_wait` blocks on a single producer. `agent_wait_all(names=[...])` that unblocks when all named agents finish would be useful.
- **Pub/sub**: each name is a single slot — multiple consumers for the same result (broadcast) isn't supported.
- **Expiry**: records accumulate indefinitely. A TTL or `agent_clear_all()` would help in long-running setups.
- **Result streaming**: results are published atomically. No way to stream partial output from a running agent.
- **Email backend**: async transport for agents with no shared infrastructure — not yet implemented.
- **SQLite push**: SQLite backend still polls; filesystem-watch (kqueue/inotify) could drop SQLite latency to ~5ms.

---

## License

MIT
