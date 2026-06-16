# agent2agent

Decentralized agent-to-agent communication for parallel Claude agents. No broker, no cloud, no orchestrator.

```
pip install agent2agent
```

---

## Setup (one-time)

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

Restart Claude. Done. `uvx` ships with Claude Code — no separate install needed. Every Claude session on your machine now shares a bus at `~/.agent_bus.db`.

---

## Capabilities

Seven tools across two communication patterns.

### Pattern 1 — Point-to-point sync

One agent produces a result. Another blocks until it's ready.

| Tool | What it does |
|------|-------------|
| `agent_start(name, task)` | Announce you're running so others can see you in status |
| `agent_publish(name, result)` | Post your result — any agent waiting on you unblocks instantly |
| `agent_wait(name, timeout)` | Block until the named agent publishes, then return its result |
| `agent_status()` | List all agents, their tasks, and whether they've finished |
| `agent_clear(name)` | Delete a slot so the name can be reused |

**Example — researcher feeds writer:**

```
# Session A (researcher)
agent_start(name="researcher", task="finding competitors")
... do research ...
agent_publish(name="researcher", result="Competitor list: ...")

# Session B (writer) — can start before or after A
agent_wait(name="researcher")   ← blocks here until A publishes
... write the report using the result ...
```

**Example — fan-in: wait for multiple agents:**

```
# Session C (orchestrator) — waits for both to finish
result_a = agent_wait(name="agent-a")
result_b = agent_wait(name="agent-b")
... combine both results ...
```

Latency: ~250ms average (polls every 500ms on SQLite, instant on Postgres).

---

### Pattern 2 — Broadcast channels

Any agent sends to a named channel. Every agent that reads that channel gets every message — messages are not consumed on read, so multiple readers each get their own copy.

| Tool | What it does |
|------|-------------|
| `agent_send(channel, message, sender)` | Post a message to a channel |
| `agent_recv(channel, since_id)` | Fetch all new messages since your last check |

**Example — status updates all agents can see:**

```
# Any agent — announce progress
agent_send(channel="global", message="I finished the scraping step", sender="scraper")

# Any other agent — check what's happening
agent_recv(channel="global", since_id=0)
# → returns all messages on the channel

# Next check — only get new ones
agent_recv(channel="global", since_id=42)   # 42 = last id you saw
```

**Example — asking a specific agent a question:**

```
# Session A — ask
agent_send(channel="main-to-sheriff", message="What did you find?", sender="main")

# Session B (sheriff) — check and reply
agent_recv(channel="main-to-sheriff", since_id=0)
agent_send(channel="sheriff-to-main", message="Found 3 issues: ...", sender="sheriff")

# Session A — receive answer
agent_recv(channel="sheriff-to-main", since_id=0)
```

---

## When to use which pattern

| Situation | Use |
|-----------|-----|
| Agent B needs Agent A's output before it can start | `publish` / `wait` |
| You want to see what all agents are doing | `status` |
| One agent wants to message another without knowing if it's ready | `send` / `recv` |
| Broadcast an update to all running agents | `send` to `"global"` channel |
| Two agents having a back-and-forth | `send` / `recv` on named channels |

---

## Remote agents (shared backend)

By default state lives at `~/.agent_bus.db` — local only. To connect agents across machines, point at a shared backend:

**Shared Postgres** (any agent with DB credentials can participate):
```json
{
  "mcpServers": {
    "agent2agent": {
      "command": "agent2agent",
      "env": { "AGENT_BUS_DB": "postgresql://user:pass@host/agents" }
    }
  }
}
```
Install Postgres support: `pip install "agent2agent[postgres]"`

Postgres uses `LISTEN`/`NOTIFY` — `agent_wait` wakes up instantly instead of polling.

**Shared file path** (agents on the same network volume):
```json
{
  "env": { "AGENT_BUS_DB": "/Volumes/shared/agents.db" }
}
```

---

## vs. Google's A2A protocol

Google's [A2A](https://google.github.io/A2A/) solves remote service discovery over HTTP — agents as hosted microservices calling each other via JSON-RPC. Right tool for cross-org, cross-vendor agent coordination.

agent2agent solves the local parallel problem A2A doesn't cover: multiple Claude sessions already running on the same machine (or sharing a backend) that need to hand off results and stay in sync.

| | agent2agent | Google A2A |
|--|-------------|------------|
| Model | Shared state (publish/wait + channels) | RPC (request/response) |
| Transport | SQLite or Postgres | HTTPS + JSON-RPC |
| Setup | One line in settings.json | Run agent servers + service discovery |
| Blocking wait | `agent_wait()` ✓ | Async callbacks only |
| Broadcast | `agent_send/recv` ✓ | ✗ |
| Works offline | ✓ | ✗ |
| Decentralized | ✓ no broker | ✗ needs agent card registry |

---

## Open problems

- **Fan-out**: `agent_wait_all(names=[...])` — unblock when all named agents finish
- **Expiry**: records accumulate; need TTL or `agent_clear_all()`
- **Result streaming**: results publish atomically; no partial/streaming output
- **Email backend**: async transport for agents with no shared infrastructure

---

## License

MIT
