<p align="center">
  <img width="1024" height="340" alt="image" src="https://github.com/user-attachments/assets/32ed8985-841d-49c3-81f7-2aabc7c7c564" />
</p>

<p align="center">
  <strong>Persistent memory for AI coding agents</strong><br>
  <em>Agent-agnostic. Single binary. Zero dependencies.</em>
</p>

<p align="center">
  <a href="docs/INSTALLATION.md">Installation</a> &bull;
  <a href="docs/AGENT-SETUP.md">Agent Setup</a> &bull;
  <a href="docs/ARCHITECTURE.md">Architecture</a> &bull;
  <a href="docs/PLUGINS.md">Plugins</a> &bull;
  <a href="CONTRIBUTING.md">Contributing</a> &bull;
  <a href="DOCS.md">Full Docs</a>
</p>

---

> **engram** `/ˈen.ɡræm/` — *neuroscience*: the physical trace of a memory in the brain.

Your AI coding agent forgets everything when the session ends. Engram gives it a brain.

A **Go binary** with SQLite + FTS5 full-text search, exposed via CLI, HTTP API, MCP server, and an interactive TUI. Works with **any agent** that supports MCP — Claude Code, OpenCode, Gemini CLI, Codex, VS Code (Copilot), Antigravity, Cursor, Windsurf, or anything else.

```
Agent (Claude Code / OpenCode / Gemini CLI / Codex / VS Code / Antigravity / ...)
    ↓ MCP stdio
Engram (single Go binary)
    ↓
SQLite + FTS5 (~/.engram/engram.db)
```

## Quick Start

### Install

```bash
brew install gentleman-programming/tap/engram
```

Windows, Linux, and other install methods → [docs/INSTALLATION.md](docs/INSTALLATION.md)

### Setup Your Agent

| Agent | One-liner |
|-------|-----------|
| Claude Code | `claude plugin marketplace add Gentleman-Programming/engram && claude plugin install engram` |
| OpenCode | `engram setup opencode` |
| Gemini CLI | `engram setup gemini-cli` |
| Codex | `engram setup codex` |
| VS Code | `code --add-mcp '{"name":"engram","command":"engram","args":["mcp"]}'` |
| Cursor / Windsurf / Any MCP | See [docs/AGENT-SETUP.md](docs/AGENT-SETUP.md) |

Full per-agent config, Memory Protocol, and compaction survival → [docs/AGENT-SETUP.md](docs/AGENT-SETUP.md)

That's it. No Node.js, no Python, no Docker. **One binary, one SQLite file.**

## How It Works

```
1. Agent completes significant work (bugfix, architecture decision, etc.)
2. Agent calls mem_save → title, type, What/Why/Where/Learned
3. Engram persists to SQLite with FTS5 indexing
4. Next session: agent searches memory, gets relevant context
```

Full details on session lifecycle, topic keys, and memory hygiene → [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## MCP Tools (15)

| Category | Tools |
|----------|-------|
| **Save & Update** | `mem_save`, `mem_update`, `mem_delete`, `mem_suggest_topic_key` |
| **Search & Retrieve** | `mem_search`, `mem_context`, `mem_timeline`, `mem_get_observation` |
| **Session Lifecycle** | `mem_session_start`, `mem_session_end`, `mem_session_summary` |
| **Utilities** | `mem_save_prompt`, `mem_stats`, `mem_capture_passive`, `mem_merge_projects` |

Full tool reference with parameters → [DOCS.md#mcp-tools-15-tools](DOCS.md#mcp-tools-15-tools)

## Terminal UI

```bash
engram tui
```

<p align="center">
<img src="assets/tui-dashboard.png" alt="TUI Dashboard" width="400" />
  <img width="400" alt="image" src="https://github.com/user-attachments/assets/0308991a-58bb-4ad8-9aa2-201c059f8b64" />
  <img src="assets/tui-detail.png" alt="TUI Observation Detail" width="400" />
  <img src="assets/tui-search.png" alt="TUI Search Results" width="400" />
</p>

**Navigation**: `j/k` vim keys, `Enter` to drill in, `/` to search, `Esc` back. Catppuccin Mocha theme.

## Git Sync

Share memories across machines. Uses compressed chunks — no merge conflicts, no huge files.

Local SQLite remains the source of truth. Cloud integration is opt-in replication.

```bash
engram sync                    # Export new memories as compressed chunk
git add .engram/ && git commit -m "sync engram memories"
engram sync --import           # On another machine: import new chunks
engram sync --status           # Check sync status
```

Full sync documentation → [DOCS.md](DOCS.md)

## Cloud Integration (Opt-In Replication)

Cloud is optional. Local SQLite stays authoritative; cloud is replication/shared access only.

```bash
# 1) SERVER-SIDE runtime config (must be set BEFORE server startup)
# Option A (docker compose runtime): defaults are included in docker-compose.cloud.yml
# - ENGRAM_CLOUD_INSECURE_NO_AUTH=1 (browser/dashboard local demo mode)
# - ENGRAM_CLOUD_ALLOWED_PROJECTS=smoke-project
docker compose -f docker-compose.cloud.yml up -d

# Option B (source-run runtime): set both token + allowlist before `engram cloud serve`
ENGRAM_DATABASE_URL="postgres://engram:engram_dev@127.0.0.1:5433/engram_cloud?sslmode=disable" \
ENGRAM_JWT_SECRET="replace-with-32+-byte-random-secret" \
ENGRAM_CLOUD_TOKEN="your-token" \
ENGRAM_CLOUD_ALLOWED_PROJECTS="my-project" \
engram cloud serve

# For local insecure development only (disables bearer auth, but still enforces project allowlist):
# ENGRAM_CLOUD_INSECURE_NO_AUTH=1 ENGRAM_CLOUD_ALLOWED_PROJECTS="my-project" engram cloud serve

# 2) CLIENT-SIDE config for CLI sync calls
# Set cloud endpoint (writes ~/.engram/cloud.json)
# Option A (docker compose runtime): published :18080
engram cloud config --server http://127.0.0.1:18080

# Option B (source-run runtime): default :8080
# engram cloud config --server http://127.0.0.1:8080

# 3) Client auth config (env var; CLI does not persist token for you)
# compose default runs insecure local demo mode, so keep token unset:
# client sync preflight only requires the configured cloud server URL,
# so no client-side ENGRAM_CLOUD_INSECURE_NO_AUTH flag is required here.
# (if remote server enforces bearer auth, set ENGRAM_CLOUD_TOKEN)
unset ENGRAM_CLOUD_TOKEN
# source-run authenticated flow: use the same token value passed to `engram cloud serve`
# export ENGRAM_CLOUD_TOKEN="your-token"

# 4) Enroll an explicit project
engram cloud enroll smoke-project

# 5) Run cloud sync explicitly
engram sync --cloud --project smoke-project
engram sync --cloud --status --project smoke-project

# Note: cloud mode requires a single explicit --project scope.
# `engram sync --cloud --all` is intentionally blocked.
```

Deterministic failure reasons are surfaced across CLI and server (`/sync/status`):

- `blocked_unenrolled`
- `auth_required`
- `cloud_config_error`
- `policy_forbidden`
- `paused`
- `transport_failed`

Cloud preflight/config errors (for example missing or invalid configured server URL) surface as `cloud_config_error`.

Dashboard access note: compose smoke defaults to insecure local-dev mode (`ENGRAM_CLOUD_INSECURE_NO_AUTH=1`) so browser access to `/dashboard` works without extra auth. In authenticated mode, browser users can open `/dashboard/login`, paste the bearer token once, and continue with an HttpOnly dashboard session cookie.

Runtime toggles:

- `ENGRAM_CLOUD_SYNC=1` enables cloud transport for `engram sync`
- `ENGRAM_CLOUD_SERVER` overrides configured server URL at runtime
- `ENGRAM_CLOUD_TOKEN` provides auth token at runtime for authenticated client sync/server auth mode
- `ENGRAM_CLOUD_ALLOWED_PROJECTS` is server-side only and must be set before `engram cloud serve` (or in compose env)
- `ENGRAM_JWT_SECRET` must be explicitly set to a non-default value in authenticated cloud server mode (`ENGRAM_CLOUD_TOKEN` set)

Cloud runtime bind host:

- `ENGRAM_CLOUD_HOST` controls `engram cloud serve` bind host (default `127.0.0.1` for local safety)
- For containerized runtime/compose publishing, set `ENGRAM_CLOUD_HOST=0.0.0.0`

## CLI Reference

| Command | Description |
|---------|-------------|
| `engram setup [agent]` | Install agent integration |
| `engram serve [port]` | Start HTTP API (default: 7437) |
| `engram mcp` | Start MCP server (stdio) |
| `engram tui` | Launch terminal UI |
| `engram search <query>` | Search memories |
| `engram save <title> <msg>` | Save a memory |
| `engram timeline <obs_id>` | Chronological context |
| `engram context [project]` | Recent session context |
| `engram stats` | Memory statistics |
| `engram export [file]` | Export to JSON |
| `engram import <file>` | Import from JSON |
| `engram sync` | Git sync export/import |
| `engram cloud <subcommand>` | Opt-in cloud config/status/enrollment + cloud runtime (`serve`) |
| `engram projects list\|consolidate\|prune` | Manage project names |
| `engram obsidian-export` | Export to Obsidian vault (beta) |
| `engram version` | Show version |

Full CLI with all flags → [docs/ARCHITECTURE.md#cli-reference](docs/ARCHITECTURE.md#cli-reference)

## Documentation

| Doc | Description |
|-----|-------------|
| [Installation](docs/INSTALLATION.md) | All install methods + platform support |
| [Agent Setup](docs/AGENT-SETUP.md) | Per-agent configuration + Memory Protocol |
| [Architecture](docs/ARCHITECTURE.md) | How it works + MCP tools + project structure |
| [Plugins](docs/PLUGINS.md) | OpenCode & Claude Code plugin details |
| [Comparison](docs/COMPARISON.md) | Why Engram vs claude-mem |
| [Intended Usage](docs/intended-usage.md) | Mental model — how Engram is meant to be used |
| [Obsidian Brain](docs/beta/obsidian-brain.md) | Export memories as Obsidian knowledge graph (beta) |
| [Contributing](CONTRIBUTING.md) | Contribution workflow + standards |
| [Full Docs](DOCS.md) | Complete technical reference |

## License

MIT

---

**Inspired by [claude-mem](https://github.com/thedotmack/claude-mem)** — but agent-agnostic, simpler, and built different.

## Contributors

<a href="https://github.com/Gentleman-Programming/engram/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Gentleman-Programming/engram&max=100" />
</a>
