[← Back to README](../README.md)

# Plugins

> Deferred scope note: plugin-level automatic cloud enrollment/login/upgrade orchestration is not part of this rollout yet. Current cloud flows are CLI-driven (`engram cloud ...`).
>
> Validation boundary (current): plugin scripts are validated for memory/session workflows, not as cloud bootstrap orchestrators. Use CLI for cloud config/auth/enrollment/upgrade.

- [OpenCode Plugin](#opencode-plugin)
- [Claude Code Plugin](#claude-code-plugin)
- [Privacy](#privacy)

---

## OpenCode Plugin

For [OpenCode](https://opencode.ai) users, a thin TypeScript plugin adds enhanced session management on top of the MCP tools:

```bash
# Install via engram (recommended — works from Homebrew or binary install)
engram setup opencode

# Or manually: cp plugin/opencode/engram.ts ~/.config/opencode/plugins/
```

The plugin auto-starts the HTTP server if it's not already running — no manual `engram serve` needed.

> **Local model compatibility:** The plugin works with all models, including local ones served via llama.cpp, Ollama, or similar. The Memory Protocol is concatenated into the existing system prompt (not added as a separate system message), so models with strict Jinja templates (Qwen, Mistral/Ministral) work correctly.

### What the Plugin Does

The plugin:
- **Auto-starts** the engram server if not running
- **Auto-imports** git-synced memories from `.engram/manifest.json` if present in the project
- **Creates sessions** on-demand via `ensureSession()` (resilient to restarts/reconnects)
- **Injects the Memory Protocol** into the agent's system prompt via `chat.system.transform` — strict rules for when to save, when to search, and a mandatory session close protocol. The protocol is concatenated into the existing system message (not pushed as a separate one), ensuring compatibility with models that only accept a single system block (Qwen, Mistral/Ministral via llama.cpp, etc.)
- **Injects previous session context** into the compaction prompt
- **Instructs the compressor** to tell the new agent to persist the compacted summary via `mem_session_summary`
- **Strips `<private>` tags** before sending data
- **Enables** `opencode-subagent-statusline` in `tui.json` or `tui.jsonc` during `engram setup opencode`, adding a live sub-agent monitor to OpenCode's sidebar/home footer. To disable it later, remove `"opencode-subagent-statusline"` from the `"plugin"` array in your TUI config and restart OpenCode.

**No raw tool call recording** — the agent handles all memory via `mem_save` and `mem_session_summary`.

### Memory Protocol (injected via system prompt)

The plugin injects a strict protocol into every agent message:

- **WHEN TO SAVE**: Mandatory after bugfixes, decisions, discoveries, config changes, patterns, preferences
- **WHEN TO SEARCH**: Reactive (user says "remember"/"recordar") + proactive (starting work that might overlap past sessions)
- **SESSION CLOSE**: Mandatory `mem_session_summary` before ending — "This is NOT optional. If you skip this, the next session starts blind."
- **AFTER COMPACTION**: Immediately call `mem_context` to recover state

### Three Layers of Memory Resilience

The OpenCode plugin uses a defense-in-depth strategy to ensure memories survive compaction:

| Layer | Mechanism | Survives Compaction? |
|-------|-----------|---------------------|
| **System Prompt** | `MEMORY_INSTRUCTIONS` concatenated into existing system prompt via `chat.system.transform` | Always present |
| **Compaction Hook** | Auto-saves checkpoint + injects context + reminds compressor | Fires during compaction |
| **Agent Config** | "After compaction, call `mem_context`" in agent prompt | Always present |

---

## Claude Code Plugin

For [Claude Code](https://docs.anthropic.com/en/docs/claude-code) users, a plugin adds enhanced session management using Claude's native hook and skill system:

```bash
# Install via Claude Code marketplace (recommended)
claude plugin marketplace add Gentleman-Programming/engram
claude plugin install engram

# Or via engram binary (works from Homebrew or binary install)
engram setup claude-code

# Or for local development/testing from the repo
claude --plugin-dir ./plugin/claude-code
```

### What the Plugin Provides (vs bare MCP)

| Feature | Bare MCP | Plugin |
|---------|----------|--------|
| MCP tools available | 17 default (`engram mcp`) | 13 agent-profile tools (`engram mcp --tools=agent`) |
| Session tracking (auto-start) | ✗ | ✓ |
| Auto-import git-synced memories | ✗ | ✓ |
| Compaction recovery | ✗ | ✓ |
| Memory Protocol skill | ✗ | ✓ |
| Previous session context injection | ✗ | ✓ |

### Plugin Structure

```
plugin/claude-code/
├── .claude-plugin/plugin.json     # Plugin manifest
├── .mcp.json                      # Registers engram MCP server
├── hooks/hooks.json               # SessionStart + SubagentStop + Stop lifecycle hooks
├── scripts/
│   ├── session-start.sh           # Ensures server, creates session, imports chunks, injects context
│   ├── post-compaction.sh         # Injects previous context + recovery instructions
│   ├── subagent-stop.sh           # Passive capture trigger on subagent completion
│   └── session-stop.sh            # Logs end-of-session event
└── skills/memory/SKILL.md         # Memory Protocol (when to save, search, close, recover)
```

### How It Works

**On session start** (`startup`):
1. Ensures the engram HTTP server is running
2. Creates a new session via the API
3. Auto-imports git-synced chunks from `.engram/manifest.json` (if present)
4. Injects previous session context into Claude's initial context

**On compaction** (`compact`):
1. Injects the previous session context + compacted summary
2. Tells the agent: "FIRST ACTION REQUIRED — call `mem_session_summary` with this content before doing anything else"
3. This ensures no work is lost when context is compressed

**Memory Protocol skill** (always available):
- Strict rules for **when to save** (mandatory after bugfixes, decisions, discoveries)
- **When to search** memory (reactive + proactive)
- **Session close protocol** — mandatory `mem_session_summary` before ending
- **After compaction** — 3-step recovery: persist summary → load context → continue

---

## MCP Tool Reference — mem_judge

`mem_judge` is available in the `agent` profile (`engram mcp --tools=agent`). It is NOT exposed in the `admin` profile.

### Purpose

Records a verdict on a pending memory conflict surfaced by `mem_save`. When `mem_save` returns `judgment_required: true`, the agent iterates `candidates[]` and calls `mem_judge` once per entry.

### Parameters

| Parameter | Required | Type | Description |
|-----------|----------|------|-------------|
| `judgment_id` | yes | string | From `candidates[].judgment_id` in the `mem_save` response. Format: `rel-<hex>`. |
| `relation` | yes | string | One of: `related`, `compatible`, `scoped`, `conflicts_with`, `supersedes`, `not_conflict` |
| `reason` | no | string | Free-text explanation of the verdict |
| `evidence` | no | string | Supporting evidence (JSON or free text; e.g., user's exact words) |
| `confidence` | no | float | 0.0..1.0 — default 1.0; clamped to range |
| `session_id` | no | string | Session ID for provenance (auto-detected if omitted) |

### Behavior

On success, `mem_judge`:
- Flips `judgment_status` from `pending` to `judged` on the matching `memory_relations` row
- Persists `relation`, `reason`, `evidence`, `confidence`, actor provenance (`actor="agent"`, `marked_by_kind="agent"`), and `session_id`
- Returns the updated relation row as JSON

On error (unknown `judgment_id` or invalid `relation`), returns `IsError: true`. The relation row is NOT mutated on error.

Re-judging an already-judged `judgment_id` overwrites the verdict (deliberate revision is allowed).

### Search annotation behavior (observed)

After a verdict is recorded, `mem_search` annotations surface as follows:

| Relation verdict | Annotation in `mem_search` results |
|-----------------|-----------------------------------|
| `supersedes` | `supersedes: #<id> (<title>)` on the source observation; `superseded_by: #<id> (<title>)` on the target |
| `supersedes` (target deleted) | `supersedes: #<id> (deleted)` — falls back to `(deleted)` when the related observation is missing |
| `conflicts_with` (judged) | `conflicts: #<id> (<title>)` on both observations — one line per conflict |
| `pending` (not yet judged) | `conflict: contested by #<id> (pending)` on both observations |
| `compatible`, `related`, `scoped`, `not_conflict` | No annotation line. Judgment is stored but not surfaced. |

### Annotation format contract (REQ-012)

The annotation format is a stable, versioned contract. Agent parsers use prefix-based matching — these prefixes will not change in Phase 3:

```
supersedes: #<id> (<title>)
superseded_by: #<id> (<title>)
conflicts: #<id> (<title>)
conflict: contested by #<id> (pending)
```

Multiple entries appear on separate lines (one per related observation), in query-return order. The `<title>` is retrieved via JOIN at search time (no N+1 queries). When the related observation has been deleted, `(deleted)` replaces the title.

> **Parser note**: match by prefix (`supersedes:`, `superseded_by:`, `conflicts:`, `conflict:`). The format `#<integer-id> (<title>)` within parentheses is stable. Do not attempt to parse the title itself — it may contain any characters.

### Cloud sync for judgments

When a project is enrolled in Engram Cloud and autosync is enabled, `mem_judge` verdicts sync across machines. The `memory_relations` table propagates via the standard mutation push/pull cycle — the same pipeline used for observations and sessions. Judgments appear in `mem_search` annotations on any machine that has pulled the relevant mutations.

Relations where the referenced observation does not yet exist locally are deferred (see `sync_apply_deferred`) and retried automatically on subsequent pull cycles.

### mem_save envelope fields (conflict surfacing)

When `mem_save` detects candidates, the JSON response includes:

| Field | Type | Description |
|-------|------|-------------|
| `judgment_required` | bool | `true` when candidates were found; `false` otherwise |
| `judgment_status` | string | `"pending"` (only present when `judgment_required: true`) |
| `judgment_id` | string | Convenience: the first candidate's `judgment_id` (use `candidates[].judgment_id` for multi-candidate loops) |
| `candidates` | array | Each entry has `id`, `sync_id`, `title`, `type`, `score`, `judgment_id`, and optionally `topic_key` |
| `id` | int | Internal ID of the just-saved observation |
| `sync_id` | string | Stable sync ID of the just-saved observation |

Old clients that read only the `result` string continue to work — these fields are additive.

---

## Privacy

Wrap sensitive content in `<private>` tags — it gets stripped at TWO levels:

```
Set up API with <private>sk-abc123</private> key
→ Set up API with [REDACTED] key
```

1. **Plugin layer** — stripped before data leaves the process
2. **Store layer** — `stripPrivateTags()` in Go before any DB write
