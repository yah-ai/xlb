# /yah-foreign — Orient a foreign agent in a yah-managed workspace

You're in a workspace that yah manages, but your harness wasn't launched
through yah — so the `mcp__yah__*` and `board.*` MCP tools aren't loaded.
This skill gives you the same context a yah-aware session gets, plus the
CLI fallbacks for everything yah-aware sessions do via MCP.

## What yah is, in one sentence

yah is an AI-agent harness with a source-embedded ticket system (the
"hack-board"). Tickets live as `@yah:` doc-comment annotations in code or
HTML comments in markdown — there's no separate issue tracker. The `yah`
CLI is the canonical interface; everything else (MCP tools, the kanban
UI) is a surface over the same commands.

## First things to check

```bash
yah --version                    # confirm the binary is on PATH
yah board status                 # 1-screen camp summary: counts, active owners
yah board ready                  # tickets with deps satisfied — usually your starting point
```

If `yah` isn't on PATH, the workspace probably has a local build at
`./target/debug/yah` or `./target/release/yah`. Use that.

## The rules + per-ticket pickup prompt

The full SDLC isn't pasted here — fetch it on demand:

```bash
yah board rules                                # Rule01–Rule12 + Col01 (canonical)
yah board rules --context pickup               # narrow to a situation
yah board prompt                               # list embedded prompts (refine/handoff/comment)
yah board prompt rules                         # same rules, prompt-formatted for an agent
yah board tickets --prompt <ID>                # pickup prompt for one ticket (rules + context)
```

Read `yah board rules` once at the start of any board-touching work.

## Key CLI verbs (mirror of the MCP tool names)

| Verb | Purpose | MCP name |
|---|---|---|
| `yah board status` | Counts + active owners + smell warnings | `board.status` |
| `yah board show <ID>` | One ticket's full state (deps, verify, history) | `board.show` |
| `yah board ready` | Tickets whose deps are satisfied | `board.ready` |
| `yah board inflight` | Open + Active + Handoff (skips Review) | `board.inflight` |
| `yah board tickets` | All tickets, filterable | `board.list_tickets` |
| `yah board open --kind <K> --parent <ID> --file <F> --title …` | File a new ticket (Open column) | `board.open` |
| `yah board claim <ID>` or `claim --kind … --parent …` | Start work (sets in-progress + assignee) | `board.claim` |
| `yah board move <ID> <column>` | Transition: open/active/handoff/review | `board.move` |
| `yah board update <ID> …` | Edit annotation fields in place | `board.update` |
| `yah board archive <ID>` | Strip annotation + log to events.jsonl | `board.archive` |
| `yah board summary --ticket <ID> --text …` | Pin a progress note to a card | `board.summary` |
| `yah board adopt --file <PATH>` | Resolve ID collisions after hand-writing `@yah:` annotations | `board.adopt` |

Read tools (status / show / ready / inflight / tickets) don't need
approval. Write tools (open / claim / move / update / archive / summary /
adopt) modify source — review the diff in `git status` after each.

## Slash commands available

The same templates are installed at `.claude/commands/`:

- `/refine` — turn a multi-phase plan into a relay + sub-tickets
- `/handoff` — write a structured relay handoff for the next agent
- `/comment` — log a progress summary to `.yah/summaries/`

If they're not registered in your harness, their bodies are reachable as:

```bash
yah board prompt refine
yah board prompt handoff
yah board prompt comment
```

## Two semantic rules the CLI help doesn't surface

- **Moving into handoff:** update `@yah:handoff(...)` and `@yah:next(...)`
  in source *first*, then `yah board move <ID> handoff`. The baton moves
  with the source, not the card.
- **Never pick IDs yourself.** Always go through `yah board open` /
  `yah board claim` — they file-lock, scan for the next free ID, and
  write the annotation atomically. Two agents racing on the same R-number
  is the failure mode this prevents.

## Output convention

When you reference a file or symbol, prefer markdown links with the
`yah://` scheme over bare paths in chat output — yah-aware UIs render
them as clickable affordances:

- `[path/to/file.rs:42](yah://file/path/to/file.rs#L42)`
- `[Foo](yah://arch/symbol/Foo)`

Bare backticked paths still work; yah:// is just the upgrade.

## When in doubt

```bash
yah help                         # top-level
yah board --help                 # all board subcommands
yah board <subcommand> --help    # flags for one verb
yah board prompt                 # list embedded agent prompts
```

The CLI is the source of truth — if a subcommand's `--help` disagrees
with this skill, trust `--help`.
