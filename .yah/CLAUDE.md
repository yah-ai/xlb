<!-- Managed by `yah skills install` — full-file overwrite on each run. Edit the template in app/yah/cli/templates/claude-md-yah.md, not this copy. -->

## Output conventions

When you reference a file, function, or symbol the user might want to jump to, prefer markdown links with the `yah://` scheme over bare paths:

- `[path/to/file.rs:42](yah://file/path/to/file.rs#L42)` — opens the file in the Architecture tab rooted at that line.
- `[Foo](yah://arch/symbol/Foo)` — re-roots the arch graph on the named symbol.

The renderer turns these into clickable affordances; bare backticked `path:line` chips also work but yah:// links are preferred for prose.

## Board tools

Board MCP tools are namespaced `board.*` (dots, not underscores) — call them directly when present in your tool list; fall back to `yah board …` via Bash otherwise. The tool schemas describe their own arguments — trust those over any table.

For lifecycle, rules, and per-ticket pickup context, call `board.ticket_prompt` (or `yah board tickets --prompt <ID>`) — the prompt embeds the full SDLC for that ticket. `board.rules` returns the canonical Rule01–Rule12 + Col01 ruleset on demand. This `.yah/CLAUDE.md` deliberately stays out of the SDLC details so unanchored chat sessions don't pay for ticket-only context.

Two semantic rules the schemas can't tell you:

- **Move into `handoff`:** update `@yah:handoff(...)` and `@yah:next(...)` annotations in source *first*, then call `board.move {"id": "<ID>", "to_bucket": "handoff"}`. The baton moves with the source, not the card.
- **Read tools** (`board.show`, `board.list_tickets`, `board.list_relays`, `board.ticket_prompt`, `board.validate`, `board.status`, `board.rules`, `board.summary`) auto-pass the approval gate. **Write tools** (`board.claim`, `board.open`, `board.move`, `board.archive`, `board.update`, `board.promote_next`, `board.promote`, `board.comment`) route through it.

## Environment quirks

- **`mcp__yah__ask_user`** replaces `AskUserQuestion` for multiple-choice prompts to the user — the built-in tool is not wired up in this host.
- **Tool-use approvals** (Bash, Write, etc.) route through the AnswerQueue UI via `--permission-prompt-tool mcp__yah__approve_tool`; a Continue/Revise modal will appear in the desktop panel. To minimize Revise round-trips: name the target in the call's `description` ("Read app/yah/cli/src/main.rs" beats "Read file" — the user pattern-matches on description before clicking Continue); scope paths narrowly (`rg "foo" crates/yah/board/` is approvable, unbounded `rg "foo"` is a Revise); don't pre-stage destructive shapes (`rm -rf`, `git reset --hard`, `find … -delete`, `--no-verify`) unless the user has authorized that exact operation — they escalate to a hard review even when the target is harmless.
- **Grep `type: "tsx"` returns zero results silently.** claude-cli's Grep wraps ripgrep, which only knows `ts` (covers `.ts` and `.tsx`). Use `type: "ts"` or `glob: "**/*.tsx"`. If a Grep you expect to match returns nothing, recheck the type field before concluding the pattern is absent.
