# /refine — Refine phases into relays + tickets on the hack-board

You just described an implementation plan with phases. Refine them into hack-board items so the work is trackable and forkable.

## Model

- **Relay** — a thread of work. One agent owns it. Carries the baton across context resets. ID is `R<n>` (e.g. `R007`).
- **Ticket** — an incremental work unit *inside* a relay. Usually session-sized — claim, work, archive. IDs are always compound: `R007-T1`, `R007-T2` (or `Q005-T1` under a quest's child relay — but never `Q005-T1` directly under the quest, see Quest below). Every ticket has a parent relay; `board claim`/`board open` reject `--kind task|feature|bug` without `--parent`.
- **Quest** — a coordination relay declared with `@yah:kind(quest)`, or *inferred* when one or more other **bare-R/Q relays** declare `@yah:parent(QXXX)` pointing at it. Coordination-of-relays, not coordination-of-tickets — sub-tickets never promote their parent relay to quest. ID is `Q<n>` (e.g. `Q005`); the R/Q counter is shared so a Q-prefix never collides with an R-prefix. **Tasks/features/bugs cannot be parented to a Q-id directly** — open a child relay under the quest first, then attach sub-tickets to that relay. Legacy `@yah:kind(epic)` is accepted as an alias and keeps an R-prefix; new work should prefer quest.
- **Phase** — ordering tag. "These items ship together." `@yah:phase(P1)` — parsed by `yah arch` and surfaced as `phase:` on tickets in `yah board tickets` / inflight / status output. Useful for grouping at refinement time even though the board UI doesn't (yet) sort columns by phase.
- **Parent** — hierarchy pointer. `@yah:parent(R007)` belongs to R007. For compound IDs the parent is inferred from the prefix.

## Process

### Step 0a: Plan with one TodoWrite call

Don't open a TaskCreate-per-step or stream individual TaskUpdates: one
`TodoWrite` with the full ordered plan at the start (scan → arch-read →
quest → relays → sub-tickets → covered-by stamp → summary), marking each
complete as you go, is the cheapest bookkeeping shape and cuts the
round-trip count by ~10 on a typical refinement.

### Step 0b: Scan what's already in flight

Before you plan anything, run:

```bash
yah board inflight
ls .yah/events/ 2>/dev/null || true   # what relay shards exist? (empty dir is fine)
# If a candidate relay looks adjacent, peek its history:
tail -n 20 .yah/events/R0XX.jsonl
```

Run probe commands as separate Bash calls (or guard each with `|| true`):
`grep '@yah:foo' file; ls .yah/events/` chained with `;` will be reported
as a failed call when the first half has no match, masking the second
half's output even though the data is there.

`board inflight` prints every Open / Active / Handoff relay and ticket with its one-line purpose and arch-doc ref. **It does not show review-state relays.** A relay in review is the normal resting state between completion and sign-off — it can sit there for days. If you're planning against a specific arch doc, also scan for relays that have already referenced it (they may be in review and the work may already be done):

```bash
# Set DOC to the arch doc you're planning against, then run:
DOC=".yah/docs/working/your-topic.md"
grep -rlF "@arch:see($DOC)" \
    --include='*.rs' --include='*.ts' --include='*.md' . 2>/dev/null \
  | xargs grep -h '@yah:relay' 2>/dev/null \
  | grep -oE '[RQ][0-9]+' | sort -u \
  | while read ID; do yah board show "$ID"; done
```

If any relay returned here is in **review or handoff**, surface it to the user before opening new tickets.

Also check the arch doc itself — `/refine` stamps it with a `@yah:covered-by` comment when it creates a relay. These survive even when the relay moves to review (invisible to `board inflight`):

```bash
grep '@yah:covered-by' "$DOC" 2>/dev/null
```

If any result shows `status=review` or `status=archived`, run `yah board show <ID>` and surface that relay before proceeding.

Five agents refining in parallel will independently plan the same problem unless they look first — R10. Read both and decide:

- **This problem is already a live relay** → don't refine. Claim it (`yah board claim <ID>` if it's Open, or `yah board move <ID> active` if it's Handoff) and continue its plan rather than starting over.
- **It partially overlaps an existing relay** → open your next steps as sub-tickets under that relay (`yah board open --kind task --parent R<n>`, per R8) instead of a new relay.
- **It's genuinely independent** → proceed below. When you write the arch doc, reference any adjacent relays so the next picker sees the relationship.

### Step 0c: Read the canon, not just the rollout

If the doc you're refining is a `working/*.md` rollout plan that points at
a `architecture/*.md` source-of-truth (look for "see [architecture/X.md]"
or "the design wins — update this file" caveats), **read the architecture
doc before opening any tickets**. Rollout plans drift; phase Verify lines
and "Depends on" sections reference sections in the canon that may have
moved or changed shape. Every relay you create inherits those references
via `--see`, so a stale rollout doc rolls forward into stale tickets.

If the working doc and the canon disagree, flag it to the user before
proceeding — don't silently encode the rollout doc's version.

### Step 1: Write the architecture doc

Pick the right folder under `.yah/docs/`:

- `.yah/docs/working/{topic}.md` — **the default for /refine plans.** Phase plans, migration playbooks, and other architecture-meets-implementation docs go here. They guide a specific piece of work; once that work ships, archive the doc with `archive_arch_doc` (it moves to `.yah/docs/archive/`). Future agents reading the design canon won't see it as load-bearing.
- `.yah/docs/architecture/{topic}.md` — **only for the design canon**: durable system architecture that future agents *should* consult as the source of truth. Use this when the doc describes how the system is shaped, not how a specific phase ships.
- `.yah/docs/archive/` — terminal state for retired `working/` docs. Never write here directly; reach it via `archive_arch_doc`.

Keep the prose — it's context future agents need.

### Step 2: Create the relay (or relay chain)

**Never pick IDs yourself.** Use `yah board open` when refining a plan — it scans source under a file lock, picks the next unused ID for the requested kind, and writes the annotation block straight into the **Open** column (unclaimed, no assignee). That's the only safe way to avoid ID collisions with another agent working in parallel, and `open` makes the intent explicit: these are inbox items waiting for someone to take them on.

```bash
# The overall effort as a relay (capture the printed ID — it's R-something)
RELAY=$(yah board open \
  --kind relay \
  --file src/module_central_to_the_work.rs \
  --title "ProcessBlock Unification" \
  --see .yah/docs/working/processblock_unification.md)
echo "$RELAY"   # e.g. R012
```

After creating the relay, stamp the arch doc at the top (above the `# Title` heading) so future `/refine` runs detect it even when the relay is in review:

```
<!-- @yah:covered-by(R012, status=open, 2026-05-12) -->
```

Update this stamp when relay status changes: `status=review` on entering review, `status=archived` after archiving.

For quests (multiple independent tracks under one coordination point), open a parent quest first, then open children with `--parent $QUEST`:

```bash
QUEST=$(yah board open --kind quest --file src/lib.rs --title "ProcessBlock unification")
# QUEST is a Q-id, e.g. Q005. Children are bare-R relays:
yah board open --kind relay --file src/phase4.rs --title "Phase 4 migration" --parent $QUEST --phase P1
yah board open --kind relay --file src/cv_bridge.rs --title "CV Port Bridge"  --parent $QUEST --phase P2
```

Don't try `--kind task --parent $QUEST` — quests own relays, not leaf tickets. The CLI rejects it with a pointer to the right pattern.

### Step 3: Create tickets under the relay

Each concrete sub-step becomes a **ticket inside** the relay. Use `yah board open --parent $RELAY`; the kind (feature/bug/task) becomes a `@yah:kind(...)` tag. The ID is allocated as a compound sub-ticket.

```bash
TID=$(yah board open \
  --kind task \
  --file src/rbj_biquad_node.rs \
  --title "Add cv_to_hz to RbjBiquadNode" \
  --parent $RELAY \
  --phase P1)
echo "$TID"   # e.g. R012-T1 (first sub-ticket under R012)
```

Sub-tickets under `$RELAY` get IDs like `R012-T1`, `R012-T2`, … regardless of `--kind`. The `-T` segment is always `T`; the feature/bug/task distinction survives as the `@yah:kind(...)` tag (and as the badge letter on the card).

**Pass `--depends-on` at open-time** when the dependency target already
exists, instead of opening bare and then `board update --replace-depends-on`
after. Linear chains (T1 → T2 → T3) and back-edges to already-allocated
relays both qualify. Only fall back to `board update` for *forward*
references between siblings being created in the same batch (R007's deps
on R008/R009 etc. when both don't exist yet).

```bash
yah board open --kind task --parent $RELAY --phase P1 \
  --title "Scaffold runtime package" --depends-on R012-T1
```

There is no "standalone" ticket form — `--parent` is required for `--kind task|feature|bug`. For a genuinely one-off piece of work, `board open --kind relay` first and claim the relay's own work under it (use `board open --kind task --parent $RELAY` for the first sub-ticket). Keeps the ID space clean and keeps every ticket's event shard rolled up under a relay.

Use `--json` for `{id, file, line}` if you're chaining commands.

### Step 4: Post a summary

```bash
yah board summary \
  --text "Created CV Port Bridge plan: R012 with 8 tickets across 3 phases." \
  --author agent:claude
```

(Or via MCP: the tool name is `board_summary`, not `hack_summary`.)

### Step 5: Confirm

Tell the user:
- The architecture doc path
- The relay IDs and what they map to
- The ticket IDs created, grouped by phase
- Which phase is ready to start

## Example

```
Created:
  .yah/docs/working/cv_port_bridge.md — full plan (archive when shipped)

  R012: CV Port Bridging (parent: R010)

  P1 — hardcoded fix (ready to start):
    R012-T1: V/Oct params produce wrong Hz           (kind: bug)     [open]
    R012-T2: Add cv_to_hz to RbjBiquadNode.process   (kind: task)    [open]
    R012-T3: Add cv_to_hz to CascadeFilter.process   (kind: task)    [open]
    R012-T4: Add cv_to_hz to LFO.process             (kind: task)    [open]

  P2 — infrastructure:
    R012-T5: CVMapping::pull() method                (kind: feature) [open]
    R012-T6: Wire prebaked fn pointer per CV input                   [open]
    R012-T7: Replace hardcoded cv_to_hz with pull()                  [open]

  P3 — cleanup:
    R012-T8: Remove dead n2v_scale/n2v_offset                        [open]
    R012-T9: Update stale doc comments                               [open]
```

## Tips

- Don't over-ticket. "Delete some dead code" is one task, not one per file.
- If the agent said "Want me to start?" — after creating tickets, run `yah board claim <first-P1-ID>` to flip that ticket into Active.
- Phases can run in parallel if independent. The relay owner decides.
