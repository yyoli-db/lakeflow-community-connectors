Build a new community connector end-to-end.

Usage: /create-connector <source_name> [tables=t1,t2,...] [doc=<url_or_path>]

Arguments: $ARGUMENTS

---

Parse arguments: first positional = **source_name** (required, lowercase); `tables=` = comma-separated tables (optional); `doc=` = API doc URL or path (optional). Stop and ask if source_name is missing.

Paths: `SRC=src/databricks/labs/community_connector/sources/{source_name}`, `TESTS=tests/unit/sources/{source_name}`, `STATE_FILE=.claude/connector-state/{source_name}.json`

---

## State Management

Workflow state is persisted to `STATE_FILE` after every completed step so the workflow can resume after interruption.

**State file schema:**
```json
{
  "source_name": "<source_name>",
  "created_at": "<ISO timestamp>",
  "updated_at": "<ISO timestamp>",
  "last_completed_step": 0,
  "tables": [],
  "auth_method": "",
  "doc_url": "",
  "steps": {
    "1": {"status": "pending", "completed_at": null, "summary": "", "artifacts": []},
    "2": {"status": "pending", "completed_at": null, "summary": "", "artifacts": []},
    "3": {"status": "pending", "completed_at": null, "summary": "", "artifacts": []},
    "4": {"status": "pending", "completed_at": null, "summary": "", "artifacts": []},
    "5": {"status": "pending", "completed_at": null, "summary": "", "artifacts": []},
    "6": {"status": "pending", "completed_at": null, "summary": "", "artifacts": []}
  }
}
```

**After each step's confirmation gate**, update the state file:
- Set `steps.{N}.status` = `"completed"`, `steps.{N}.completed_at` = current ISO timestamp
- Set `steps.{N}.summary` = one-sentence description of what was produced
- Set `steps.{N}.artifacts` = list of file paths created/modified in that step
- Set `last_completed_step` = N
- Set `updated_at` = current ISO timestamp
- If step 1, also update `tables` and `auth_method` from what was found
- Use the Write tool to save the full updated JSON to `STATE_FILE`

---

## Resume Check

Before presenting the plan, check if `STATE_FILE` exists using the Read tool (it's fine if it doesn't — that's the normal fresh-start case).

**If the state file exists and `last_completed_step >= 1`:**
Read it, then use `AskUserQuestion`:
> "Found existing progress for **{source_name}** (last completed: Step {N} — {step summary}).
> Would you like to resume from Step {N+1}, or start over from scratch?"
Options: `"Resume from Step {N+1}"` / `"Start over (discard state)"`

- If **resume**: skip all steps ≤ `last_completed_step`, restore `tables` and `auth_method` from state, jump directly to Step `last_completed_step + 1`. Still create TaskCreate entries for all 6 steps and immediately mark completed ones as `completed`.
- If **start over**: delete the state file with `Bash(rm STATE_FILE)`, then proceed fresh.

**If no state file exists**: create the directory with `Bash(mkdir -p .claude/connector-state)`, write the initial state JSON with `last_completed_step: 0` and all steps `"pending"`, then proceed normally.

---

## Protocols

**Plan first**: Present tables in scope + 6-step workflow. Hard-stop with `AskUserQuestion`: "Does this plan look good?" ("Yes, proceed" / "I have adjustments"). Do NOT start Step 1 until confirmed. (Skip this gate if resuming.)

**Task tracking**: Once confirmed, `TaskCreate` for all 6 steps. Mark `in_progress` before launching each step, `completed` after.

**Confirmation gate** (steps 1–5): After each step, commit all new/modified files under `SRC` and `TESTS` with a message like `feat({source_name}): step N - <short description>`. Then update the state file. Then `AskUserQuestion` with a summary of what was produced (files created, tables found, test results). Options: "Continue" / "Review first". Do NOT proceed without confirmation. Step 6 skips the gate.

**Subagent pattern**: `Task(subagent_type=..., run_in_background=true)` → wait for automatic completion notification — do **NOT** poll using `TaskOutput`, `sleep`, or `cat` on the output file. Once notified, verify output files with `Glob`. Every subagent prompt must include: source name, all relevant file paths, and table scope. Subagents have no prior context.

---

## Step 1 — API Research
Subagent: `source-api-researcher` → `{SRC}/{source_name}_api_doc.md`

Prompt: source name, doc URL/path (if any), table scope. Tell it not to ask the user.
Gate: summarize tables and auth method found.
State update: set `tables`, `auth_method`, `doc_url`; artifact = `{SRC}/{source_name}_api_doc.md`

---

## Step 2 — Auth Setup

Run the `/authenticate-source` skill. Read and follow `.claude/skills/authenticate-source/SKILL.md`.
Finish all the steps in the skill sequentially.

Gate: confirm auth test passes.
State update: artifact = `{TESTS}/configs/dev_config.json`

---

## Step 3 — Implementation
Subagent: `connector-dev` → python files under `{SRC}/`

Prompt: source name, API doc path, tables to implement.
Gate: verify implementation file(s) exist.
State update: artifacts = all `.py` files created under `{SRC}/`

---

## Step 4 — Testing & Fixes
Subagent: `connector-tester` → `{TESTS}/test_{source_name}_lakeflow_connect.py` (all passing)

Prompt: source name, implementation path, `dev_config.json` path.
After subagent: run `pytest {TESTS}/ -v --tb=short` yourself using a **synchronous** Bash call with `timeout=60000` (60s). Never run pytest in background. Never use `sleep`, `tail`, `wc -l`, or `ps aux` to monitor it. If pytest times out, do NOT increase the timeout — instead tighten `dev_table_config.json` (halve `window_hours`, `lookback_days`, or `max_records_per_batch`) and retry. If tests fail, do NOT proceed — report failure to user.
Gate: confirm all tests pass.
State update: artifact = `{TESTS}/test_{source_name}_lakeflow_connect.py`; summary includes pass count

---

## Step 5 — Docs + Complete Spec

**5a.** Subagent: `connector-doc-writer` → `{SRC}/README.md`
Prompt: source name, implementation and API doc paths.

**5b.** Subagent: `connector-spec-generator` → `{SRC}/connector_spec.yaml` (complete with `external_options_allowlist`)
Prompt: source name, implementation path.

Gate: verify both files exist.
State update: artifacts = `{SRC}/README.md`, `{SRC}/connector_spec.yaml`

**Post-gate**: After confirmation, use `AskUserQuestion` — "The connector is fully developed, tested, and documented. Step 6 (packaging & deployment) is optional." Options: "Proceed with deployment" / "Stop here". If they stop, skip to Final Summary.

---

## Step 6 — Deployment

Run the `/deploy-connector` skill. Read and follow `.claude/skills/deploy-connector/SKILL.md`.
Pass the source name with `use_local_source=true`. Finish all the steps in the skill sequentially.
This is an interactive process — ask the user for input at each step rather than assuming values.
State update: summary = pipeline name and workspace; artifact = generated merged source file path

---

## Final Summary

```
Connector: {source_name}
Tables:    [list]
Source:    src/databricks/labs/community_connector/sources/{source_name}/
Tests:     tests/unit/sources/{source_name}/
```

If a subagent fails (e.g. couldn't write its output file), report the failure clearly to the user — do not attempt to redo the subagent's work yourself. If the user wants to resume from a step, skip earlier ones and update the state file accordingly.
