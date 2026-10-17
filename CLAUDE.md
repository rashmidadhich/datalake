# Project: Tiger Retail Billing (TRB) - Bronze-to-Silver Transformation

## What this project is

A PySpark pipeline that transforms bronze billing CSVs into a silver layer,
demonstrating four capabilities - deduplication, type casting, SCD2 history
tracking, and entity resolution - plus a shared guardrails layer and a pytest
test suite. See `Stack AI Rewire-Data Engineering-Silver Layer-Assignment_Doc.docx`
for requirements and `Helper Guide- Silver Layer.docx` for the skill-driven
workflow.

## Skill-driven workflow (read before writing code)

For every task, read the matching file under `.claude/skills/<skill-name>/SKILL.md`
*before* implementing. Current state:

| Skill | State |
|---|---|
| `.claude/skills/type_casting` | Provided complete - used as-is. |
| `.claude/skills/deduplication` | **Modified for Task 2** (gap: boundary/ordering vs. SCD2 and entity resolution; controlled reason vocabulary). |
| `.claude/skills/scd2_history` | **Modified for Task 6** (gap: out-of-order `effective_date` rule). |
| `.claude/skills/entity_resolution` | **Authored from scratch for Task 7**. |
| `.claude/skills/pipeline_guardrails` | **Authored from scratch for Task 8**. |

The narrative evidence (gap found -> change made -> validation) lives in
`outputs/artifacts/skill_driven_evidence_log.md`; the machine log is
`outputs/artifacts/pipeline_log.md`.

## Project structure

- `data/` - baseline bronze CSVs (Batch 1) and `data/sample_batches/` (Batches
  2-5), each in its own folder with a README.
- `.claude/skills/` - the five skill folders (SKILL.md + README.md each).
- `src/` - the implementation; exactly the ten modules the Helper Guide lists,
  one module per capability.
- `tools/` - two reviewer-facing helpers that only read the written Parquet
  files and publish evidence: `tools/evidence_dump.py` and
  `tools/verify_outputs.py`.
- `silver/` - silver-layer output tables (Parquet), created at runtime.
- `quarantine/` - quarantined-record output tables (Parquet), created at runtime.
- `tests/` - the pytest suite (Task 9).
- `outputs/artifacts/` - non-code evidence (evidence log, run log, pytest report,
  evidence dump, output verification, rubric compliance check).
- `SETUP_AND_RUN.md` - how to rebuild the environment and re-run everything on a
  fresh machine.

## Conventions

- Storage format: Parquet. SCD2 is implemented via explicit
  read-close-append-write, **not** `MERGE INTO`.
- Business keys: `account_id` (billing_accounts), `invoice_id` (invoices),
  `plan_id` (plan_catalog). The SCD2 grain is the surrogate `entity_id`
  (equal to `account_id` for original entities, `<account_id>:R` when a key is
  reused by a different customer).
- Quarantine reason codes are a controlled vocabulary owned by
  `src/config.py` and documented in `pipeline_guardrails/SKILL.md`: never
  free-text.
- Writes are read-modify-write (rebuild the logical table from existing rows +
  the batch delta) so they are non-destructive and idempotent.
- Do not modify `type_casting/SKILL.md`. Modify `deduplication/SKILL.md` and
  `scd2_history/SKILL.md` before their tasks (done). Author
  `entity_resolution/SKILL.md` and `pipeline_guardrails/SKILL.md` (done).

## Running

```bash
# Environment (full fresh-machine guide: SETUP_AND_RUN.md)
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run Batch 1 through Batch 5 in order
python -m src.pipeline

# Required test suite (Task 9)
python -m pytest -v

# Re-check the written Parquet tables (pandas, no Spark needed)
python -m tools.verify_outputs

# Reviewer dump of counts, history rows and quarantine rows (uses Spark)
python -m tools.evidence_dump
```
