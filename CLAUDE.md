# Project: Tiger Retail Billing (TRB) - Bronze-to-Silver Transformation

## What this project is

A PySpark pipeline that transforms bronze billing CSVs into a silver layer,
demonstrating four capabilities: deduplication, type casting, SCD2 history
tracking, and entity resolution - plus a shared guardrails layer and a pytest
test suite. See `Stack AI Rewire - Data Engineering - Silver Layer -
Assignment Doc.docx` for the full requirements and `Helper Guide - Silver
Layer.docx` for the skill-driven workflow.

## Skill-driven workflow (read before writing code)

For every task, read the matching file under `skills/<skill-name>/SKILL.md`
*before* implementing. Two skills (`deduplication`, `scd2_history`) are
provided as incomplete starting points and must be modified before you use
them for Task 2 and Task 6 respectively - identify the gap first, edit the
skill, then implement. Two skills (`entity_resolution`, `pipeline_guardrails`)
are not provided at all and must be authored from scratch for Task 7 and Task
8. `type_casting` is provided complete and used as-is.

## Project structure

- `data/` - baseline bronze CSVs (Batch 1) and `data/sample_batches/` (Batches
  2-5), each in its own folder with a README describing what it tests.
- `skills/` - the five skill folders described above.
- `src/` - your implementation. Each module maps to one capability.
- `silver/` - your pipeline's silver-layer output (created at runtime).
- `quarantine/` - your pipeline's quarantined-record output (created at
  runtime).
- `tests/` - the required pytest suite (Task 9).
- `outputs/artifacts/` - place non-code evidence here (logs, screenshots,
  pytest reports, skill-driven evidence log).

## Conventions

- Storage format: Parquet. SCD2 is implemented via explicit
  read-close-append-write, not `MERGE INTO`.
- Business keys: `account_id` (billing_accounts), `invoice_id` (invoices),
  `plan_id` (plan_catalog).
- Quarantine reason codes are a controlled vocabulary - see
  `skills/pipeline_guardrails/SKILL.md` once you've authored it - do not use
  free-text reasons.
- Do not modify `type_casting/SKILL.md`. Do modify
  `deduplication/SKILL.md` and `scd2_history/SKILL.md` before their respective
  tasks. Do author `entity_resolution/SKILL.md` and
  `pipeline_guardrails/SKILL.md` from scratch.

## Running

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/pipeline.py        # runs Batch 1 through Batch 5 in order
pytest tests/ -v               # required test suite (Task 9)
```
