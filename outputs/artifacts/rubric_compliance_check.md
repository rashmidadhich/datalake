# Rubric compliance check

This file walks the evaluation rubric row by row against the delivered work.
It is a self-check written by the learner, **not** a score: the rubric's
`Score Earned` and `Evaluator Notes` cells are left blank for the evaluator.

Status legend: **MET** = evidence exists and is listed; **MET (deviation)** =
met, but with a difference from the brief that is documented further down.

## AI-native engineering (60%)

| # | Rubric criterion | Max | Status | Evidence |
| --- | --- | --- | --- | --- |
| 1 | Task 0: skill-driven workflow evidence for every applicable task (2, 3, 5, 6, 7, 8) | 10 | MET | `Skill-Driven Evidence Log` sheet in the rubric workbook has one row per task; long form in `outputs/artifacts/skill_driven_evidence_log.md`; machine half in `outputs/artifacts/pipeline_log.md`; all five skill folders contain `SKILL.md` + `README.md` |
| 2 | Task 2: deduplication skill gap identified and skill modified before implementation | 8 | MET (deviation) | `.claude/skills/deduplication/SKILL.md` modified before `src/dedup.py` was written; the gap closed and the reason for the deviation are in the "Note on the brief" section of the evidence log |
| 3 | Task 6: SCD2 out-of-order `effective_date` gap identified and skill modified before implementation | 8 | MET | Rule added to `.claude/skills/scd2_history/SKILL.md` ("Decision Rules"); implemented in `src/scd2_writer.py`; `ACC-0053` quarantined as `out_of_order_effective_date`; `tests/test_scd2.py` |
| 4 | Task 7: `entity_resolution/SKILL.md` authored from scratch, required structure, genuinely reusable | 10 | MET | New folder with `SKILL.md` (frontmatter + Purpose/When to Use/Inputs/Expected Outcome/Recommended Approach/Validation/Edge Cases/Common Mistakes/Reusability) and `README.md`; implemented in `src/entity_resolution.py`; `tests/test_entity_resolution.py` |
| 5 | Task 8: `pipeline_guardrails/SKILL.md` authored from scratch, covers all 5 required rules | 10 | MET | All five rules have their own section in `.claude/skills/pipeline_guardrails/SKILL.md`; implemented as reusable assertions in `src/guardrails.py`, called after every batch from `src/pipeline.py`; `tests/test_guardrails.py` |
| 6 | Skill quality: no assignment-specific literals, no full solution code, single-capability focus | 8 | MET | `Select-String` over `\.claude\skills\*\SKILL.md` for `ACC-`, `INV-`, `batch_2..5`, row counts returns nothing; embedded snippets are 2-6 line illustrative fragments, no complete functions; each skill covers exactly one capability |
| 7 | Skill-driven evidence log: gap found, change made, validation recorded per task | 6 | MET | Six rows in the workbook sheet, plus per-task narrative (skill read, gap, change, how validated) in `outputs/artifacts/skill_driven_evidence_log.md` |

## Data engineering (40%)

| # | Rubric criterion | Max | Status | Evidence |
| --- | --- | --- | --- | --- |
| 8 | Task 1: Batch 1 baseline loaded with consistent types | 3 | MET | Explicit schemas in `src/ingest.py` (no `inferSchema`); counts 100 / 6 / 480 and the column-by-column schema are printed in `outputs/artifacts/evidence_dump.md` ("Batch 1 baseline", "Schemas") |
| 9 | Task 3: date + `plan_id` casting correct; unrecognized-format row quarantined | 6 | MET | `src/type_casting.py`; `ACC-0104` -> `unrecognized_format`, never guessed or nulled; `tests/test_type_casting.py`; checks 2-4 in `outputs/artifacts/verify_outputs.md` |
| 10 | Task 4: interim testing evidence for Batch 2 / Batch 3 | 3 | MET (deviation) | `tests/test_dedup.py` (5 tests incl. the Batch 2 outcome) and `tests/test_type_casting.py` (4 tests); Batch 2 narrative-vs-data note in the evidence log and `README.md` |
| 11 | Task 5: exactly one `is_current` row per entity, history queryable by date | 6 | MET | `src/scd2_writer.py` closes the open row and appends a new one; as-of query for `ACC-0068` returns PLAN-BASIC then PLAN-GOLD; `tests/test_scd2.py`; check 5 and 6 in `verify_outputs.md` |
| 12 | Task 9: pytest suite covers transformation logic, bad records, quarantine, expected outputs, guardrail rules, with a saved report | 12 | MET | 26 tests over five files; saved output `outputs/artifacts/pytest_report.md` |
| 13 | Guardrail correctness in practice: conservation, masked-field integrity, idempotent re-run, non-destructive writes | 6 | MET | Conservation checked after every batch in `src/pipeline.py`; replay test `test_replaying_batches_2_to_5_changes_nothing`; masked passthrough compared against the CSVs; results per batch in `pipeline_log.md` |
| 14 | Code quality: readable, modular, matches the Helper Guide structure | 4 | MET | `src/` holds exactly the ten modules the helper guide lists, one per capability; the two reviewer helpers live in `tools/` so the pipeline tree is unchanged; `silver/`, `quarantine/`, `tests/`, `.claude/skills/` in the expected places |

## Differences from the brief, and why

1. **Batch 2 does not match its own narrative.** The brief, the sample-batch
   README and a test stub describe 4 duplicates + 1 update + 1 conflict (or 4 + 2).
   The CSV contains **2 byte-identical retries and 4 rows that disagree with the
   baseline on identity columns** (so 4 conflicts, 0 updates). The pipeline handles
   the data as written, the tests assert the real outcome, and the discrepancy is
   recorded in `README.md` and
   `outputs/artifacts/skill_driven_evidence_log.md`. Rule (2) of the brief -
   overwrite-as-update - is still implemented and unit-tested, it simply has no
   row to act on in the supplied data.

2. **The deduplication skill's stated gap was already closed.** The brief says the
   provided skill does not define `overwrite-as-update` and `retain-unchanged`,
   but the skill delivered by TigerOS already lists all three outcomes. The gap
   actually found and closed is the skill's **boundary**: it did not say what to do
   when the effective change is a plan change (SCD2) or a reused key (entity
   resolution), which would otherwise be quarantined as a conflicting invoice row.
   The change adds an ordered classification step and requires reason codes from
   the controlled vocabulary. Details in the evidence log.

3. **Out-of-order row is quarantined, not rebuilt.** The modified SCD2 skill allows
   either rebuilding the timeline or quarantining. Reconciliation would rewrite
   intervals that have already been published, so the pipeline quarantines
   `ACC-0053` with `out_of_order_effective_date` and leaves its history open and
   untouched - the safer of the two allowed outcomes.

4. **Task 4's "interim testing evidence"** is delivered as the Batch 2 / Batch 3
   test files plus the per-batch log, rather than a separate interim report; no
   rubric row asks for a particular artifact shape.

5. **`src/` matches the guide's module list exactly.** The Helper Guide names ten
   modules under `src/`; all ten are present, one module per capability. The two
   reviewer-facing helpers that publish evidence (`evidence_dump`,
   `verify_outputs`) live in `tools/` so they cannot be mistaken for pipeline
   code - `src/pipeline.py` never imports them. The only dependency added to the
   provided `requirements.txt` is `pyarrow`, used by `tools/verify_outputs.py`
   (pandas reads Parquet through it); the pipeline itself needs PySpark only.

6. **The provided `type_casting/SKILL.md` is byte-identical to the supplied
   file** (confirmed with `git diff`). Only its `README.md` was updated, to
   record two data quirks found in Batch 3/5: the malformed row carries an
   unquoted comma (Spark reports it as `_corrupt_record`), and one batch declares
   `platform_v2` while writing legacy `yyyy-MM-dd` dates. The skill's guidance is
   applied as-is; the extra quarantine handling lives in the pipeline, exactly as
   the brief requires.

7. **SCD2 grain is the surrogate identity, not `account_id`.** The rubric row for
   Task 5 says "exactly one `is_current` row per account", while Task 7 requires
   the three reused `account_id` values to become distinct identities. Both
   cannot hold literally at once, so the pipeline keeps one open row per
   `entity_id` (110 rows / 106 identities / 103 distinct `account_id`s). For the
   100 original accounts the two readings are identical.
