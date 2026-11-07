# Skill-Driven Evidence Log

Project: Tiger Retail Billing - Bronze to Silver transformation
Tooling: OpenCode (agent) + DeepSeek V4.1 Flash; PySpark 3.5.1; Java 17; Hadoop
3.3.5 winutils.

This log records, per applicable task, the skill-driven workflow actually
followed: the skill that governed the task, the gap identified (if any), the
change made to the skill, and the validation performed. It is the narrative
half of the evidence; the machine half is `outputs/artifacts/pipeline_log.md`
and `outputs/artifacts/pytest_report.md`.

---

## Task 0 - Skill-driven workflow (all applicable tasks)

Workflow applied for every task: **read the governing `SKILL.md` first, compare
it against the concrete batch, record the gap, edit/author the skill, then
implement against the modified skill, then validate.**

| Task | Governing skill | Action taken before implementation |
|---|---|---|
| 1 | (baseline; conventions in `CLAUDE.md`) | Read `type_casting` skill to fix the canonical types; implemented explicit schemas. |
| 2 | `deduplication` | Modified (gap below) |
| 3 | `type_casting` | Used as-is; implemented the skill's quarantine escalation |
| 5/6 | `scd2_history` | Modified (gap below) |
| 7 | `entity_resolution` | Authored from scratch |
| 8 | `pipeline_guardrails` | Authored from scratch |

A gap was identified and closed for tasks 2, 6, 7, 8 *before* the matching code
was written; task 3 used its skill as provided and only implemented the
quarantine behaviour the skill delegates to the pipeline.

---

## Task 2 - Deduplication (`deduplication/SKILL.md`)

**Gap identified.** The skill as provided already defined the three outcomes
(quarantine / overwrite-as-update / retain-unchanged), but it did **not** define
the *boundary and ordering* against the neighbouring skills. As written it would
quarantine any repeated key whose non-key columns differ - including a
tracked-attribute change (Task 5) or a key reused by a different customer
(Task 7) - and its "Expected Outcome" item for quarantine was self-contradictory
(it described a duplicate being quarantined while stating it should not be).

**Change made.** Added:
- an explicit, ordered **classification rule** ("check this before quarantining")
  routing a repeated key to the SCD2 skill when the row carries an effective
  date, and to the entity-resolution skill when the identity attributes
  disagree;
- a rule that quarantine reason codes must come from the pipeline's controlled
  vocabulary;
- rewrote the quarantine expected-outcome item so a byte-identical duplicate is
  never described as quarantined;
- added a common-failure scenario covering the boundary mistake.

**Note on the brief.** The assignment states that the deduplication starting
point "does not yet define the overwrite-as-update or retain-unchanged
outcomes". In the copy provisioned to this project those two outcomes **were
already present** (Purpose, Expected Outcome items 2-3, and the Decision
Rules), so they were verified against the batch rather than re-added - the
genuine remaining gap was the one closed above. This is recorded here so the
difference from the brief is not mistaken for a missed gap.

**Validation.** `tests/test_dedup.py` proves all three outcomes on a controlled
frame (retain / overwrite / quarantine) plus the reason code; the Batch 4 and
Batch 5 tests confirm plan changes and reused keys are *not* quarantined by the
dedup path.

---

## Task 3 - Type Casting (`type_casting/SKILL.md`)

**Gap identified.** None in the skill; it is provided complete and used as-is.
The skill expressly leaves the *unrecognized format* case to the pipeline
("quarantine at the pipeline level"), which had to be implemented.

**Change made.** Implemented exactly that escalation: per-source pattern
(`yyyy-MM-dd` for `legacy_v1`, `MM/dd/yyyy` for `platform_v2`), an
**unambiguous-format-shape fallback** (the skill allows the format's own shape
as a signal), and quarantine of any value matching no known pattern. A row Spark
reports as `_corrupt_record` (the long-form date with an unquoted comma, which
shifts every later column) is treated as unrecognized rather than silently
mis-parsed.

**Validation.** `tests/test_type_casting.py` covers the happy path, the
format-drift fallback (needed because one batch declares `platform_v2` yet writes
legacy-format dates), and the unrecognized-date quarantine with the correct
reason code.

---

## Task 5 / Task 6 - SCD2 History (`scd2_history/SKILL.md`)

**Gap identified.** The skill did not define behaviour for an `effective_date`
earlier than the entity's current `valid_from`. Implementing the naive
close-and-append would write an **inverted interval** (`valid_to` earlier than
`valid_from`) and corrupt history.

**Change made.** Added a decision rule: on an out-of-order date, either rebuild
the timeline so intervals stay contiguous and non-overlapping with exactly one
open row, or quarantine with a controlled reason
(`out_of_order_effective_date`). Also added a rule that a stated prior value that
disagrees with the current value is stale and gets the same handling, plus
matching validation steps and a common-failure entry.

**Validation.** `tests/test_scd2.py` proves the normal close-and-append, the
single-open-row invariant, the as-of-date query, and that the out-of-order change
is quarantined and leaves the account's history untouched.

---

## Task 7 - Entity Resolution (`entity_resolution/SKILL.md`, authored)

**Gap identified.** No skill existed; had to be authored per the Helper Guide's
skill-creation template.

**Change made.** Authored a reusable skill: detect key reuse via a stable,
non-mutable, non-sensitive **identity fingerprint**; branch to same-entity vs
different-entity; assign a deterministic **surrogate identity** to a reused key;
never merge the original entity's rows/history; never use a masked field as an
identity or join key. Explicitly non-decisive on a `source_system` change alone
(a customer can migrate systems).

**Validation.** `tests/test_entity_resolution.py` proves a reused key becomes a
distinct identity, a new key keeps `account_id`, the three Batch 5 keys resolve
to three identities, and the original owners' rows are untouched.

---

## Task 8 - Pipeline Guardrails (`pipeline_guardrails/SKILL.md`, authored)

**Gap identified.** No skill existed; authored per template.

**Change made.** Authored the five invariants: row-count conservation, controlled
quarantine reason vocabulary, masked-field passthrough integrity, idempotent
re-runs, and non-destructive writes. Implemented them as reusable assertions in
`src/guardrails.py`, called after every batch from `src/pipeline.py`.

**Validation.** `tests/test_guardrails.py` proves each assertion passes on the
real run and can fail when deliberately broken, plus a full idempotency replay
(re-running Batches 2-5 leaves every table byte-identical) and non-destructive
checks.

### Issues found while applying the guardrails (and fixed)

| # | Issue | How it showed up | Fix |
|---|---|---|---|
| 1 | A write to a silver table cannot re-read the table it is overwriting: Spark deletes the target directory before the write plan re-reads it | `FileNotFoundException` / broken lineage when re-running a batch | `ingest.materialize()` (`localCheckpoint(eager=True)`) on any DataFrame derived from the table being overwritten |
| 2 | The Task 8 test read stale part-files: a `DataFrame` created from a path remembers the files it saw, and the idempotency replay overwrites them with new names | `test_earlier_batches_are_not_destroyed` failed with `SparkFileNotFoundException` although the data was intact | the `silver` fixture is now function-scoped so each test re-reads; reproduced and confirmed with a standalone `read -> overwrite -> count` experiment |
| 3 | Verification read Spark `DATE` columns back as `datetime.date` objects, not `datetime64` | `verify_outputs` raised `TypeError: Cannot compare Timestamp with datetime.date` | the verifier normalizes date columns after reading, and reports a crash as a FAIL row instead of a traceback |
| 4 | `local[*]` on a 12-core machine starts one Python worker per task slot, and worker startup dominates the run | integration tests took 2-3 minutes each | master is now overridable with `TRB_SPARK_MASTER`; the suite runs `local[2]` with `spark.default.parallelism=4` |
| 5 | Three Task 3 unit tests built their input with `createDataFrame(rows, [column names])`, so Spark had to **infer** types; a one-row batch has a null `_corrupt_record`, and inference raised `CANNOT_DETERMINE_TYPE` | `tests/test_type_casting.py` failed 3/4 on the first full run | the tests now pass the same explicit schema the CSV reader declares (`RAW_ACCOUNT_SCHEMA`, derived from `ingest.ACCOUNT_COLS`), so nothing is inferred - the same rule the assignment imposes on the pipeline |

Issue 2 is worth calling out because it is the exact situation this guardrail
exists to catch (a later batch destroying earlier data). The failure was a false
positive from a stale handle, not data loss: the `verify_outputs` check "Batch 1
account ids all still present (non-destructive writes)" reads the final tables
and passes.

---

## Data-quality discrepancy found (Batch 2)

The provided `batch_2_duplicate_retry` CSV does not match its documentation:

| Source | Expected Batch 2 |
|---|---|
| Assignment doc (Task 2) | 4 duplicates, **1 legitimate update**, 1 conflict |
| `data/sample_batches/README.md` | 4 duplicates, 2 conflicts |
| provisioned `tests/test_dedup.py` stub | quarantine count = 2 |
| **Actual CSV** | **2 duplicates, 4 conflicts, 0 updates** |

Verified programmatically against `data/invoices.csv`: `INV-00449` and
`INV-00122` are byte-identical, while `INV-00113`, `INV-00220`, `INV-00301`,
`INV-00378` each disagree on `account_id` and `invoice_date`. Decision taken
(with the project owner): implement the pipeline to the data, assert the actual
outcome in tests, and record the discrepancy here rather than fabricate an
update.

A second, related data quirk: Batch 5 declares `source_system = platform_v2` but
writes `yyyy-MM-dd` dates, and the Batch 3 unrecognized-date row is an unquoted
comma inside `December 11, 2025`. Both are handled (format-shape fallback;
corrupt-record quarantine) and noted rather than silently normalized.

---

## Validation summary

See `outputs/artifacts/pipeline_log.md` for the per-batch run log and
`outputs/artifacts/pytest_report.md` for the test results. Final table counts:
silver billing_accounts 110, plan_catalog 6, invoices 480; quarantine
billing_accounts 2, invoices 4.

Supporting artifacts, all under `outputs/artifacts/`:

| File | Contents |
|---|---|
| `pipeline_log.md` | per-batch counts, quarantine reasons, guardrail results (machine half of this log) |
| `pytest_report.md` | saved pass/fail output of the pytest suite (Task 9) |
| `evidence_dump.md` | Batch 1 baseline, schemas, SCD2 history, entity split, quarantine rows, invariants |
| `verify_outputs.md` | result of `python -m tools.verify_outputs` (pandas checks, no Spark needed) |
| `how_to_verify_outputs.md` | step-by-step ways to re-check the Parquet tables |
| `rubric_compliance_check.md` | this project checked row by row against the rubric |

`SETUP_AND_RUN.md` (project root) explains how to rebuild the environment on a
fresh machine and re-run the pipeline, the tests and the verification.
