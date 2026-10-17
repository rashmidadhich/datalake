---
name: silver-type-casting
description: >
  Normalize inconsistent formats (dates, casing, numeric representations)
  arising from multiple source system versions into one consistent silver-layer
  type, without guessing at values that match no known format. Use whenever an
  entity is sourced from more than one upstream system or version.
---

# Skill Name

Type Casting (Multi-Source Format Normalization)

## Purpose

Guide the normalization of a field that arrives in more than one format across
source systems, into a single consistent representation in the silver layer.

## Problem This Skill Solves

Different source systems (or different versions of the same system) frequently
write the same logical field - a date, a code value - in different formats. A
pipeline that assumes one fixed format will either crash or silently misparse
values from the other source. This skill defines how to detect the format in
use and cast correctly, while refusing to guess at a format it does not
recognize.

## When to Use

- The same entity is ingested from more than one source system or source
  version.
- A field's textual representation (date format, casing, numeric format)
  differs by source, but its logical meaning is the same.

This skill does not cover values that are missing entirely (see data-quality /
guardrail handling for that), or fields whose logical meaning itself differs
by source.

## Inputs / Context Required

- **The set of known format patterns** for the field, one per source
  system/version (e.g. `YYYY-MM-DD` for `legacy_v1`, `MM/DD/YYYY` for
  `platform_v2`).
- **A signal for which pattern applies** to a given row - a `source_system`
  column, or the format's own shape if it is unambiguous.
- **The incoming batch.**

## Expected Outcome

Every row's field ends up in one consistent type/format in the silver output,
for every source system the field is known to arrive from. A value that
matches none of the known patterns is never guessed at, coerced, or silently
dropped - it is set aside for review.

## Core Guidance

**Understand**: this is a comparison-and-branch problem, not a single parsing
rule. You are choosing which known pattern applies to a given row, applying it
explicitly, and treating "none of the above" as its own outcome rather than an
error to swallow.

**Reason**: before implementing, decide:
- What are all the known formats for this field, and how do you tell them
  apart (an explicit source flag is more reliable than shape-sniffing alone)?
- What should happen to a value that matches no known format? (Answer: it must
  not be cast at all - see Decision Rules.)

## Decision Rules

- Maintain an explicit list of known format patterns per source system/version.
  Do not add a new pattern silently just because a value happened to parse
  under a loose rule.
- Cast a value only when it clearly matches one of the known patterns for its
  source.
- A value that matches no known pattern must not be cast, must not be guessed
  at, and must not fail the whole batch - it is a candidate for quarantine
  (handled at the pipeline level, not inside this skill).
- Casing/formatting normalization (e.g. `gold` vs `GOLD`) should be resolved to
  one canonical representation, decided once and applied consistently.

## Implementation Guidance

1. Read the incoming batch together with its source signal (e.g.
   `source_system`).
2. For each row, attempt the format pattern associated with its source.
3. If the pattern matches, cast to the canonical type and value.
4. If no pattern matches, do not cast - flag the row (e.g. add a boolean or
   reason column) so the pipeline can route it to quarantine.
5. Log which rows required a cast and which were flagged as unrecognized.

```python
from pyspark.sql import functions as F

casted_df = df.withColumn(
    "signup_date_casted",
    F.when(F.col("source_system") == "legacy_v1", F.to_date("signup_date", "yyyy-MM-dd"))
     .when(F.col("source_system") == "platform_v2", F.to_date("signup_date", "MM/dd/yyyy"))
     .otherwise(F.lit(None))
)
flagged_df = casted_df.withColumn(
    "format_unrecognized", F.col("signup_date_casted").isNull() & F.col("signup_date").isNotNull()
)
```

## Validation

- Ingest a batch with only known formats; assert every row casts correctly and
  none are flagged.
- Ingest a batch containing one unrecognized-format row; assert it is flagged
  and not cast to any value (not even null-by-coincidence from a wrong
  pattern).
- Assert casing/format normalization produces exactly one canonical value for
  logically-equivalent inputs (e.g. `gold` and `GOLD` both become `PLAN-GOLD`
  or whatever the canonical form is).

## Common Failure Scenarios

- A loose date parser "succeeds" on an unrecognized format by guessing wrong
  (e.g. swapping day/month), producing a plausible-looking but incorrect date.
- The unrecognized-format row crashes the whole batch instead of being
  isolated.
- Casing normalization is applied inconsistently across batches, so the same
  logical value ends up under two different canonical spellings over time.

## Common Mistakes

- Using a single "smart" date parser across all sources instead of an explicit
  per-source pattern.
- Silently dropping a row that fails to parse instead of flagging it for
  quarantine.
- Normalizing casing only going forward, without checking whether historical
  silver rows also need the canonical form.

## Reusability

Applies to any field, in any pipeline, that arrives in more than one format
across sources or source versions - substitute the field, the known patterns,
and the source signal for the case at hand.

---
