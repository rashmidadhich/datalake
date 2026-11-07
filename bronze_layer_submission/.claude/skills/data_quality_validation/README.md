# data_quality_validation

## Overview

This skill covers row-level content validation: deciding whether a readable record meets a declared business rule and routing rejected records to quarantine with a reason. It is the new capability required for Task 7, Batch 5.

## Purpose and Scope

The existing ingestion-metadata, idempotency, and schema-drift skills address lineage, duplicate business keys, and column shape. They do not decide whether a row's required business-key value is present. This skill fills that gap without taking responsibility for schema evolution or duplicate detection.

## Expected Inputs and Outputs

| | |
|---|---|
| **Input** | An ingested DataFrame with metadata, a configured business key/rule, and a quarantine destination. |
| **Valid output** | Rows satisfying the rule; these continue through idempotency checks and into Bronze. |
| **Quarantine output** | Invalid rows with original columns, ingestion metadata, and a `rejection_reason`. |

## Batch 5 Scenario

`customers_batch5_null_customer_id.csv` contains five rows. Three have a non-blank `customer_id`; two have a blank key. The only rule for this scenario is that `customer_id` must not be null, empty, or whitespace-only.

Expected behavior: the batch completes; valid rows reach Bronze; invalid rows are retained separately in quarantine with a reason and source lineage; the run reports the quarantine count. The exact counts should be read from the data and asserted by tests rather than duplicated as pipeline constants.

## Validation Guidance

Read `SKILL.md` before implementing Task 7. Validate both sides of the split, confirm metadata survives on quarantined rows, and verify invalid rows are not written to Bronze. Also check that retries do not accumulate duplicate quarantine output for the same batch.

## Reusability

The rule is passed by business key and can be adapted to other entities and required fields. Assignment-specific filenames belong in configuration and tests, not in the general validation rules.
