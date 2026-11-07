---
name: data-quality-validation
description: >
  Validate row-level business rules during ingestion and route invalid records
  to quarantine with traceability and a clear rejection reason. Use when source
  rows can be structurally readable but violate required content rules.
---

# Data Quality Validation

## Purpose

Define row-level content checks that separate records safe for the normal output from records that need review. This complements schema-drift handling, which checks columns, and idempotency, which checks repeated business keys.

## When to Use

Use when readable source rows must meet business rules before being written to the primary dataset, especially when invalid rows must be retained for review rather than silently discarded.

## Inputs

- An ingested DataFrame with source columns and ingestion metadata already attached.
- The business key or other required fields and their validation rules.
- The entity, batch identifier, and configured quarantine destination.

## Expected Outcome

Return valid and quarantined DataFrames. Valid rows continue through idempotency checks to Bronze. Invalid rows are excluded from Bronze, retained in quarantine, and tagged with a non-empty rejection reason. Both outputs retain the original source fields and ingestion metadata.

## Recommended Approach

1. Define each rule from a business requirement; do not infer validation rules from convenience.
2. Treat a required string key as invalid when it is null, empty, or whitespace-only (trim before checking).
3. Split rows using the rule without dropping either side. Add a `rejection_reason` to each quarantined row.
4. Validate after metadata tagging so valid and quarantined rows both retain `batch_id`, `source_file`, and `ingestion_timestamp`.
5. Write valid rows through the normal idempotent Bronze path. Write quarantined rows separately by entity and batch, with a reason and observable rejected-row count. A retry of the same batch should not multiply that batch's quarantine output.
6. Keep the job running when records are invalid; reject the rows, not the whole batch.

## Validation

- Derive the expected valid and invalid sets from the configured input and rule; do not duplicate source row counts in code.
- Confirm valid rows reach Bronze and invalid rows do not.
- Confirm every quarantined row has its original values, ingestion metadata, and a non-empty rejection reason.
- Confirm the run reports a non-zero quarantine count when invalid input exists and succeeds for empty or all-invalid input.

## Edge Cases

- Null, empty, and whitespace-only values for a required key.
- Empty batches and batches where all rows are invalid.
- A retry of the same batch; quarantine output should remain attributable to that batch and not silently accumulate duplicate retry results.

## Common Mistakes

- Filtering invalid rows away without retaining them in quarantine.
- Rejecting only nulls while accepting empty or whitespace-only keys.
- Adding metadata after splitting, leaving quarantined rows without lineage.
- Recording no rejection reason or failing the whole batch because some rows are invalid.
- Treating schema changes or duplicate detection as row-content validation.

## Reusability

Keep rules parameterized by entity and field rather than embedding assignment file names. The same pattern applies to required identifiers, dates, enumerated values, and other explicit row-level business constraints.

## Assignment Example

For Batch 5, `customers.customer_id` is required. Three customer rows have a non-blank key and should reach Bronze; the two rows with blank keys should be quarantined with a reason. No other Batch 5 field is invalid.
