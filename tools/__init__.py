"""Reviewer-facing helper scripts.

These are not part of the pipeline. ``src/`` holds exactly the modules the
Helper Guide lists; the two scripts here only *read* the written Parquet tables
and publish evidence under ``outputs/artifacts/``:

* ``python -m tools.evidence_dump``  -> outputs/artifacts/evidence_dump.md (uses Spark)
* ``python -m tools.verify_outputs`` -> outputs/artifacts/verify_outputs.md (pandas only)
"""
