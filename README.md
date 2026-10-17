# Tiger Retail Billing - Silver Layer Transformation (Starter Code)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

## Project layout

See `CLAUDE.md` for the full project structure and conventions. In short:

- `data/` - input CSVs (Batch 1 baseline + Batches 2-5 sample batches).
- `skills/` - reusable skill files guiding each task. Some are provided
  as-is, some must be modified, some you must author (see the assignment
  doc and `CLAUDE.md`).
- `src/` - implement your pipeline here. Stub modules with `TODO` markers are
  provided; fill them in, don't rename them, unless you document why.
- `silver/`, `quarantine/` - pipeline outputs, created when you run
  `src/pipeline.py`.
- `tests/` - required pytest suite (Task 9). Stub test files with function
  names are provided; fill in the assertions.
- `outputs/artifacts/` - place non-code evidence here (skill-driven evidence
  log, pytest report, screenshots).

## Running the pipeline

```bash
python src/pipeline.py
```

This should process Batch 1 through Batch 5 in order, writing to `silver/`
and `quarantine/`.

## Running tests

```bash
pytest tests/ -v
```

## What to submit

Zip the complete project folder per the assignment's Section 8 (Submission
Rule), named `empid_emp_name_DE_SilverTransformation.zip`.
