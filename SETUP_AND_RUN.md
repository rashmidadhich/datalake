# Run this project on another machine (setup + execution guide)

This project is a self-contained PySpark pipeline. The submission zip does **not**
contain the `.venv` folder or any Spark cache, so a fresh machine builds its own
environment from `requirements.txt`. Everything below was verified on Windows 11;
the Linux/macOS notes are the standard equivalents.

## 1. What you need before you start

| Item | Version used here | Why |
| --- | --- | --- |
| Python | 3.11.9 (3.10-3.12 works) | PySpark 3.5.1 publishes no wheel for Python 3.13 |
| Java JDK | 17 (11 also supported by Spark 3.5) | Spark runs on the JVM; `JAVA_HOME` must point at the JDK |
| Windows only: Hadoop winutils | Hadoop 3.3.5 build | Spark on Windows needs `winutils.exe` and `hadoop.dll` |
| Free disk | ~2 GB | Spark temp/shuffle plus the `silver/` and `quarantine/` Parquet outputs |

## 2. Create the environment

Windows (PowerShell):

```powershell
cd "path\to\Silver Layer Project"
python -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Linux / macOS:

```bash
cd path/to/Silver\ Layer\ Project
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` pins `pyspark==3.5.1`, `pandas==2.2.2`, `pyarrow==17.0.0`,
`pytest==8.2.0`.

## 3. Point Spark at Java (and on Windows, at Hadoop)

Linux/macOS: install a JDK and export `JAVA_HOME` so Spark can find it.

Windows:

1. Install JDK 17 and note the folder, e.g.
   `C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot`.
2. Download a Hadoop 3.3.5 winutils build (for example
   `github.com/cdarlint/winutils`) and copy `winutils.exe` and `hadoop.dll`
   into `C:\hadoop\bin`.
3. Set the variables once, then open a **new** shell:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME","C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot","User")
[Environment]::SetEnvironmentVariable("HADOOP_HOME","C:\hadoop","User")
```

> `src/spark_session.py` fills `JAVA_HOME`, `HADOOP_HOME`, `PATH` and
> `PYSPARK_PYTHON` in automatically **only when they are unset** and the default
> folders exist, so a machine with just the JDK configured also works.

Smoke test (should print a Spark version and stop cleanly):

```powershell
.venv\Scripts\python.exe -c "from src.spark_session import get_spark; s=get_spark('smoke'); print(s.version); s.stop()"
```

## 4. Run the pipeline

```powershell
.venv\Scripts\python.exe -m src.pipeline
```

It loads Batch 1 (baseline), then applies Batches 2-5 in order, checking the
guardrails after every batch. It writes:

* `silver/` - `silver_billing_accounts`, `silver_plan_catalog`, `silver_invoices` (Parquet)
* `quarantine/` - `billing_accounts_quarantine`, `invoices_quarantine` (Parquet)
* `outputs/artifacts/pipeline_log.md` - one row per audited batch

Expected final log row:

```text
counts: silver_billing_accounts=110, silver_plan_catalog=6, silver_invoices=480,
billing_accounts_quarantine=2, invoices_quarantine=4 | final table counts
```

Re-running is safe and changes nothing (read-modify-write, idempotent).

## 5. Run the tests (Task 9)

```powershell
cmd /c ".venv\Scripts\python.exe" -m pytest -v > outputs\artifacts\pytest_report.md 2>&1
```

Expected: `26 passed`.

Windows note: use the `cmd /c ... > file 2>&1` form above. Piping pytest output
through `Tee-Object` (or leaving Spark's JVM attached to a console pipe) can look
like a hang because the JVM inherits the handle and block-buffers. The file
redirect avoids it. The suite starts one Spark session, runs the full pipeline
once and reuses those outputs, so expect roughly 8-12 minutes on a 12-core
Windows machine (the saved run took 8:28).

Speed knob: Spark starts one Python worker per task slot and worker startup
dominates the run. The suite defaults to `local[2]`; override it if you want to
trade memory for time:

```powershell
set TRB_SPARK_MASTER=local[4]
```

## 6. Verify the Parquet outputs

* One command: `.venv\Scripts\python.exe -m tools.verify_outputs`
  (no Spark needed; writes `outputs/artifacts/verify_outputs.md` and exits
  non-zero if any check fails).
* The same numbers as a ready-made dump: `outputs/artifacts/evidence_dump.md`
  (`python -m tools.evidence_dump`, this one uses Spark).
* Step-by-step pandas and PySpark recipes:
  `outputs/artifacts/how_to_verify_outputs.md`.

## 7. Where the assessment evidence is

| File | Contents |
| --- | --- |
| `Stack_AI_Rewire_Silver_Layer_Evaluation_Rubric.xlsx` | Sheet 1 "Rubric" (evaluator fills `Score Earned` / `Evaluator Notes`); Sheet 2 "Skill-Driven Evidence Log" - one row per skill task (2, 3, 5, 6, 7, 8) |
| `outputs/artifacts/skill_driven_evidence_log.md` | longer narrative: gap found, change made, how validated; includes the Batch 2 data note |
| `outputs/artifacts/pipeline_log.md` | per-batch counts, quarantine reasons, guardrail results |
| `outputs/artifacts/pytest_report.md` | saved pass/fail output of the pytest suite |
| `outputs/artifacts/evidence_dump.md` | counts, SCD2 history, entity split, quarantine rows, invariants |
| `outputs/artifacts/verify_outputs.md` | result of the one-command verification |
| `outputs/artifacts/rubric_compliance_check.md` | this project, checked row by row against the rubric |
| `.claude/skills/*/SKILL.md` | the five skills: one used as provided, two modified, two authored from scratch |
| `Helper Guide - Silver Layer.docx` | the provided helper guide, included for reference |

## 8. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `HADOOP_HOME and hadoop.home.dir are unset` warning, or `java.lang.UnsatisfiedLinkError: NativeIO$Windows` | Windows only: winutils missing - redo step 3 |
| `Python in worker has different version` | Set `PYSPARK_PYTHON` to the same interpreter (done automatically on Windows by `src/spark_session.py`; on Linux activate the venv before running) |
| `pip install pyspark` tries to build from source | Python version too new - use 3.10-3.12 |
| pytest appears stuck | Use the `cmd /c ... > file 2>&1` form from step 5 |
| `Path already exists` on a re-run | Shouldn't happen: all writes are overwrite + de-dup; see `src/ingest.py` |
| Leftover `java` processes after a crash | `Get-Process java | Stop-Process` (only if no IDE is using a JVM) |
