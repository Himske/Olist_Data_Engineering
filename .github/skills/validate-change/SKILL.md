---
name: validate-change
description: "Inspect a code change in this Olist Spark/Airflow repository, run focused tests and lint checks, and report exact outcomes."
---

# Validate a Change

Inspect the current change, select focused checks that cover it, run the applicable checks, and report their exact outcomes. Do not change application code or dependencies as part of validation.

## Procedure

1. Inspect the worktree and the change diff. Identify changed files and the behavior they affect; do not attribute unrelated pre-existing changes to this change.
2. From the repository root, find relevant tests under `tests/` or alongside the changed code. Run the narrowest applicable tests with `python -m pytest <test-file-or-directory> -v`. For broad changes, or when no narrower selection is reliable, run `python -m pytest tests -v`.
3. If there are no applicable tests, do not report a successful test run. Mark tests as **not run** and say no relevant tests were found.
4. For changed Python files, run Ruff with `ruff check <changed-python-files>`. Ruff is a project dependency configured in `pyproject.toml`. If Ruff cannot be run, mark lint as **not run** and explain why. Do not invent a linter for file types without a configured project check.
5. Tests in `tests/` mock PySpark; they do not prove Spark/Azurite or a complete Airflow deployment works. For storage or ingestion changes, when the required runtime is available, also run the README's single-dataset `spark-submit` flow and verify the resulting Parquet in Azurite. Report when this integration check cannot be run.
6. Keep checks isolated from external services and persistent data. Never use real credentials; use local test fixtures or mocks. Do not run database-population or destructive maintenance commands as validation.
7. Respect the project's current scope: this is a Bronze ingestion starter; Silver, dbt, and a complete Airflow deployment are not implemented. Do not claim coverage or runtime capabilities that the repository does not have.
8. Report each check under **Passed**, **Failed**, or **Not run**. Include the command, concise results, and any failure details. Do not describe a skipped or unavailable check as passing.
