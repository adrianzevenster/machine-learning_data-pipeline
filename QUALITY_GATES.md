# Quality Gates

This repository now has two layers of quality gates.

## Local And CI Gates

Run before pushing:

```bash
make quality
```

This fails on:

- Python syntax errors in the Airflow DAG, Spark jobs, EDA, monitoring, Flask ingestion, and quality scripts.
- Invalid Docker Compose configuration.
- DAG Docker images that do not have matching Compose build targets.
- Batch job services that are not protected by the `build` profile.
- Missing DAG data-quality gates before model training or monitoring.
- Drift between MySQL schema initialization and runtime data contracts.
- Missing ignore rules for generated runtime artifacts.

The GitHub Actions CI workflow runs the same static gates and then builds the orchestration images.

## Runtime DAG Gates

The Airflow DAG fails fast at these checkpoints:

- `validate_raw_data`: verifies `DP_CDR_Data` exists, has required columns, has rows, and has non-null dates.
- `validate_processed_data`: verifies `Processed_Data` exists, has required model columns, has rows, and has at least two label classes.
- `validate_model_predictions`: verifies `model_predictions` exists, has prediction/probability columns, has rows, and has at least two label classes.

Monitoring only runs after all upstream data contracts pass.
