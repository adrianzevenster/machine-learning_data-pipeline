import os
from pathlib import Path


MIGRATIONS_DIR = Path(os.getenv("MYSQL_MIGRATIONS_DIR", "/app/mysql/migrations"))

TABLE_COLUMN_UPGRADES = {
    "pipeline_runs": {
        "run_id": "VARCHAR(250)",
        "dag_id": "VARCHAR(250)",
        "git_sha": "VARCHAR(64)",
        "image_tag": "VARCHAR(250)",
        "data_start": "DATE",
        "data_end": "DATE",
        "raw_row_count": "BIGINT",
        "processed_row_count": "BIGINT",
        "prediction_row_count": "BIGINT",
        "status": "VARCHAR(32) NOT NULL DEFAULT 'started'",
        "created_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        "updated_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    },
    "model_versions": {
        "model_version_id": "VARCHAR(250)",
        "run_id": "VARCHAR(250)",
        "model_name": "VARCHAR(128)",
        "algorithm": "VARCHAR(128)",
        "parameters_json": "TEXT",
        "metrics_json": "TEXT",
        "artifact_uri": "VARCHAR(512)",
        "mlflow_run_id": "VARCHAR(250)",
        "mlflow_model_uri": "VARCHAR(512)",
        "promotion_status": "VARCHAR(32) NOT NULL DEFAULT 'candidate'",
        "promotion_stage": "VARCHAR(32) NOT NULL DEFAULT 'None'",
        "promotion_reason": "TEXT",
        "promoted_at": "TIMESTAMP NULL",
        "created_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    },
    "model_predictions": {
        "pipeline_run_id": "VARCHAR(250)",
        "model_version_id": "VARCHAR(250)",
        "label": "DOUBLE",
        "prediction": "DOUBLE",
        "probability_0": "DOUBLE",
        "probability_1": "DOUBLE",
        "Date": "DATETIME",
    },
    "model_deployments": {
        "model_name": "VARCHAR(128)",
        "stage": "VARCHAR(32)",
        "model_version_id": "VARCHAR(250)",
        "mlflow_run_id": "VARCHAR(250)",
        "mlflow_model_uri": "VARCHAR(512)",
        "promoted_by_run_id": "VARCHAR(250)",
        "promoted_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    },
    "monitoring_reports": {
        "report_id": "BIGINT",
        "pipeline_run_id": "VARCHAR(250)",
        "model_version_id": "VARCHAR(250)",
        "reference_rows": "BIGINT",
        "analysis_rows": "BIGINT",
        "metrics_path": "VARCHAR(512)",
        "plot_path": "VARCHAR(512)",
        "created_at": "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
    },
}


def mysql_config(include_database=True):
    config = {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "user": os.getenv("MYSQL_USER", "spark"),
        "password": os.getenv("MYSQL_PASSWORD", "sparkpw"),
    }
    if include_database:
        config["database"] = os.getenv("MYSQL_DATABASE", "RawData")
    return config


def split_sql(script):
    statements = []
    current = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).rstrip(";"))
            current = []
    if current:
        statements.append("\n".join(current))
    return statements


def apply_sql_migrations(cursor):
    if not MIGRATIONS_DIR.exists():
        raise RuntimeError(f"Migration directory does not exist: {MIGRATIONS_DIR}")

    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        version = path.stem.split("_", 1)[0]

        print(f"[migrations] applying idempotent statements from {path.name}")
        for statement in split_sql(path.read_text(encoding="utf-8")):
            normalized = statement.strip().lower()
            if normalized.startswith("create database") or normalized.startswith("use "):
                continue
            cursor.execute(statement)
        cursor.execute(
            """
            INSERT IGNORE INTO schema_migrations (version, description)
            VALUES (%s, %s)
            """,
            (version, path.stem),
        )


def table_exists(cursor, table_name):
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        """,
        (table_name,),
    )
    return cursor.fetchone()[0] > 0


def existing_columns(cursor, table_name):
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cursor.fetchall()}


def apply_column_upgrades(cursor):
    for table_name, columns in TABLE_COLUMN_UPGRADES.items():
        if not table_exists(cursor, table_name):
            continue

        present = existing_columns(cursor, table_name)
        for column_name, column_definition in columns.items():
            if column_name in present:
                continue
            print(f"[migrations] adding {table_name}.{column_name}")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")


def main():
    import mysql.connector

    with mysql.connector.connect(**mysql_config()) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(32) PRIMARY KEY,
                description VARCHAR(255) NOT NULL,
                applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        apply_sql_migrations(cursor)
        apply_column_upgrades(cursor)
        conn.commit()
    print("[migrations] complete")


if __name__ == "__main__":
    main()
