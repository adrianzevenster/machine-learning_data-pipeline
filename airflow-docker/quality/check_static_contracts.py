import ast
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
AIRFLOW_ROOT = REPO_ROOT / "airflow-docker"

DAG_PATH = AIRFLOW_ROOT / "dags" / "docker_container_orchestration.py"
COMPOSE_PATH = AIRFLOW_ROOT / "docker-compose.yml"
SCHEMA_PATH = AIRFLOW_ROOT / "mysql" / "init" / "001_schema.sql"
CONTRACTS_PATH = AIRFLOW_ROOT / "quality" / "validate_mysql_tables.py"
DOCKERIGNORE_PATH = AIRFLOW_ROOT / ".dockerignore"
GITIGNORE_PATH = REPO_ROOT / ".gitignore"

REQUIRED_DAG_TASKS = {
    "validate_raw_data",
    "validate_processed_data",
    "validate_model_predictions",
    "run_monitoring",
}

REQUIRED_IMAGES = {
    "custom-airflow:latest",
    "python-app:latest",
    "pyspark-app:latest",
    "model-monitoring:latest",
}

EXTERNAL_IMAGES = {
    "busybox:1.36",
}

REQUIRED_SCHEMA_COLUMNS = {
    "Processed_Data": {
        "Date",
        "User",
        "M_Out_Call_Count",
        "M_Out_Call_Time",
        "M_Data_Sum",
        "M_Data_Count",
        "M_In_Call_Count",
        "M_In_Call_Time",
        "M_TENURE_CHURN",
    },
    "model_predictions": {
        "label",
        "prediction",
        "probability_0",
        "probability_1",
        "Date",
    },
}

REQUIRED_IGNORE_PATTERNS = {
    "__pycache__/",
    "*.py[cod]",
    "airflow-docker/logs/",
    "airflow-docker/output/",
    "pySpark/output/",
}

REQUIRED_DOCKERIGNORE_PATTERNS = {
    "logs/",
    "output/",
    "jars/",
    "**/__pycache__/",
    "**/*.py[cod]",
}


def read_text(path: Path) -> str:
    if not path.exists():
        raise AssertionError(f"Required file is missing: {path.relative_to(REPO_ROOT)}")
    return path.read_text(encoding="utf-8")


def load_table_contracts() -> dict:
    module = ast.parse(read_text(CONTRACTS_PATH), filename=str(CONTRACTS_PATH))
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "TABLE_CONTRACTS":
                    return ast.literal_eval(node.value)
    raise AssertionError("TABLE_CONTRACTS was not found.")


def extract_compose_images(compose_text: str) -> set[str]:
    return set(re.findall(r"^\s+image:\s+([^\s]+)\s*$", compose_text, flags=re.MULTILINE))


def extract_dag_images(dag_text: str) -> set[str]:
    return set(re.findall(r'image="([^"]+)"', dag_text))


def extract_schema_table_body(schema_text: str, table_name: str) -> str:
    pattern = rf"CREATE TABLE IF NOT EXISTS {re.escape(table_name)}\s*\((.*?)\);"
    match = re.search(pattern, schema_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise AssertionError(f"Schema does not create required table: {table_name}")
    return match.group(1)


def assert_compose_images() -> None:
    compose_text = read_text(COMPOSE_PATH)
    dag_text = read_text(DAG_PATH)

    compose_images = extract_compose_images(compose_text)
    missing_build_images = sorted(REQUIRED_IMAGES - compose_images)
    if missing_build_images:
        raise AssertionError(f"Compose is missing build targets for images: {missing_build_images}")

    dag_images = extract_dag_images(dag_text)
    missing_dag_images = sorted(dag_images - compose_images - EXTERNAL_IMAGES)
    if missing_dag_images:
        raise AssertionError(f"DAG references images not defined in compose: {missing_dag_images}")

    for service in ("python-app", "pyspark-app", "model-monitoring", "custom-airflow"):
        service_block = re.search(rf"^\s{{2}}{service}:\n(.*?)(?=^\s{{2}}\S|\Z)", compose_text, re.DOTALL | re.MULTILINE)
        if not service_block or 'profiles: ["build"]' not in service_block.group(1):
            raise AssertionError(f"{service} must stay behind the build profile.")


def assert_dag_quality_gates() -> None:
    dag_text = read_text(DAG_PATH)
    missing_tasks = sorted(task for task in REQUIRED_DAG_TASKS if f'task_id="{task}"' not in dag_text)
    if missing_tasks:
        raise AssertionError(f"DAG is missing quality/monitoring tasks: {missing_tasks}")

    required_order = (
        "validate_raw_data",
        "pyspark_analysis",
        "validate_processed_data",
        "pyspark_model",
        "validate_model_predictions",
        "run_monitoring",
    )
    positions = [dag_text.rfind(task) for task in required_order]
    if any(position < 0 for position in positions) or positions != sorted(positions):
        raise AssertionError("DAG quality gates are not ordered before their dependent tasks.")

    for expected in ("max_active_runs=1", "dagrun_timeout=timedelta(hours=4)"):
        if expected not in dag_text:
            raise AssertionError(f"DAG missing production guard: {expected}")


def assert_schema_matches_contracts() -> None:
    schema_text = read_text(SCHEMA_PATH)
    contracts = load_table_contracts()
    table_contracts = {
        contract["table"]: contract["required_columns"]
        for contract in contracts.values()
        if contract["table"] in REQUIRED_SCHEMA_COLUMNS
    }

    for table_name, required_columns in REQUIRED_SCHEMA_COLUMNS.items():
        schema_body = extract_schema_table_body(schema_text, table_name)
        missing_schema_columns = sorted(
            column for column in required_columns if not re.search(rf"\b`?{re.escape(column)}`?\b", schema_body)
        )
        if missing_schema_columns:
            raise AssertionError(f"{table_name} schema missing columns: {missing_schema_columns}")

        contract_columns = table_contracts.get(table_name)
        if not contract_columns:
            raise AssertionError(f"No data-quality contract found for {table_name}")
        missing_contract_columns = sorted(required_columns - set(contract_columns))
        if missing_contract_columns:
            raise AssertionError(f"{table_name} contract missing columns: {missing_contract_columns}")


def assert_ignore_hygiene() -> None:
    gitignore = set(read_text(GITIGNORE_PATH).splitlines())
    dockerignore = set(read_text(DOCKERIGNORE_PATH).splitlines())

    missing_gitignore = sorted(REQUIRED_IGNORE_PATTERNS - gitignore)
    if missing_gitignore:
        raise AssertionError(f".gitignore missing generated artifact patterns: {missing_gitignore}")

    missing_dockerignore = sorted(REQUIRED_DOCKERIGNORE_PATTERNS - dockerignore)
    if missing_dockerignore:
        raise AssertionError(f"airflow-docker/.dockerignore missing patterns: {missing_dockerignore}")


def main() -> int:
    checks = [
        assert_compose_images,
        assert_dag_quality_gates,
        assert_schema_matches_contracts,
        assert_ignore_hygiene,
    ]

    failures = []
    for check in checks:
        try:
            check()
            print(f"[quality-gate] PASS {check.__name__}")
        except Exception as exc:
            failures.append(f"[quality-gate] FAIL {check.__name__}: {exc}")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1

    print("[quality-gate] all static contracts passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
