import importlib.util
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_PATH = REPO_ROOT / "quality" / "run_mysql_migrations.py"


def load_migrations_module():
    spec = importlib.util.spec_from_file_location("run_mysql_migrations", MIGRATIONS_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_split_sql_handles_schema_file_statements():
    migrations = load_migrations_module()
    schema_path = REPO_ROOT / "mysql" / "migrations" / "001_create_core_tables.sql"

    statements = migrations.split_sql(schema_path.read_text(encoding="utf-8"))

    assert any("CREATE TABLE IF NOT EXISTS model_versions" in statement for statement in statements)
    assert any("CREATE TABLE IF NOT EXISTS monitoring_reports" in statement for statement in statements)
    assert statements[-1].startswith("INSERT IGNORE INTO schema_migrations")
