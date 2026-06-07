import importlib.util
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRACTS_PATH = REPO_ROOT / "quality" / "check_static_contracts.py"


def load_contracts_module():
    spec = importlib.util.spec_from_file_location("check_static_contracts", CONTRACTS_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_static_contracts_pass():
    contracts = load_contracts_module()

    contracts.assert_compose_images()
    contracts.assert_dag_quality_gates()
    contracts.assert_schema_matches_contracts()
    contracts.assert_ignore_hygiene()
    contracts.assert_runtime_hardening()
