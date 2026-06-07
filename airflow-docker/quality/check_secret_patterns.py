from pathlib import Path


AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = AIRFLOW_ROOT.parent

SKIP_DIRS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    "logs",
    "output",
}

SKIP_FILES = {
    "RawData.csv",
}

FORBIDDEN_PATTERNS = {
    "legacy_mysql_password": "a?" + "xBVq1!",
    "airflow_default_password_arg": "--password " + "airflow",
    "flask_debug_hardcoded": "app.run(" + "debug=True",
    "mysql_root_runtime_user": "MYSQL_USER=" + "root",
    "db_root_runtime_user": "DB_USER=" + "root",
}


def iter_text_files():
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.name in SKIP_FILES:
            continue
        try:
            yield path, path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue


def main():
    failures = []
    for path, text in iter_text_files():
        relative_path = path.relative_to(REPO_ROOT)
        for name, pattern in FORBIDDEN_PATTERNS.items():
            if path.name == "check_secret_patterns.py":
                continue
            if pattern in text:
                failures.append(f"{relative_path}: contains forbidden pattern {name}")

    if failures:
        print("\n".join(failures))
        return 1

    print("[quality-gate] secret pattern scan passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
