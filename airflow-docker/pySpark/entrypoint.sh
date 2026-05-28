set -euo pipefail

CFG=/app/config.json
HOST="${MYSQL_HOST:-local-mysql}"

python - <<PY
import json, os, re
cfg = "${CFG}"
host = os.environ.get("MYSQL_HOST","local-mysql")

def fix(s: str) -> str:
    # replace hostname in any jdbc:mysql://HOST:...
    s2 = re.sub(r"(?<=jdbc:mysql://)([^:/?#]+)", host, s)
    return s2.replace("flaskapp-db", host)

with open(cfg) as f:
    d = json.load(f)

def walk(o):
    if isinstance(o, dict):
        for k, v in list(o.items()):
            if isinstance(v, (dict, list)):
                walk(v)
            elif isinstance(v, str):
                o[k] = fix(v)
        for k in ("host","hostname","db_host","mysql_host"):
            if k in o:
                o[k] = host
        for k in ("url","jdbc_url","jdbcUrl","connection","connection_string"):
            if k in o and isinstance(o[k], str):
                o[k] = fix(o[k])
    elif isinstance(o, list):
        for i, v in enumerate(o):
            if isinstance(v, (dict, list)):
                walk(v)
            elif isinstance(v, str):
                o[i] = fix(v)

walk(d)
with open(cfg, "w") as f:
    json.dump(d, f)
print("Patched config.json -> host =", host)
PY

exec "$@"
