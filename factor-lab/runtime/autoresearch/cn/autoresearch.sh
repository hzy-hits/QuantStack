#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/ivena/coding/quant-stack/factor-lab"
cd "$ROOT"

MARKET="cn"
FORMULA="${FORMULA:?set FORMULA='rank(...)'}"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

if ! uv run python eval_factor.py --market "$MARKET" --formula "$FORMULA" >"$TMP" 2>&1; then
  cat "$TMP"
  exit 1
fi

python - "$TMP" <<'PY'
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(errors="ignore")

def extract(name: str):
    m = re.search(rf"^{name}:\s+(.+)$", text, re.MULTILINE)
    return m.group(1).strip() if m else ""

is_ic = extract("is_ic")
is_ic_ir = extract("is_ic_ir")
gates = extract("gates")
max_corr = extract("max_corr")
print(f"METRIC is_ic={is_ic or 'nan'}")
print(f"METRIC is_ic_ir={is_ic_ir or 'nan'}")
print(f"METRIC max_corr={max_corr or 'nan'}")
print(f"METRIC gates_pass={1 if gates == 'PASS' else 0}")
PY

cat "$TMP"
