#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(TZ=Asia/Shanghai date +%Y-%m-%d-%H%M%S)"
OUT="$ROOT/reports/review_packets/$STAMP"

mkdir -p "$OUT"
cd "$ROOT"

git status --short --untracked-files=all > "$OUT/git_status.txt"
git diff --stat > "$OUT/git_diff_stat.txt"
git diff > "$OUT/git_diff.patch"

"${PYTHON_BIN:-python3}" - <<'PY' > "$OUT/risk_tags.md"
from __future__ import annotations

import re
import subprocess
from pathlib import Path

import yaml

root = Path.cwd()
mapping_path = root / "ops" / "risk_path_map.yaml"
mapping = yaml.safe_load(mapping_path.read_text(encoding="utf-8")) if mapping_path.exists() else {"rules": []}
changed = subprocess.check_output(["git", "status", "--short", "--untracked-files=all"], text=True).splitlines()
tags_by_path: dict[str, set[str]] = {}
for line in changed:
    path = line[3:].strip()
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    tags: set[str] = set()
    for rule in mapping.get("rules", []):
        if re.search(str(rule.get("pattern") or ""), path):
            tags.update(str(tag) for tag in rule.get("tags") or [])
    tags_by_path[path] = tags or {"unclassified"}

print("# Risk Tags")
print()
all_tags = sorted({tag for tags in tags_by_path.values() for tag in tags})
print("Tags: " + (", ".join(all_tags) if all_tags else "-"))
print()
for path, tags in sorted(tags_by_path.items()):
    print(f"- `{path}`: {', '.join(sorted(tags))}")
PY

{
    echo "# Changed Files By Project"
    echo
    git status --short --untracked-files=all | awk '
      {
        path=$2
        if (path ~ /^ops\//) group="ops"
        else if (path ~ /^scripts\//) group="shared scripts"
        else if (path ~ /^quant-research-v1\//) group="us producer"
        else if (path ~ /^quant-research-cn\//) group="cn producer"
        else if (path ~ /^factor-lab\//) group="factor lab"
        else if (path ~ /^docs\//) group="docs"
        else group="root/other"
        rows[group]=rows[group] "- " $0 "\n"
      }
      END {
        for (g in rows) {
          print "## " g
          printf "%s\n", rows[g]
        }
      }
    '
} > "$OUT/changed_files_by_project.md"

{
    echo "# Cron Tasks Affected"
    echo
    if git status --short --untracked-files=all | grep -E '^(.. )?ops/|^(.. )?crates/quant-stack-cli|^(.. )?quant-research-v1/scripts/run_full.sh|^(.. )?quant-research-cn/scripts|^(.. )?factor-lab/scripts' >/dev/null; then
        echo "Potentially affected. Review ops/tasks.yaml and generated crontab."
    else
        echo "No obvious cron/task-runner files changed."
    fi
    echo
    if [[ -f ops/crontab.quant-stack ]]; then
        echo "## Rendered crontab"
        echo
        sed -n '1,220p' ops/crontab.quant-stack
    fi
} > "$OUT/cron_tasks_affected.md"

cat > "$OUT/commands_run.md" <<'EOF'
# Commands Run

Fill this in before review if command output matters.
EOF

{
    echo "# AI Universe Production Basket Audit"
    echo
    audit_date="${REVIEW_PACKET_AUDIT_DATE:-$(TZ=Asia/Shanghai date +%Y-%m-%d)}"
    echo "- Date: $audit_date"
    if "${PYTHON_BIN:-python3}" scripts/audit_production_basket_ai_universe.py --as-of "$audit_date" 2>&1; then
        echo
        echo "Status: pass."
    else
        echo
        echo "Status: FAIL or missing report. Inspect reports/review_dashboard/main_strategy_v2/$audit_date/."
    fi
} > "$OUT/production_basket_audit.md"

cat > "$OUT/tests_run.md" <<'EOF'
# Tests Run

Fill this in before review.
EOF

cat > "$OUT/reports_generated.md" <<'EOF'
# Reports Generated

Fill this in before review.
EOF

{
    echo "# Risk Summary"
    echo
    tmp_cron="$OUT/current_crontab.txt"
    if crontab -l > "$tmp_cron" 2>"$OUT/current_crontab.err"; then
        if diff -q "$tmp_cron" ops/crontab.quant-stack >/dev/null 2>&1; then
            echo "- Production crontab currently matches ops/crontab.quant-stack: yes."
        else
            echo "- Production crontab currently matches ops/crontab.quant-stack: no."
            echo "- Current crontab captured at: current_crontab.txt."
        fi
    else
        echo "- Production crontab currently matches ops/crontab.quant-stack: unavailable in this runtime."
        echo "- Crontab read error captured at: current_crontab.err."
    fi
    echo "- Rollback crontab snapshot: ops/crontab.legacy.snapshot."
    echo "- Production delivery path changed: inspect git diff."
    echo "- Alpha/ranker/prompt changed: inspect changed_files_by_project.md and risk_tags.md."
    echo "- Risk tags: generated from ops/risk_path_map.yaml."
} > "$OUT/risk_summary.md"

echo "$OUT"
