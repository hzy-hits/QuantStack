#!/usr/bin/env bash
# Idempotent Oracle (Ampere A1, aarch64, Ubuntu 24.04) provisioning for quant-stack.
# Phase 2a of docs/superpowers/specs/2026-06-24-portability-refactor-design.md.
#
# Installs the toolchain + aligns TZ/Python/DuckDB with the WSL source of truth:
#   - System TZ  -> Asia/Shanghai  (cron schedules are authored in CST wall-clock)
#   - Python     -> 3.11           (matches quant-research-v1/.venv = 3.11.13)
#   - DuckDB py  -> 1.4.4          (matches WSL DB file format)
#   - Rust       -> stable aarch64 (builds quant-cn + quant-stack from source)
#
# Run AFTER the repo is cloned, FROM the repo root:
#   bash deploy/oracle/provision.sh
# Re-runnable; each step checks before acting.
set -euo pipefail

TZ_WANT="Asia/Shanghai"
PY_WANT="3.11"
DUCKDB_WANT="1.4.4"

log() { printf '\n=== %s ===\n' "$*"; }

log "1/6 system TZ -> ${TZ_WANT}"
# Authoritative check via timedatectl (cron uses /etc/localtime, which set-timezone updates).
CUR_TZ="$(timedatectl show -p Timezone --value 2>/dev/null || true)"
if [ "${CUR_TZ}" != "${TZ_WANT}" ]; then
  sudo timedatectl set-timezone "${TZ_WANT}"
fi
# Keep the Debian-legacy /etc/timezone text file consistent (some tools read it).
echo "${TZ_WANT}" | sudo tee /etc/timezone >/dev/null
echo "tz now: $(timedatectl | grep -i 'time zone' | xargs)"

log "2/6 apt build/runtime deps"
# Fresh cloud VMs run unattended-upgrades on boot — wait for the dpkg/apt lock first.
for _ in $(seq 1 90); do
  if sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 \
     || sudo fuser /var/lib/apt/lists/lock >/dev/null 2>&1; then
    echo "apt lock held, waiting 10s..."; sleep 10
  else break; fi
done
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  build-essential pkg-config libssl-dev ca-certificates curl git unzip \
  python3-venv tzdata
echo "apt deps ok"

log "3/6 uv (Python/venv manager)"
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
uv --version

log "4/6 Python ${PY_WANT} via uv"
uv python install "${PY_WANT}"
echo "python ${PY_WANT}: $(uv python find ${PY_WANT})"

log "5/6 Rust stable (aarch64)"
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
. "$HOME/.cargo/env"
rustc --version

log "6/6 reminders (NOT automated — see deploy/oracle/README.md)"
cat <<EOF
  - codex CLI: present on this host (narrator primary). 'codex login' if not authed.
  - Python venvs + DuckDB ${DUCKDB_WANT}: created by the per-market venv step in the runbook
      (uv venv --python ${PY_WANT} ... && uv pip install duckdb==${DUCKDB_WANT} ...).
  - Secrets (config.yaml / credentials.json / token.json / deepseek+tushare keys):
      hand-provision per deploy/oracle/README.md §Secrets — NEVER from git.
  - Hot data: migrate per runbook §Data (EXPORT on WSL -> transfer -> IMPORT here).
EOF
log "provision toolchain complete"
