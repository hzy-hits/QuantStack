# Oracle / NAS / Raspberry Pi Migration Checklist - 2026-06-24

Purpose: move quant-stack production execution off WSL and into a more reliable
always-on architecture.

Current source of truth during planning:

- Active stack root: `/home/ivena/coding/quant-stack`
- Current root disk: 1007G total, 174G used, 782G available
- Active task registry: `ops/tasks.yaml`
- Active crontab: generated from `ops/tasks.yaml`, runs `ops/run_task.sh`
- Current large active data:
  - `quant-research-v1/data`: 45G
  - `factor-lab/data`: 12G
  - `quant-research-cn/data`: 4.6G
  - `reports`: 3.7G
  - `data/strategy_backtest_history.duckdb`: 1.9G
- Do not use old standalone repos as production roots:
  - `/home/ivena/coding/python/quant-research-v1`
  - `/home/ivena/coding/rust/quant-research-cn`
  - `/home/ivena/coding/python/factor-lab`

## Target Architecture

| Role | Device | Responsibility |
|---|---|---|
| Production runner | Oracle VPS | Schedules, API ingest, DuckDB updates, report generation, delivery outbox |
| Cold archive | NAS | Historical DuckDB snapshots, old reports, logs, backups |
| Watchdog | Raspberry Pi | Check Oracle health, task freshness, NAS sync, alert if Oracle is down |
| Control plane | Cloudflare Free Worker | Status page, webhook trigger, latest report redirect, lightweight queue/status API |
| Development | WSL | Code changes, local tests, manual investigation only |

Cloudflare Worker Free is not the compute plane. It should not run Python,
Rust, DuckDB, full market data pulls, or report generation.

## Cloudflare Free Worker Scope

Recommended endpoints:

- `GET /status`
  - Return latest task state from KV or JSON pushed by Oracle.
- `GET /latest/us`
  - Redirect to latest US report URL, or return a small JSON payload.
- `GET /latest/cn`
  - Same for CN report.
- `POST /webhook/run`
  - Authenticated trigger that asks Oracle to run a named task.
- `POST /webhook/send-latest`
  - Authenticated trigger that asks Oracle outbox to resend the latest report.
- Scheduled Worker every 15 minutes
  - Ping Oracle health endpoint and write the result to KV.

Free-plan boundaries to design around:

- 100,000 Worker requests/day.
- 50 external subrequests per invocation.
- Cron and Queue consumers are not suitable for long CPU-heavy work.
- KV free storage is small and write-limited; store only status JSON, not data.
- R2 free storage is about 10GB-month; not enough for the current DuckDB archive.

## Migration Principles

1. Git is for code, not data.
2. Oracle gets hot production data only.
3. NAS gets cold historical data and backups.
4. WSL production crontab is disabled only after Oracle canary passes.
5. Secrets are copied explicitly and chmodded to `600`; never printed in logs.
6. DuckDB files are copied from a quiescent snapshot, not while writers are active.
7. Every destructive/cutover command has a rollback command.

## Phase 0 - Stop Making The Local Tree More Complex

- [ ] Decide migration window. Best window: after US postmarket/report delivery and before CN early-morning ingest.
- [ ] Ensure no active local production task is running:

```bash
ps -eo pid,ppid,lstart,etime,cmd \
  | rg 'ops/run_task|quant-stack daily|quant-stack us-daily|daily_factors|run_full|send_production'
```

- [ ] Save local crontab before any cutover:

```bash
mkdir -p /home/ivena/migration_backups
crontab -l > /home/ivena/migration_backups/crontab.quant-stack.$(date +%Y%m%d_%H%M%S).bak
```

- [ ] Record current git state:

```bash
cd /home/ivena/coding/quant-stack
git status --short > /home/ivena/migration_backups/quant-stack.git-status.$(date +%Y%m%d_%H%M%S).txt
git diff --stat > /home/ivena/migration_backups/quant-stack.diff-stat.$(date +%Y%m%d_%H%M%S).txt
```

Current dirty work includes production-relevant fixes in:

- `ops/run_task.py`
- `quant-research-v1/src/quant_bot/delivery/gmail.py`
- `quant-research-v1/src/quant_bot/orchestration/watchdog.py`
- `quant-research-v1/tests/test_cron_watchdog.py`
- `scripts/ingest_cn_flow_signals.py`
- `scripts/verify_cn_ai_evidence.py`

These must be committed, patched, or explicitly rsynced before Oracle becomes production.

## Phase 1 - Oracle Host Bootstrap

Assumed target layout:

```text
/srv/quant-stack              # code + hot runtime
/srv/quant-stack-secrets      # optional root-owned secret staging
/var/log/quant-stack          # optional external logs
/var/backups/quant-stack      # local short backups before NAS sync
```

Create user and base directories on Oracle:

```bash
sudo useradd --create-home --shell /bin/bash quant || true
sudo mkdir -p /srv/quant-stack /srv/quant-stack-secrets /var/log/quant-stack /var/backups/quant-stack
sudo chown -R quant:quant /srv/quant-stack /srv/quant-stack-secrets /var/log/quant-stack /var/backups/quant-stack
sudo chmod 700 /srv/quant-stack-secrets
```

Install system dependencies:

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential curl git pkg-config libssl-dev ca-certificates \
  python3 python3-venv python3-pip rsync unzip jq sqlite3
```

Install Rust and uv for the `quant` user:

```bash
sudo -iu quant
curl https://sh.rustup.rs -sSf | sh -s -- -y
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Phase 2 - Deploy Code To Oracle

Preferred path: push/commit local changes, then clone on Oracle.

```bash
sudo -iu quant
cd /srv
git clone <YOUR_QUANT_STACK_REMOTE> quant-stack
cd /srv/quant-stack
git status --short
```

If dirty local changes are not committed yet, use a temporary rsync overlay
after clone. Run dry-run first:

```bash
export ORACLE_HOST=<oracle-user-or-quant>@<oracle-host>
export ORACLE_ROOT=/srv/quant-stack

rsync -a --dry-run --delete \
  --exclude '.git/' \
  --exclude 'target/' \
  --exclude '**/target/' \
  --exclude '.venv/' \
  --exclude '**/.venv/' \
  --exclude '__pycache__/' \
  --exclude '**/__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude 'data/' \
  --exclude 'reports/' \
  --exclude 'ops/logs/' \
  --exclude 'ops/state/' \
  /home/ivena/coding/quant-stack/ \
  "$ORACLE_HOST:$ORACLE_ROOT/"
```

Execute only after reviewing dry-run output:

```bash
rsync -a --delete \
  --exclude '.git/' \
  --exclude 'target/' \
  --exclude '**/target/' \
  --exclude '.venv/' \
  --exclude '**/.venv/' \
  --exclude '__pycache__/' \
  --exclude '**/__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude 'data/' \
  --exclude 'reports/' \
  --exclude 'ops/logs/' \
  --exclude 'ops/state/' \
  /home/ivena/coding/quant-stack/ \
  "$ORACLE_HOST:$ORACLE_ROOT/"
```

## Phase 3 - Build Runtime On Oracle

On Oracle:

```bash
sudo -iu quant
cd /srv/quant-stack

# Rust release binary
cargo build --release

# Python dependencies: prefer project-local envs, not global system Python.
cd /srv/quant-stack/quant-research-v1
uv sync

cd /srv/quant-stack/factor-lab
uv sync || python3 -m venv .venv
```

If a subproject still expects conda, document it before installing conda on Oracle.
Do not blindly copy `/home/ivena/miniconda3`.

## Phase 4 - Migrate Secrets

Secrets to copy from active stack only:

```text
/home/ivena/coding/quant-stack/quant-research-v1/config.yaml
/home/ivena/coding/quant-stack/quant-research-v1/token.json
/home/ivena/coding/quant-stack/quant-research-v1/credentials.json
/home/ivena/coding/quant-stack/quant-research-cn/config.yaml
```

Current CN token/credentials are relative symlinks to the active US directory:

```text
quant-research-cn/token.json -> ../quant-research-v1/token.json
quant-research-cn/credentials.json -> ../quant-research-v1/credentials.json
```

Copy command:

```bash
export ORACLE_HOST=<oracle-user-or-quant>@<oracle-host>
export ORACLE_ROOT=/srv/quant-stack

rsync -a --relative \
  /home/ivena/coding/quant-stack/./quant-research-v1/config.yaml \
  /home/ivena/coding/quant-stack/./quant-research-v1/token.json \
  /home/ivena/coding/quant-stack/./quant-research-v1/credentials.json \
  /home/ivena/coding/quant-stack/./quant-research-cn/config.yaml \
  "$ORACLE_HOST:$ORACLE_ROOT/"

ssh "$ORACLE_HOST" '
  chmod 600 /srv/quant-stack/quant-research-v1/config.yaml \
            /srv/quant-stack/quant-research-v1/token.json \
            /srv/quant-stack/quant-research-v1/credentials.json \
            /srv/quant-stack/quant-research-cn/config.yaml
  cd /srv/quant-stack/quant-research-cn
  ln -sfn ../quant-research-v1/token.json token.json
  ln -sfn ../quant-research-v1/credentials.json credentials.json
'
```

Validation:

```bash
ssh "$ORACLE_HOST" '
  stat -c "%N %a %U:%G %s" \
    /srv/quant-stack/quant-research-v1/config.yaml \
    /srv/quant-stack/quant-research-v1/token.json \
    /srv/quant-stack/quant-research-v1/credentials.json \
    /srv/quant-stack/quant-research-cn/config.yaml \
    /srv/quant-stack/quant-research-cn/token.json \
    /srv/quant-stack/quant-research-cn/credentials.json
'
```

## Phase 5 - Seed Hot Data To Oracle

Hot data should be enough to run tomorrow's pipelines without rehydrating all
history.

Copy these first:

| Source | Target | Why |
|---|---|---|
| `quant-research-v1/data/quant.duckdb` | Oracle hot | Active US DB |
| `quant-research-v1/data/quant_report.duckdb` | Oracle hot | Active US report DB |
| Latest `quant_research_YYYY-MM-DD_post.duckdb` | Oracle hot optional | Recent session debug/replay |
| Latest `quant_report_YYYY-MM-DD_post.duckdb` | Oracle hot optional | Recent report replay |
| `quant-research-cn/data/quant_cn.duckdb` | Oracle hot | Active CN DB |
| `quant-research-cn/data/quant_cn_report.duckdb` | Oracle hot | Active CN report DB |
| `quant-research-cn/data/quant_cn_research.duckdb` | Oracle hot | CN research DB |
| `factor-lab/data/factor_lab.duckdb` | Oracle hot | Factor Lab state |
| `data/strategy_backtest_history.duckdb` | Oracle hot | Strategy history |
| `ai_infra/data` | Oracle hot | Production universe/evidence state |

Freeze writers before copying DuckDB files:

```bash
ps -eo pid,ppid,lstart,etime,cmd \
  | rg 'ops/run_task|duckdb|quant-stack daily|quant-stack us-daily|daily_factors|run_full' \
  | rg -v 'rg '
```

Hot-data dry-run:

```bash
export ORACLE_HOST=<oracle-user-or-quant>@<oracle-host>
export ORACLE_ROOT=/srv/quant-stack

rsync -a --dry-run --info=progress2 --relative \
  /home/ivena/coding/quant-stack/./quant-research-v1/data/quant.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-v1/data/quant_report.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-cn/data/quant_cn.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-cn/data/quant_cn_report.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-cn/data/quant_cn_research.duckdb \
  /home/ivena/coding/quant-stack/./factor-lab/data/factor_lab.duckdb \
  /home/ivena/coding/quant-stack/./data/strategy_backtest_history.duckdb \
  /home/ivena/coding/quant-stack/./ai_infra/data/ \
  "$ORACLE_HOST:$ORACLE_ROOT/"
```

Execute after dry-run review:

```bash
rsync -a --info=progress2 --relative \
  /home/ivena/coding/quant-stack/./quant-research-v1/data/quant.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-v1/data/quant_report.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-cn/data/quant_cn.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-cn/data/quant_cn_report.duckdb \
  /home/ivena/coding/quant-stack/./quant-research-cn/data/quant_cn_research.duckdb \
  /home/ivena/coding/quant-stack/./factor-lab/data/factor_lab.duckdb \
  /home/ivena/coding/quant-stack/./data/strategy_backtest_history.duckdb \
  /home/ivena/coding/quant-stack/./ai_infra/data/ \
  "$ORACLE_HOST:$ORACLE_ROOT/"
```

Optional: seed latest session snapshots for immediate debugging:

```bash
find /home/ivena/coding/quant-stack/quant-research-v1/data \
  -maxdepth 1 -type f \
  \( -name 'quant_research_2026-06-23_post.duckdb' -o -name 'quant_report_2026-06-23_post.duckdb' \) \
  -print
```

## Phase 6 - Archive Cold Data To NAS

NAS target layout:

```text
<NAS_ROOT>/quant-stack/
  cold-data/
    quant-research-v1/
    quant-research-cn/
    factor-lab/
  reports/
  logs/
  old-standalone-repos/
  backups/
```

Cold archive candidates:

- `quant-research-v1/data/quant_*_YYYY-MM-DD_*.duckdb` snapshots.
- `quant-research-v1/reports`.
- `quant-research-cn/reports`.
- `reports/review_dashboard`.
- `ops/logs` and legacy logs.
- Old standalone repo data:
  - `/home/ivena/coding/python/quant-research-v1/data` (24G)
  - `/home/ivena/coding/rust/quant-research-cn/data` (2G)
  - `/home/ivena/coding/python/factor-lab/reports` (2G)

Dry-run:

```bash
export NAS_ROOT=/mnt/nas/quant-stack

rsync -a --dry-run --info=progress2 \
  /home/ivena/coding/quant-stack/quant-research-v1/data/ \
  "$NAS_ROOT/cold-data/quant-research-v1/data/"

rsync -a --dry-run --info=progress2 \
  /home/ivena/coding/quant-stack/reports/ \
  "$NAS_ROOT/reports/"
```

Execute only after NAS mount and dry-run are verified.

## Phase 7 - Oracle Validation Before Cutover

On Oracle:

```bash
sudo -iu quant
cd /srv/quant-stack

export QUANT_STACK_ROOT=/srv/quant-stack

python3 ops/run_task.py --list
python3 ops/run_task.py us.postmarket --dry-run
python3 ops/run_task.py cn.morning --dry-run
python3 -m py_compile ops/run_task.py scripts/ingest_cn_flow_signals.py scripts/verify_cn_ai_evidence.py
```

Validate DuckDB files are readable:

```bash
python3 - <<'PY'
from pathlib import Path
import duckdb

dbs = [
    "quant-research-v1/data/quant.duckdb",
    "quant-research-v1/data/quant_report.duckdb",
    "quant-research-cn/data/quant_cn.duckdb",
    "quant-research-cn/data/quant_cn_report.duckdb",
    "factor-lab/data/factor_lab.duckdb",
    "data/strategy_backtest_history.duckdb",
]
root = Path("/srv/quant-stack")
for rel in dbs:
    path = root / rel
    con = duckdb.connect(str(path), read_only=True)
    tables = con.execute("show tables").fetchall()
    con.close()
    print(rel, len(tables), "tables")
PY
```

Canary tasks, in order:

1. `research.fear_greed_ingest`
2. `research.cn_index_ingest`
3. `research.cn_flow_signals`
4. `paper.report`
5. `us.postmarket --dry-run` or `send_production_decision_report.py --delivery-dry-run`

Do not enable Oracle production cron until canaries pass.

## Phase 8 - Install Oracle Scheduler

Option A: install crontab, same model as WSL:

```bash
cd /srv/quant-stack
# If ops/crontab.quant-stack is generated and reviewed:
crontab ops/crontab.quant-stack
crontab -l
```

Option B: systemd timer wrapper for the catch-up runner:

`/etc/systemd/system/quant-stack-catchup.service`

```ini
[Unit]
Description=Quant Stack catch-up runner

[Service]
Type=oneshot
User=quant
WorkingDirectory=/srv/quant-stack
Environment=QUANT_STACK_ROOT=/srv/quant-stack
ExecStart=/srv/quant-stack/ops/run_task.sh ops.catch_up
```

`/etc/systemd/system/quant-stack-catchup.timer`

```ini
[Unit]
Description=Run Quant Stack catch-up every 15 minutes

[Timer]
OnCalendar=*:05,20,35,50
Persistent=true

[Install]
WantedBy=timers.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now quant-stack-catchup.timer
systemctl list-timers 'quant-stack*'
```

Initial recommendation: use crontab first because it matches current production.
Move to systemd timers after the Oracle cutover stabilizes.

## Phase 9 - Cutover

Before cutover:

- [ ] Oracle canary tasks passed.
- [ ] Oracle can send a test report to `13502448752hzy@gmail.com`.
- [ ] Oracle writes `ops/state/*.last_success.json`.
- [ ] NAS cold archive dry-run reviewed.
- [ ] Local crontab backup exists.
- [ ] Rollback command is ready.

Disable local WSL production schedule:

```bash
crontab -l > /home/ivena/migration_backups/crontab.before-oracle-cutover.$(date +%Y%m%d_%H%M%S).bak
crontab -r
```

Enable Oracle schedule:

```bash
ssh "$ORACLE_HOST" 'cd /srv/quant-stack && crontab ops/crontab.quant-stack && crontab -l | head -n 40'
```

Post-cutover checks:

```bash
ssh "$ORACLE_HOST" '
  cd /srv/quant-stack
  tail -n 80 ops/logs/us.watchdog.log 2>/dev/null || true
  find ops/state -maxdepth 1 -type f -printf "%TY-%Tm-%Td %TH:%TM %s %p\n" | sort -r | head -n 30
'
```

## Phase 10 - Raspberry Pi Watchdog

Raspberry Pi should not run quant jobs. It should check:

- Oracle SSH reachable.
- Oracle HTTP health endpoint reachable.
- Latest `ops/state/us.postmarket.last_success.json` is fresh.
- Latest `ops/state/cn.morning.last_success.json` is fresh.
- NAS mount reachable.
- NAS received latest archive sync.

Suggested checks:

```bash
ssh quant@<oracle-host> 'test -d /srv/quant-stack && echo ok'
ssh quant@<oracle-host> 'find /srv/quant-stack/ops/state -name "*.last_success.json" -mmin -1440 | wc -l'
```

Alerts should go through a channel independent of Gmail if possible:

- Telegram bot
- Pushover
- ntfy
- Cloudflare Worker webhook that writes a visible status

## Phase 11 - Cloudflare Worker Control Plane

Minimum KV keys:

```text
status:latest
status:tasks
report:latest:us
report:latest:cn
incident:latest
```

Oracle pushes small status JSON:

```json
{
  "updated_at": "2026-06-24T12:00:00+08:00",
  "runner": "oracle",
  "tasks": {
    "us.postmarket": {"status": "success", "finished_at": "..."},
    "cn.morning": {"status": "success", "finished_at": "..."}
  },
  "reports": {
    "us_post": "https://...",
    "cn_morning": "https://..."
  }
}
```

Worker should not store DuckDB or full reports. Store only status and pointers.

## Phase 12 - Rollback

If Oracle fails before production cron is stable:

```bash
# On WSL
crontab /home/ivena/migration_backups/<saved-local-crontab-file>
crontab -l | head -n 40
```

If Oracle generated newer hot DBs and you must roll back data:

```bash
export ORACLE_HOST=<oracle-user-or-quant>@<oracle-host>
export ORACLE_ROOT=/srv/quant-stack

rsync -a --dry-run --info=progress2 \
  "$ORACLE_HOST:$ORACLE_ROOT/quant-research-v1/data/quant.duckdb" \
  /home/ivena/coding/quant-stack/quant-research-v1/data/
```

Do not overwrite local DBs without copying them to a timestamped backup first.

## What Not To Migrate

Do not migrate these to Oracle:

- `/home/ivena/coding/quant-stack/target`
- Any `target/` directory.
- Any `.venv/` directory.
- `/home/ivena/miniconda3`
- `/home/ivena/.cache`
- `/home/ivena/.npm`, `.cargo/registry`, `.rustup/toolchains` as raw copies.
- Old standalone repos as production roots.
- `ops/logs` older than the recent diagnostic window.
- `reports/review_dashboard` full history unless explicitly needed on Oracle.

## Acceptance Criteria

Migration is complete only when all are true:

- [ ] Oracle has current code and production patches.
- [ ] Oracle can build `target/release/quant-stack`.
- [ ] Oracle can read all hot DuckDB files.
- [ ] Oracle has secrets with permissions `600`.
- [ ] Oracle can run `python3 ops/run_task.py --list`.
- [ ] Oracle can complete at least one non-email canary task.
- [ ] Oracle can send one explicit test email.
- [ ] Oracle writes `ops/state/*.last_success.json`.
- [ ] NAS has a verified cold-data archive copy.
- [ ] WSL production crontab is disabled.
- [ ] Oracle schedule is enabled.
- [ ] Raspberry Pi or Cloudflare status can detect a missed report.
- [ ] Rollback crontab and data-backup commands are documented and tested in dry-run.

## Open Decisions

- [ ] Oracle hostname/user and SSH key path.
- [ ] NAS mount path from Oracle and from WSL.
- [ ] Whether NAS is mounted over NFS, SMB, or rsync-over-SSH.
- [ ] Whether Cloudflare stores report pointers to NAS, Oracle, R2, or a private static endpoint.
- [ ] Whether Gmail remains the primary delivery channel or becomes one of several outbox transports.
- [ ] Whether to commit current dirty production fixes before migration or rsync a working-tree overlay.
