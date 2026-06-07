# Quant Stack Ops

`ops/` is the root control surface for scheduled jobs. It does not replace
market producers; it standardizes how cron calls them.

## Files

- `tasks.yaml`: task registry that mirrors the current crontab.
- `run_task.sh`: stable cron entrypoint.
- `run_task.py`: lock/log/state runner.
- `render_cron.py`: generates `ops/crontab.quant-stack`.
- `crontab.legacy.snapshot`: current crontab snapshot before migration.
- `task_inventory.md`: human-readable inventory of scheduled jobs.
- `review_packet.sh`: creates a review handoff under `reports/review_packets/`.

Autoresearch is manual-only. It is intentionally excluded from
`tasks.yaml`/`crontab.quant-stack`; run `bash factor-lab/scripts/autoresearch.sh`
directly for scoped research reviews.

## Dry Run

```bash
ops/run_task.sh --list
ops/run_task.sh --dry-run cn.evening --date 2026-05-08
ops/render_cron.py --output ops/crontab.quant-stack
```

## Install

Do not install automatically. After reviewing `ops/crontab.quant-stack`, install
manually:

```bash
crontab ops/crontab.quant-stack
```

Rollback:

```bash
crontab ops/crontab.legacy.snapshot
```
