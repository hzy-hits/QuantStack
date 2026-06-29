# Quant Stack Task Inventory

Date: 2026-05-09

This inventory mirrors the active user crontab captured in
`ops/crontab.legacy.snapshot`. It does not install or change cron.

| Task id | Schedule | Current cwd | Command | Sends email | Primary log |
| --- | --- | --- | --- | --- | --- |
| `us.premarket` | manual only | `quant-research-v1` | `./scripts/run_full.sh --prod --premarket` | no | `quant-research-v1/logs/cron_premarket.log` |
| `us.postmarket` | manual only | `quant-research-v1` | `./scripts/run_full.sh --prod` | no | `quant-research-v1/logs/cron_postmarket.log` |
| `cn.precompute_alpha` | `20 7 * * 1-5` | `quant-research-cn` | `QUANT_CN_REVIEW_BACKFILL_DAYS=7 bash scripts/precompute_alpha.sh` | no | `quant-research-cn/reports/logs/cron_precompute_alpha.log` |
| `cn.morning` | manual only | `quant-stack` | `./target/release/quant-stack daily --markets cn --session morning ...` | no | `quant-research-cn/reports/logs/cron_morning.log` |
| `cn.evening` | manual only | `quant-stack` | `./target/release/quant-stack daily --markets cn --session evening ...` | no | `quant-research-cn/reports/logs/cron_evening.log` |
| `weekly.us` | manual only | `quant-research-v1` | `./scripts/run_weekly.sh` | no | `quant-research-v1/logs/cron_weekly.log` |
| `weekly.cn` | manual only | `quant-research-cn` | `bash scripts/weekly_pipeline.sh` | no | `quant-research-cn/reports/logs/cron_weekly.log` |
| `us.watchdog.reboot` | `@reboot` | `quant-research-v1` | `sleep 180 && uv run python scripts/cron_watchdog.py` | no | `quant-research-v1/logs/cron_watchdog.log` |
| `us.watchdog` | `12,27,42,57 * * * *` | `quant-research-v1` | `uv run python scripts/cron_watchdog.py` | no | `quant-research-v1/logs/cron_watchdog.log` |

The generated root-only crontab is `ops/crontab.quant-stack`. It should not be
installed until dry-run review passes.

Autoresearch is intentionally not scheduled. Run `bash factor-lab/scripts/autoresearch.sh`
manually for scoped research reviews only.
