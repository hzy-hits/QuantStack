# Quant Stack Task Inventory

Date: 2026-05-09

This inventory mirrors the active user crontab captured in
`ops/crontab.legacy.snapshot`. It does not install or change cron.

| Task id | Schedule | Current cwd | Command | Sends email | Primary log |
| --- | --- | --- | --- | --- | --- |
| `us.premarket` | `0 20 * * 1-5` | `quant-research-v1` | `./scripts/run_full.sh --prod --premarket` | yes | `quant-research-v1/logs/cron_premarket.log` |
| `us.postmarket` | `0 5 * * 2-6` | `quant-research-v1` | `./scripts/run_full.sh --prod` | yes | `quant-research-v1/logs/cron_postmarket.log` |
| `cn.precompute_alpha` | `20 7 * * 1-5` | `quant-research-cn` | `QUANT_CN_REVIEW_BACKFILL_DAYS=7 bash scripts/precompute_alpha.sh` | no | `quant-research-cn/reports/logs/cron_precompute_alpha.log` |
| `cn.morning` | `30 8 * * 1-5` | `quant-stack` | `./target/release/quant-stack daily --markets cn --session morning ...` | yes | `quant-research-cn/reports/logs/cron_morning.log` |
| `cn.evening` | `0 18 * * 1-5` | `quant-stack` | `./target/release/quant-stack daily --markets cn --session evening ...` | yes | `quant-research-cn/reports/logs/cron_evening.log` |
| `weekly.us` | `30 9 * * 6` | `quant-research-v1` | `./scripts/run_weekly.sh` | yes | `quant-research-v1/logs/cron_weekly.log` |
| `weekly.cn` | `0 10 * * 6` | `quant-research-cn` | `bash scripts/weekly_pipeline.sh` | yes | `quant-research-cn/reports/logs/cron_weekly.log` |
| `factor.cn.daily` | `0 4 * * 1-5` | `factor-lab` | `bash scripts/daily_factors.sh --market cn` | no | `factor-lab/logs/daily_YYYYMMDD.log` |
| `factor.us.daily` | `0 9 * * 2-6` | `factor-lab` | `bash scripts/daily_factors.sh --market us` | no | `factor-lab/logs/daily_YYYYMMDD.log` |
| `paper.record` | `33 4 * * 2-6` | `factor-lab` | `python3 scripts/paper_trade.py record` | no | `factor-lab/logs/paper_YYYYMMDD.log` |
| `paper.evaluate` | `47 7 * * 2-6` | `factor-lab` | `python3 scripts/paper_trade.py evaluate` | no | `factor-lab/logs/paper_YYYYMMDD.log` |
| `paper.report` | `53 7 * * 2-6` | `factor-lab` | `python3 scripts/paper_trade.py report` | no | `factor-lab/logs/paper_YYYYMMDD.log` |
| `autoresearch.cn.morning` | `0 6 * * 1-5` | `factor-lab` | `bash scripts/autoresearch.sh --market cn` | no | `factor-lab/logs/autoresearch_YYYYMMDD.log` |
| `autoresearch.all.midday` | `0 10 * * 1-5` | `factor-lab` | `bash scripts/autoresearch.sh` | no | `factor-lab/logs/autoresearch_YYYYMMDD.log` |
| `autoresearch.all.afternoon` | `0 14 * * 1-5` | `factor-lab` | `bash scripts/autoresearch.sh` | no | `factor-lab/logs/autoresearch_YYYYMMDD.log` |
| `factor.maintenance.weekly` | `17 8 * * 6` | `factor-lab` | `python3 scripts/weekly_maintenance.py --days 250` | no | `factor-lab/logs/maintenance_YYYYMMDD.log` |
| `us.watchdog.reboot` | `@reboot` | `quant-research-v1` | `sleep 180 && uv run python scripts/cron_watchdog.py` | no | `quant-research-v1/logs/cron_watchdog.log` |
| `us.watchdog` | `12,27,42,57 * * * *` | `quant-research-v1` | `uv run python scripts/cron_watchdog.py` | no | `quant-research-v1/logs/cron_watchdog.log` |

The generated root-only crontab is `ops/crontab.quant-stack`. It should not be
installed until dry-run review passes.
