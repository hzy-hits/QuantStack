# Secrets & Auth Inventory — Migration Reference

Date: 2026-06-25 · Scope: every local-only credential/auth artifact the system
needs to run, what migration must carry, and current backup status.

**No secret values are recorded here — only file locations and handling.**

## Inventory

| Artifact | Contains | Location | In git? | Backed up? | Migration handling |
|---|---|---|---|---|---|
| `quant-research-v1/config.yaml` | `finnhub_key`, `fred_key`, `anthropic_key`, 15 report recipients | repo (gitignored) | no | **no** | copy verbatim, `chmod 600` |
| `quant-research-cn/config.yaml` | `deepseek_key`, `tushare_token`, 14 recipients | repo (gitignored) | no | **no** | copy verbatim, `chmod 600` |
| `quant-research-v1/credentials.json` | Gmail OAuth **client** (durable) | repo (gitignored) | no | **no** | copy verbatim |
| `quant-research-v1/token.json` | Gmail OAuth **token** (auto-refreshes / rewritten on each send) | repo (gitignored) | no | **no** | copy; will refresh on new host |
| `quant-research-cn/credentials.json` | symlink → `../quant-research-v1/credentials.json` | repo | no | n/a | recreate symlink on target (`ln -sfn`) |
| `quant-research-cn/token.json` | symlink → `../quant-research-v1/token.json` | repo | no | n/a | recreate symlink on target |
| `~/.codex/auth.json` + `~/.codex/config.toml` | **Codex/ChatGPT subscription auth** — narrator PRIMARY backend | home | no | **no** | copy, OR `codex login` (device flow) on headless host |
| `~/.claude/.credentials.json` + `~/.claude.json` | Claude auth | home | no | **no** | copy, OR re-login |
| `~/.ssh/id_ed25519` (+ `.pub`, `config`, `known_hosts`) | GitHub (`git@`) + future NAS/Oracle SSH | home | no | **no** | generate/copy a key for the target host; add to GitHub + NAS/Oracle |

## Two risks to act on

1. **Zero backups today.** `~/migration_backups/` holds only crontab + DB copies — **no secrets**. A disk loss on this WSL box loses Gmail OAuth, all API keys, and the Codex/Claude subscription auth. None are in git; API keys would need re-issuing, OAuth re-authorizing. Make an off-machine encrypted backup independent of the migration.
2. **`~/.codex` + `~/.claude` are missing from the migration checklist's "Migrate Secrets" phase.** The report narrator runs on **codex (primary) → DeepSeek (fallback)**; without Codex auth on the target, narration silently falls back to DeepSeek (or fails if fallback is off). Treat Codex/Claude CLI auth as first-class migration secrets.

## Handling notes

- **Durable vs refreshable:** `credentials.json` (OAuth client) is durable; `token.json` is refreshed at runtime (see `gmail_token_saved` in `ops/logs/us.postmarket.log`). Copy both; the token re-mints on the new host.
- **Keys live only in `config.yaml`** (not env vars) — confirmed: `finnhub_key`/`fred_key`/`anthropic_key` (US), `deepseek_key`/`tushare_token` (CN). `deepseek_key` is reused by the narrator's DeepSeek fallback.
- **Permissions:** every copied secret → `chmod 600`; never echo into logs; never commit (all are gitignored — keep it that way).
- **conda:** PATH includes conda but runtime uses `uv` (pyproject + uv.lock); conda hits are docs/prompts only. Do **not** bulk-copy `~/miniconda3`; verify per-subproject before assuming a conda dependency.

## Off-machine backup (recommended one-liner shape)

Bundle the local-only secrets into a single encrypted archive and move it off this
host (to NAS or external), independent of the code/data migration:

```
tar -czf - \
  quant-research-v1/config.yaml quant-research-v1/credentials.json quant-research-v1/token.json \
  quant-research-cn/config.yaml \
  -C ~ .codex .claude .ssh/id_ed25519 \
  | gpg -c > quant-stack-secrets-$(date +%Y%m%d).tar.gz.gpg
```

Store the passphrase separately from the archive.
