# Oracle 迁移 Runbook — Phase 2a(热算 / 投递 cutover)

> 目标:把生产 quant-stack 从 WSL 迁到 Oracle Ampere A1(aarch64, Ubuntu 24.04)。
> Oracle 成为活 DuckDB **唯一写者** + 算 + 投递;WSL 转只读兜底直到 Oracle 连续稳定 N 天。
> NAS 冷湖 + Pi broker(2b)不在本 runbook,随后做。

**代码 from git · 数据 from 迁移脚本 · 密钥 from 手工 provision。**

## 0. 对齐事实(已勘查,baked into provision.sh)
- Oracle:aarch64 / Ubuntu 24.04.4 / 4 核 23GiB / root 31G avail / **TZ=UTC(要改)** / codex-cli 在、Rust+uv+duckdb 缺。
- 必须对齐:**系统 TZ → Asia/Shanghai**(cron 按本地时间触发)、**Python 3.11**(配 .venv 3.11.13)、**DuckDB 1.4.4**(配库文件格式)。

## 1. 仓库上机(需 GitHub 鉴权 — 你做)
私有仓库 `git@github.com:hzy-hits/QuantStack.git`。在 Oracle 上二选一:
- **deploy key**:`ssh-keygen -t ed25519 -f ~/.ssh/gh_deploy -N ""`,把 `~/.ssh/gh_deploy.pub` 加到 GitHub 仓库 Deploy Keys(read-only 即可),`~/.ssh/config` 配 `Host github.com / IdentityFile ~/.ssh/gh_deploy`。
- 或 HTTPS + PAT。
然后:`git clone git@github.com:hzy-hits/QuantStack.git ~/quant-stack && cd ~/quant-stack`(默认 `main` 即当前权威主干,已含全部迁移代码)。

## 2. Provision 工具链(幂等)
```bash
cd ~/quant-stack && bash deploy/oracle/provision.sh
```
装:TZ、apt 依赖、uv、Python 3.11、Rust(aarch64)。可重跑。

## 3. 建 venv + 装依赖(对齐 DuckDB 1.4.4)
```bash
cd ~/quant-stack/quant-research-v1
uv venv --python 3.11 .venv
uv pip install -p .venv -r requirements.txt   # 若无 requirements.txt,见 §3a
.venv/bin/python -c "import duckdb; assert duckdb.__version__=='1.4.4', duckdb.__version__; print('duckdb ok')"
```
§3a:若用 pyproject/手装,至少 `uv pip install -p .venv duckdb==1.4.4 pyyaml tushare pandas numpy scipy ...`(以 WSL `.venv` 的包清单为准:`pip freeze`)。

## 4. 编译 Rust(根 workspace + 排除的 quant-cn)
```bash
cd ~/quant-stack && . ~/.cargo/env
cargo build --release                                   # quant-stack 主二进制
cargo build --release --manifest-path quant-research-cn/Cargo.toml   # quant-cn(workspace 排除)
```
ARM 上首次编译较慢(~10-30min);31G 盘要盯 `target/` 体积。

## 5. 密钥手工 provision(你做 — 永不进 git)
从 WSL 安全拷贝(scp)到 Oracle,`chmod 600`:
- `quant-research-cn/config.yaml`、`quant-research-v1/config.yaml`(deepseek/tushare/finnhub/fred/anthropic keys)
- Gmail:`credentials.json` + `token.json`(US/CN 各自路径,见 docs/MIGRATION_AUTH_INVENTORY.md)
- codex:`codex login`(已在);narrator fallback DeepSeek 用 config 里的 key。
- 软链/路径按 inventory 重建。
> 清单与确切位置见 `docs/MIGRATION_AUTH_INVENTORY.md`。

## 6. 迁热数据(EXPORT/IMPORT — 唯一写者切换点之一)
对每个 canonical 热库(US:`quant-research-v1/data/quant.duckdb`;CN:`quant-research-cn/data/{quant_cn,quant_cn_research,quant_cn_report}.duckdb`)+ 不可再生 JSONL 账本:
1. **WSL** 无写窗口内:`scp` 库文件 → Oracle 对应路径,**或** `EXPORT DATABASE` 成 Parquet 再 Oracle `IMPORT`(跨版本更稳)。
2. 校验:Oracle 上 `count(*)` 关键表与 WSL 一致。
3. JSONL 账本(`report_decisions`/`report_outcomes` 等已在库内;`ai_infra/data/*.jsonl` 一并 scp)。
> **Oracle 自此为活库唯一写者;WSL 停止写(只读兜底)。不双写。**

## 7. 去 WSL 化
- 禁自动代理:`export QUANT_DISABLE_AUTO_PROXY=1`(或确认 `_default_gateway_ip()` 不触发);Oracle 直连公网,无需 7897。
- `ops/catch_up.py`:确认无 "WSL 睡眠" 硬假设阻塞(留崩溃兜底)。
- TZ 已 Asia/Shanghai(§2)。

## 8. Smoke(test 模式,不发邮件)
```bash
cd ~/quant-stack
ops/run_task.sh research.bubble_hedge_radar --dry-run
QUANT_DELIVERY_MODE=test QUANT_TEST_RECIPIENT=<你> \
  ./target/release/quant-stack daily --date <last-trading-day> --markets cn --session evening \
  --run-producers --with-narrative --send-reports --delivery-mode test --test-recipient <你> --stack-root .
```
确认:CN/US 报告生成、narrator codex 跑通(失败 fallback DeepSeek)、无 WSL 报错。

## 9. Cutover(crontab)
```bash
cd ~/quant-stack && python3 ops/render_cron.py --output ops/crontab.quant-stack
crontab ops/crontab.quant-stack    # Oracle 接管定时;WSL crontab 清空/停用
```

## 回滚
WSL 实例保持只读可跑,直到 Oracle 连续 N 天稳定。出问题:WSL 重新 `crontab` 接管 + Oracle 停 cron。数据以 Oracle 为准回拷。

## 验收
Oracle 上 `us.postmarket` + `cn.evening`(test)出报告;narrator + fallback 通;`smoke-check.sh` 过;连续 N 个交易日无漏跑/无 WSL 残留分支。
