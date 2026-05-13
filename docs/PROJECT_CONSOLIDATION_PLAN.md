# Project Consolidation Plan

Date: 2026-05-09

## Goal

把当前量化工程收口成一个可审计、可运行、可交给 Claude Code review 的单一项目控制面。

这里的“单一项目”不是第一步就把所有文件物理搬进一个目录，而是先做到：

1. 所有定时任务从 `quant-stack` 根目录进入。
2. 所有 cron 任务由一个 task registry 生成，不再手写多套 cd/command。
3. 所有日志、状态、产物、review packet 有固定位置。
4. 子项目仍保留市场边界：US producer、CN producer、Factor Lab、shared alpha factory 各自负责自己的事实生产和研究模块。
5. `ALPHA_SLEEVE_ENGINEERING_PLAN.md` 继续执行，但在新增 sleeve 前先完成工程收口的关键前置项。

## Current Reality

根目录已经是 `/home/ivena/coding/quant-stack`，并且 README 已把它定义成 shared control plane。问题不是没有根项目，而是根项目没有完全接管运行面。

Status update 2026-05-09:

- `ops/tasks.yaml` 已落地，当前 18 个 cron schedule 都有 task id。
- `ops/run_task.sh` / `ops/run_task.py` 已落地，支持 dry-run、lock、统一日志、legacy log mirror、state JSON。
- `ops/render_cron.py` 已生成 `ops/crontab.quant-stack`。
- 当前用户 crontab 已切换为 root-only `ops/run_task.sh` 入口。
- `ops/crontab.legacy.snapshot` 保留回滚路径。
- `ops/review_packet.sh` 已可生成 Claude Code review packet。
- `ops/risk_path_map.yaml` 已落地，review packet 会按路径打风险标签。

当前结构：

```text
quant-stack/
  crates/                  # Rust shared control plane
  scripts/                 # shared Python review/backtest scripts
  quant-research-v1/       # US producer + agents + delivery
  quant-research-cn/       # CN producer + agents + delivery
  factor-lab/              # factor mining/research
  data/                    # shared alpha/report-model DB
  reports/                 # shared review outputs
  docs/
```

当前 cron 入口分散：

| Job group | Current cwd | Current entry |
| --- | --- | --- |
| US premarket/postmarket | `quant-research-v1` | `./scripts/run_full.sh` wrapper -> `quant-stack us-daily` |
| CN morning/evening | `quant-stack` | `./target/release/quant-stack daily --markets cn ...` |
| CN precompute | `quant-research-cn` | `scripts/precompute_alpha.sh` |
| Weekly US | `quant-research-v1` | `scripts/run_weekly.sh` |
| Weekly CN | `quant-research-cn` | `scripts/weekly_pipeline.sh` |
| Factor Lab daily | `factor-lab` | `scripts/daily_factors.sh --market cn/us` |
| Paper trading | `factor-lab` | `scripts/paper_trade.py record/evaluate/report` |
| Autoresearch | `factor-lab` | `scripts/autoresearch.sh` |
| Maintenance | `factor-lab` | `scripts/weekly_maintenance.py` |
| US watchdog | `quant-research-v1` | `uv run python scripts/cron_watchdog.py` |

当前痛点：

- cron 真实执行路径不在一个地方。
- 日志分散在 `quant-research-v1/logs`、`quant-research-cn/reports/logs`、`factor-lab/logs`。
- Claude Code review 需要同时理解 root、US、CN、Factor Lab 多套入口。
- 新 alpha sleeve 工作会继续碰 `scripts/`、`quant-research-v1/`、`quant-research-cn/`、`factor-lab/`，如果没有 review packet，很难看清改动边界。
- 物理搬目录风险高，因为大量脚本依赖相对路径和历史 DB/report 路径。

## Target Shape

第一目标：根目录变成唯一运行入口。

```text
quant-stack/
  ops/
    tasks.yaml                 # 所有生产/研究/维护任务注册表
    run_task.sh                # 按 task id 执行，统一 env/cwd/log/lock
    render_cron.py             # 从 tasks.yaml 生成 crontab
    crontab.quant-stack        # generated, 可安装版本
    crontab.legacy.snapshot    # 当前 crontab 快照
    logs/                      # 统一 task log，可保留子项目 log mirror
    state/                     # lock/status/last_success/last_failure
    review_packet.sh           # 生成 Claude Code 审核包
  apps/                        # 第二阶段物理收口目标；第一阶段先不搬
    us-producer/               # future move or symlink of quant-research-v1
    cn-producer/               # future move or symlink of quant-research-cn
    factor-lab/                # future move or symlink of factor-lab
  crates/
  scripts/
    sleeves/                   # alpha sleeve modules after split
    lib/
  data/
  reports/
  docs/
```

第一阶段不移动 `quant-research-v1`、`quant-research-cn`、`factor-lab`。先让 root `ops/run_task.sh` 包住它们。物理目录移动放到最后，并且必须保留兼容 symlink 或 wrapper 一段时间。

## Task Registry

新增 `ops/tasks.yaml`，每个任务必须有稳定 task id。

建议 schema：

```yaml
tasks:
  us.premarket:
    schedule: "0 20 * * 1-5"
    timezone: Asia/Shanghai
    cwd: quant-research-v1
    command: ["./scripts/run_full.sh", "--prod", "--premarket"]
    log: ops/logs/us.premarket.log
    lock: ops/state/us.premarket.lock
    sends_email: true
    market: us
    session: premarket
    owner: us_pipeline
    outputs:
      - quant-research-v1/reports
      - quant-research-v1/logs

  cn.evening:
    schedule: "0 18 * * 1-5"
    timezone: Asia/Shanghai
    cwd: .
    command:
      - ./target/release/quant-stack
      - daily
      - --date
      - "{cst_date}"
      - --markets
      - cn
      - --session
      - evening
      - --run-producers
      - --with-narrative
      - --send-reports
      - --delivery-mode
      - prod
      - --stack-root
      - "{stack_root}"
    log: ops/logs/cn.evening.log
    lock: ops/state/cn.evening.lock
    sends_email: true
    market: cn
    session: evening
```

字段要求：

- `task_id`: 稳定 id，例如 `us.postmarket`、`factor.cn.daily`。
- `schedule`: cron 表达式。
- `cwd`: 相对 `QUANT_STACK_ROOT` 的 cwd。
- `command`: argv list，不用 shell 字符串拼接。
- `log`: 统一 log 路径。
- `lock`: 防重入 lock。
- `timeout_minutes`: 长任务必须声明。
- `depends_on`: 可选，表达依赖关系，例如 Factor Lab US 等待 US data readiness。
- `sends_email`: true/false，review 时重点关注。
- `outputs`: 任务主要产物。

## Unified Cron

目标 crontab 只保留统一入口：

```cron
HOME=/home/ivena
QUANT_STACK_ROOT=/home/ivena/coding/quant-stack
PATH=/home/ivena/miniconda3/bin:/home/ivena/.local/bin:/home/ivena/.nvm/versions/node/v20.19.5/bin:/home/ivena/.cargo/bin:/usr/local/bin:/usr/bin:/bin

0 20 * * 1-5 cd $QUANT_STACK_ROOT && ops/run_task.sh us.premarket
0 5 * * 2-6 cd $QUANT_STACK_ROOT && ops/run_task.sh us.postmarket
20 7 * * 1-5 cd $QUANT_STACK_ROOT && ops/run_task.sh cn.precompute_alpha
30 8 * * 1-5 cd $QUANT_STACK_ROOT && ops/run_task.sh cn.morning
0 18 * * 1-5 cd $QUANT_STACK_ROOT && ops/run_task.sh cn.evening
...
```

不再在 crontab 里直接写 `cd $PROJ && ...`。真实命令只在 `tasks.yaml` 里维护。

迁移规则：

- 先 `crontab -l > ops/crontab.legacy.snapshot`。
- 根据 legacy crontab 写 `ops/tasks.yaml`，保持一比一行为。
- `ops/render_cron.py` 生成 `ops/crontab.quant-stack`。
- dry-run 对比：确认 schedule、cwd、command、log 都等价。
- 用户确认后才安装新 crontab。
- 保留 legacy snapshot 和 rollback 命令。

## Runtime Wrapper

`ops/run_task.sh TASK_ID` 第一版可以是 shell，后面可以升级成 `quant-stack task run TASK_ID`。

职责：

- 读取 `ops/tasks.yaml`。
- 设置 `QUANT_STACK_ROOT`。
- 进入指定 cwd。
- 创建 lock，避免重入。
- 统一 stdout/stderr 到 `ops/logs/<task_id>.log`。
- 记录 `ops/state/<task_id>.last_success.json` 或 `.last_failure.json`。
- 打印开始/结束时间、git commit、dirty status summary、exit code。
- 支持 `--dry-run` 只展示将执行的命令。
- 支持 `--date YYYY-MM-DD` 覆盖 `{cst_date}` / `{ny_date}`。

不建议第一版把所有逻辑写进 Rust CLI。先用薄 shell/Python registry 验证任务模型；稳定后再把 task runner 收进 `quant-stack` Rust CLI。

## Review Packet For Claude Code

新增 `ops/review_packet.sh`，让任何一次改动能被外部 reviewer 快速看懂。

输出建议：`reports/review_packets/YYYY-MM-DD-HHMMSS/`

内容：

- `git_status.txt`
- `git_diff_stat.txt`
- `git_diff.patch`
- `changed_files_by_project.md`
- `commands_run.md`
- `tests_run.md`
- `reports_generated.md`
- `cron_tasks_affected.md`
- `risk_summary.md`

`changed_files_by_project.md` 分组：

- root control plane
- shared alpha scripts
- US producer
- CN producer
- Factor Lab
- docs only
- generated/runtime files

这样 Claude Code review 时不需要猜“这次到底动了哪个系统”。

## Relationship To Alpha Sleeve Plan

`ALPHA_SLEEVE_ENGINEERING_PLAN.md` 继续执行，但新增前置依赖：

1. 先完成 task registry 和 review packet。
2. 再做 `scripts/sleeves/` 零行为拆分。
3. 再做 hedge ledger。
4. 再做 CN tape / US theme sleeve。

Ownership:

- Project Consolidation owns ops, cron migration, review packet, risk path map, and zero-behavior split of existing alpha sleeve modules.
- Alpha Sleeve Engineering owns historical hedge ledger, `scripts/lib/hedge.py` extraction when the ledger becomes the second caller, new sleeves, calibration, promotion gates, and `promoted_sleeves`.

原因：

- 新 sleeve 会横跨 root scripts、CN Rust、US Python、Factor Lab data。
- 没有统一 review packet，每次改动都很难审。
- 没有统一 task registry，改完也不知道生产 cron 到底跑了哪条路径。

## Phases

### Phase 0: Inventory and lock current behavior

Deliverables:

- `ops/crontab.legacy.snapshot`
- `ops/task_inventory.md`
- 当前所有 task id、schedule、cwd、command、log、产物表。
- 标记哪些任务会发邮件，哪些只做研究/维护。

Acceptance:

- 能从一个文件回答“现在每天几点跑什么、在哪跑、产物在哪、日志在哪”。
- 不改 crontab，不改任务行为。

### Phase 1: Task registry and root runner

Deliverables:

- `ops/tasks.yaml`
- `ops/run_task.sh`
- `ops/render_cron.py`
- `ops/crontab.quant-stack`

Acceptance:

- `ops/run_task.sh --dry-run <task_id>` 能显示完整命令。
- 每个 legacy cron job 在 `tasks.yaml` 有一条等价 task。
- 生成的 crontab 与 legacy schedule 一致。

### Phase 2: Non-invasive cron migration

Deliverables:

- 新 crontab 安装说明。
- Rollback command。
- 第一周并行观察表。

Acceptance:

- 只改 cron 入口，不改实际 producer/report 命令。
- 所有任务日志写入 `ops/logs/`，同时可以保留旧日志路径一段时间。
- 任一任务失败时能从 `ops/state` 找到 exit code 和最后一次成功时间。
- Watchdog verification required before declaring stable: simulate a missed US report/log condition and confirm `us.watchdog` still catches it under the root-runner log layout.

### Phase 3: Review packet workflow

Deliverables:

- `ops/review_packet.sh`
- `ops/risk_path_map.yaml`
- `reports/review_packets/...`
- PR/review handoff 模板。

Acceptance:

- 每次让 Claude Code 审核，只需要给 review packet 路径和目标问题。
- Packet 自动列出改动是否触碰 cron、生产邮件、ranker、sleeve、prompt。
- Risk dimensions are explicit in `ops/risk_path_map.yaml`, not inferred only from ad hoc path grouping.

### Phase 4: Shared script/module cleanup

Deliverables:

- `scripts/sleeves/`
- `scripts/run_alpha_sleeve_backtest.py` 只保留 orchestration/render/write。

Acceptance:

- alpha sleeve split 是零行为变更。
- backtest 输出前后一致。
- Existing sleeve modules are split without introducing new trading behavior.

### Phase 5: Optional physical directory consolidation

只有 Phase 1-4 稳定后才考虑物理搬目录：

```text
quant-research-v1  -> apps/us-producer
quant-research-cn  -> apps/cn-producer
factor-lab         -> apps/factor-lab
```

迁移规则：

- 先建立 symlink 或 wrapper，旧路径继续可用。
- 所有脚本改用 `QUANT_STACK_ROOT` 和 task registry，不依赖硬编码相对路径。
- 迁移 DB/report/log 前先做路径映射表。
- 生产 cron 已经只调用 `ops/run_task.sh`，所以物理移动不会影响 cron。

不建议现在直接做 Phase 5。先把运行入口、日志、审查和 alpha 模块化做好，物理路径自然会变简单。

## Proposed Task IDs

第一版 task ids：

| Task id | Purpose |
| --- | --- |
| `us.premarket` | US premarket report |
| `us.postmarket` | US postmarket report |
| `us.watchdog` | missed US report watchdog |
| `cn.precompute_alpha` | CN review backfill + stable alpha precompute |
| `cn.morning` | CN morning report |
| `cn.evening` | CN evening report |
| `weekly.us` | US weekly report |
| `weekly.cn` | CN weekly report |
| `factor.cn.daily` | CN factor mining/export/research |
| `factor.us.daily` | US factor mining/export/research |
| `paper.record` | Factor Lab paper trade record |
| `paper.evaluate` | Factor Lab paper trade evaluate |
| `paper.report` | Factor Lab paper trade report |
| `autoresearch.cn.morning` | CN autoresearch |
| `autoresearch.all.midday` | all-market autoresearch midday |
| `autoresearch.all.afternoon` | all-market autoresearch afternoon |
| `factor.maintenance.weekly` | weekly Factor Lab maintenance |

## Immediate Implementation Order

1. Add `ops/` skeleton and snapshot current crontab.
2. Write `ops/task_inventory.md` from the current crontab.
3. Write `ops/tasks.yaml` that exactly mirrors current behavior.
4. Write `ops/run_task.sh --dry-run` and validate all task ids.
5. Write `ops/render_cron.py` and generate `ops/crontab.quant-stack`.
6. Add `ops/review_packet.sh`.
7. Only after dry-run review, ask before installing the generated crontab.
8. Resume `ALPHA_SLEEVE_ENGINEERING_PLAN.md` from module split and hedge ledger.

## Non-goals

- 不把 US/CN/Factor Lab 强行改成同一个语言或同一个 package。
- 不第一步移动 DB、reports、logs 的物理位置。
- 不在没有 rollback 的情况下替换 crontab。
- 不让 prompt 继续承担运行约束；运行约束放到 task registry、promoted_sleeves、tests 和 code assertions。

## Success Criteria

完成后应该能回答：

- 今天生产会跑哪些任务？
- 每个任务由哪条命令跑？
- 哪些任务会发邮件？
- 失败日志在哪里？
- 上次成功是什么时间？
- 这次代码改动影响哪些 task？
- Claude Code 要审核什么文件、什么风险、什么测试？
- 新 alpha sleeve 是否在统一 task/report/review 流程里被执行和验证？
