# GitHub Repo Operating Model

状态：repo setup plan
用途：把本地 AI Infra 研究系统变成可迁移、可版本化、可在新电脑恢复的 GitHub 仓库。

## 推荐仓库定位

推荐仓库名：

```text
ai-super-cycle
```

推荐先建 **private repo**。

原因：

- 当前包含 ChatGPT Pro 输出、个人研究路径、候选池和未核验判断；
- 部分内容不适合直接公开为投资观点；
- 后续可能接入价格数据、ETF holdings、13F、IBKR 导出或个人组合记录；
- private repo 仍可在新电脑 clone 使用。

如果未来要 public，应拆出一个干净版本：

```text
ai-super-cycle-public
```

只保留方法论、模板和脚本，不放个人 notes、候选结论、未核验公司卡和任何账户相关数据。

## 应该提交什么

建议提交：

| 目录 / 文件 | 是否提交 | 理由 |
| --- | --- | --- |
| `README.md` | 是 | 项目总入口 |
| `START_HERE.md` | 是 | 新电脑恢复后第一入口 |
| `docs/` | 是 | 稳定方法论和模板 |
| `scripts/` | 是 | 可复跑数据层和队列 |
| `data/seed/*.jsonl` | 是 | 公开安全样例 |
| `reports/*.md` | public repo 不提交；private repo 可以提交 | 当前研究仪表盘 |
| `reports/*.csv` | public repo 不提交；private repo 可以提交 | 可筛选队列，体积小 |
| `evidence/` | public repo 不提交；private repo 可以提交 | 原文证据卡，项目核心资产 |
| `data/global_universe_v2.jsonl` | public repo 不提交；private repo 可以提交 | 第一版 universe 输入，便于重建 |
| `data/ai_infra_universe.sqlite` | 可不提交 | 可由 JSONL 和脚本重建 |
| `notes/` | private repo 可以提交 | 保留 Pro 输出和研究脉络 |

建议不提交：

| 文件 | 原因 |
| --- | --- |
| `.DS_Store` | macOS 垃圾文件 |
| `__pycache__/` | Python 缓存 |
| `*.pyc` | Python 缓存 |
| `data/*.sqlite` | 生成物，可重建 |
| 任何 IBKR 导出 / 账户文件 | 个人敏感数据 |
| API key / cookie / browser profile | 绝不入库 |

## 建议 `.gitignore`

```gitignore
.DS_Store
__pycache__/
*.pyc
*.pyo
.venv/
venv/

# generated local database
data/*.sqlite
data/*.sqlite-*

# future private market/account data
private/
secrets/
ibkr/
*.key
*.pem
*.env
```

## 第一次建仓库

在项目根目录：

```bash
git init
git add README.md START_HERE.md docs scripts data/README.md data/seed .gitignore Makefile DISCLAIMER.md tests
git commit -m "Initial AI infra research system"
```

如果要提交完整 `notes/`、`reports/`、`evidence/` 和完整 universe，必须先确认远端是 private repo。

如果已安装并登录 `gh`：

```bash
gh repo create ai-super-cycle --private --source=. --remote=origin --push
```

如果不用 `gh`：

1. 在 GitHub 网页创建 private repo：`ai-super-cycle`。
2. 回到本地执行：

```bash
git remote add origin git@github.com:<your-user>/ai-super-cycle.git
git branch -M main
git push -u origin main
```

## 新电脑恢复

```bash
git clone git@github.com:<your-user>/ai-super-cycle.git
cd ai-super-cycle

cp /path/to/private/global_universe_v2.jsonl data/global_universe_v2.jsonl
python3 scripts/build_universe_system.py
python3 scripts/generate_source_verification_queue.py
python3 scripts/scaffold_evidence_cards.py
python3 scripts/generate_us_alpha_mining_queue.py
```

恢复后先读：

```text
START_HERE.md
docs/fund-management-philosophy.md
docs/llm-dependency-bfs-framework.md
docs/research-checklist.md
```

## 分支策略

```text
main
  稳定方法论、脚本、已整理 reports、已完成 evidence card

research/YYYY-MM-DD-topic
  新主题、新公司卡、新数据源接入

data/YYYY-MM-DD-source
  接入 ETF holdings、FRED、13F、价格数据等
```

提交规范：

```text
docs: add fund philosophy
data: rebuild universe dashboard
evidence: verify COHR Q3 FY2026 sources
reports: add US alpha mining queue
scripts: add ETF holdings fetcher
```

## 数据安全规则

1. 不提交 API key、cookie、Chrome profile、IBKR token。
2. 不提交真实账户持仓和交易记录，除非放在 private 并明确确认。
3. public 版本不放具体买卖判断、目标价、仓位建议。
4. 所有未核验内容保留 `pending_original_source_verification`。
5. GitHub repo 是研究系统，不是投资建议发布渠道。

## 推荐下一步

1. 先把当前目录初始化为本地 git。
2. 加 `.gitignore`。
3. 做第一笔 commit。
4. 创建 private GitHub repo。
5. 在另一台电脑 clone 并跑四条重建命令验证。
