# Public / Private Boundary

状态：working data safety policy
用途：定义 `ai-super-cycle` 哪些内容可公开，哪些必须留在 private repo 或 gitignored 目录。

## 适合公开

```text
README.md
DISCLAIMER.md
docs/fund-management-philosophy.md
docs/llm-dependency-bfs-framework.md
docs/research-checklist.md
docs/source-evidence-template.md
docs/public-private-boundary.md
scripts/*.py
data/schemas/*.json
evidence/examples/
reports/public_methodology_snapshot.md
```

公开内容只能包含方法论、schema、脚本、模板、示例和已经可公开的原始来源链接。

## 必须私有

```text
ChatGPT Pro conversation URLs
browser profile / CDP port / cookies / tokens
personal filesystem paths
IBKR exports or account data
real or paper portfolio trades
position sizes
alpha score rankings
unverified company conclusions
paid data or broker notes
private notes
```

## 灰区

`evidence card` 可以公开，也可以私有，取决于内容。

可公开：

- 公司原文链接；
- 原文已证明的公开事实；
- 指标口径；
- 中性核验问题；
- 不含组合意图的模板。

应私有：

- watchlist 排名；
- 10x 弹性判断；
- 未核验推论；
- 交易计划；
- 仓位或组合意图；
- 付费数据摘要。

## 原则

1. 分支不是安全边界。
2. public/private 应通过不同 repo、gitignored 目录或 redacted export 分离。
3. private repo 也不能提交 API key、cookie、browser profile、broker token。
4. public snapshot 必须通过 private-data leak test。
