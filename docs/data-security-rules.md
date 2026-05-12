# Data Security Rules

状态：working policy
用途：防止研究仓库泄露个人路径、浏览器状态、券商数据、token 或未公开判断。

## 永不提交

```text
API keys
OAuth tokens
cookies
browser profiles
Chrome CDP endpoints
IBKR exports
account statements
real holdings
real trade logs
private broker notes
paid research PDFs
```

## 默认 private

```text
ChatGPT Pro URLs
local absolute filesystem paths
alpha rankings
paper portfolio ledger
score history
unverified company conclusions
raw downloads
notebooks/exploratory
```

## 可提交但需标注

```text
ChatGPT Pro summarized outputs -> pending original-source verification
evidence cards -> source status required
CSV queues -> research priority, not investment ranking
SQLite-derived reports -> generated, not source of truth
```

## Private Data Leak Test Keywords

```text
/Users/
/home/
chatgpt.com/g/
/c/
CDP port
api_key
secret
token
cookie
position
portfolio_value
broker
IBKR
```
