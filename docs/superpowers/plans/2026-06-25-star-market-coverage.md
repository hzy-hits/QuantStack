# STAR Market (科创板) Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let STAR Market (688) names enter the CN probability pipeline + report via a config-gated, opt-in `scan.star` flag — relaxing the single 688 exclusion and adding a 科创50 scan — so the AI-infra basket's STAR holdings get full CN analytics.

**Architecture:** One cohesive Rust change in the standalone `quant-cn` crate: add a `scan.star: bool` config field (serde-default false), thread it into `is_tradable_a_share(code, allow_star)` at both call sites, and add the 科创50 index (`000688.SH`) to the scan array. Microstructure (±20% limits) is already handled by `price_limit_pct` — untouched. A separate operator runbook documents enable/backfill/watchlist-fallback.

**Tech Stack:** Rust 2021, `quant-cn` crate (`quant-research-cn/`, EXCLUDED from the root workspace — build/test via `--manifest-path quant-research-cn/Cargo.toml`), serde, DuckDB, Tushare `index_weight`.

## Global Constraints

- `scan.star` field MUST be `#[serde(default)]` (defaults `false`) — existing `config.yaml` files have no `star` key and must still parse with zero behavior change.
- Gate semantics, exact: `fn is_tradable_a_share(code: &str, allow_star: bool) -> bool { allow_star || !code.starts_with("688") }`. With `star=false` behavior is identical to today (688 excluded).
- 科创50 index code is exactly `"000688.SH"`.
- Do NOT touch any analytics module, `price_limit_pct` (already returns 20.0 for 688, with test `test_price_limit_rules`), flow, regime, or report rendering.
- Build/test the CN crate with `--manifest-path quant-research-cn/Cargo.toml` (it is excluded from the root workspace; package name `quant-cn`). Do NOT `cd` (avoids permission prompts).
- Backfill (`scripts/backfill_cn_prices.py`) is an operator step documented in Task 2, not code.

---

## Task 1: Config flag + gated 688 filter + 科创50 scan (+ Rust unit test)

**Files:**
- Modify: `quant-research-cn/src/config.rs` (`ScanConfig` struct, ~line 64-69)
- Modify: `quant-research-cn/src/fetcher/tushare/universe.rs` (`is_tradable_a_share` ~line 9; call sites ~line 42 and ~line 56; `indices` array ~line 21-24)
- Modify: `quant-research-cn/config.example.yaml` (`universe.scan` block)
- Test: `quant-research-cn/src/fetcher/tushare/universe.rs` (`#[cfg(test)] mod tests` at end of file)

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `is_tradable_a_share(code: &str, allow_star: bool) -> bool`; `ScanConfig.star: bool` (serde-default false), read as `cfg.universe.scan.star`.

- [ ] **Step 1: Write the failing Rust unit test** — append to the END of `quant-research-cn/src/fetcher/tushare/universe.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::is_tradable_a_share;

    #[test]
    fn star_gated_by_flag() {
        // 688 excluded when star off, allowed when star on
        assert!(!is_tradable_a_share("688981.SH", false));
        assert!(is_tradable_a_share("688981.SH", true));
        // main board / ChiNext always tradable, regardless of flag
        assert!(is_tradable_a_share("600519.SH", false));
        assert!(is_tradable_a_share("600519.SH", true));
        assert!(is_tradable_a_share("300750.SZ", false));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile/pass**

Run: `cargo test --manifest-path quant-research-cn/Cargo.toml star_gated_by_flag`
Expected: FAIL — compile error, `is_tradable_a_share` takes 1 argument not 2 (signature not yet changed).

- [ ] **Step 3: Add the `star` config field** — in `quant-research-cn/src/config.rs`, change the `ScanConfig` struct:

```rust
#[derive(Deserialize, Clone)]
pub struct ScanConfig {
    pub csi300: bool,
    pub csi500: bool,
    pub csi1000: bool,
    pub sse50: bool,
    #[serde(default)]
    pub star: bool,
}
```

- [ ] **Step 4: Gate the filter + add the 科创50 scan** — in `quant-research-cn/src/fetcher/tushare/universe.rs`:

Change the function:
```rust
fn is_tradable_a_share(code: &str, allow_star: bool) -> bool {
    allow_star || !code.starts_with("688")
}
```

Add 科创50 to the `indices` array (after the `sse50` line):
```rust
    let indices = [
        (cfg.universe.scan.csi300, "399300.SZ"),
        (cfg.universe.scan.csi500, "000905.SH"),
        (cfg.universe.scan.csi1000, "000852.SH"),
        (cfg.universe.scan.sse50, "000016.SH"),
        (cfg.universe.scan.star, "000688.SH"),
    ];
```

Update the index-scan call site (was `if is_tradable_a_share(code) {`):
```rust
                if is_tradable_a_share(code, cfg.universe.scan.star) {
```

Update the watchlist call site (was `if is_tradable_a_share(sym) && !symbols.contains(sym) {`):
```rust
        if is_tradable_a_share(sym, cfg.universe.scan.star) && !symbols.contains(sym) {
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test --manifest-path quant-research-cn/Cargo.toml star_gated_by_flag`
Expected: PASS (1 passed). If other pre-existing tests compile-break, they are unrelated to this change — but they should not, since only a private fn signature + a struct field changed.

- [ ] **Step 6: Confirm the crate still builds**

Run: `cargo build --release --manifest-path quant-research-cn/Cargo.toml 2>&1 | tail -5`
Expected: `Finished release` with no errors.

- [ ] **Step 7: Add the example-config line** — in `quant-research-cn/config.example.yaml`, under `universe.scan`, add after the `sse50` line:

```yaml
    star: false                 # 科创板(688),±20% 涨跌幅;opt-in,默认关
```

- [ ] **Step 8: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add quant-research-cn/src/config.rs quant-research-cn/src/fetcher/tushare/universe.rs quant-research-cn/config.example.yaml
git commit -m "feat(cn): opt-in 科创板 (STAR/688) universe coverage via scan.star + 科创50 scan"
```

---

## Task 2: STAR operator runbook

**Files:**
- Create: `quant-research-cn/docs/STAR_MARKET.md`

**Interfaces:**
- Consumes: `scan.star` flag + the gated filter from Task 1.
- Produces: operator documentation (no code).

- [ ] **Step 1: Write `quant-research-cn/docs/STAR_MARKET.md`:**

```markdown
# 科创板 (STAR Market) Coverage

科创板(688)默认**不在** CN scan universe(历史上被 `is_tradable_a_share` 排除)。
本功能用 `universe.scan.star` 开关 opt-in 纳入。

## 启用
1. `config.yaml` 的 `universe.scan` 设 `star: true`。
2. **必须先补价格历史**(新标的默认只有 ~45 行,`n<60` 会被分析静默丢弃):
   `python3 scripts/backfill_cn_prices.py`
3. 跑一次 CN 流水线(test 模式)确认无 panic、688 名字进报告:
   `./target/release/quant-cn run`

启用后扫描 **科创50(000688.SH)** 成分;微观结构(±20% 涨跌幅)由
`src/analytics/rv.rs::price_limit_pct` 已正确处理(688 → 20.0),无需额外配置。

## AI-infra 篮子兜底
`ai_infra/data/global_universe_v2.jsonl` 含 24 个 688 名字。**不在科创50 成分里的**,
加入 `config.yaml` 的 `universe.watchlist`——`star: true` 后 watchlist 的 688 也会放行,
保证篮子持仓全覆盖,与科创50 成员无关。

## 验证
- universe 含 688 名字;`quant-cn run` 无 panic。
- CN 报告出现科创板标的,limit/vol 信号按 ±20% 计。
- 此前无 CN 分析的 ai_infra STAR 名字现在有 momentum/flow/regime 输出。

## 回滚
`universe.scan.star: false`(默认)→ 688 重新排除,回到现状。纯 config,无需重编。

## 后续(未实现)
科创100(`000698.SH`)留作第二个开关 `scan.kc100`,需要中盘广度时再加。
```

- [ ] **Step 2: Commit**

```bash
cd /home/ivena/coding/quant-stack
git add quant-research-cn/docs/STAR_MARKET.md
git commit -m "docs(cn): STAR market coverage runbook (enable/backfill/watchlist/rollback)"
```

---

## Self-Review

- **Spec coverage:** 组件1 配置开关 → Task 1 Steps 3,7. 组件2 放开闸门 → Task 1 Step 4. 组件3 科创50 扫描 → Task 1 Step 4 (indices). 组件4 篮子兜底 → Task 2 (watchlist doc). 组件5 补价格 → Task 2 (backfill doc). 测试(is_tradable_a_share 门控)→ Task 1 Step 1. "不动 price_limit_pct" → honored (untouched).
- **Placeholder scan:** none — every step has exact code/commands.
- **Type consistency:** `is_tradable_a_share(code: &str, allow_star: bool) -> bool` and `cfg.universe.scan.star` used identically in the function, both call sites, the indices array, and the config field. `000688.SH` consistent.
- **Backward-compat:** `#[serde(default)] pub star: bool` ensures existing `config.yaml` (no `star` key) parses with `star=false` → zero behavior change until opt-in.
