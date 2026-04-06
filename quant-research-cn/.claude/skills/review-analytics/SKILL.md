---
name: review-analytics
description: Review analytics modules for correctness — verify axiom compliance, Beta-Binomial usage, detail JSON completeness, and DuckDB write patterns.
---

# Review Analytics Module

Review one or more analytics modules in `src/analytics/` for:

## Checklist

1. **Axiom compliance** — Does the module trace to exactly one axiom from `spec.md` §3?
2. **Beta-Binomial** — If applicable, does it use `bayes.rs` (not hand-rolled)?
3. **Detail JSON** — Does every `analytics` INSERT include detail with:
   - `horizon` (e.g., "5D")
   - `conditioning_set` (e.g., "trending, low_vol")
   - `sample_size` (integer)
   - `ci_lower` and `ci_upper` (95% credible interval)
4. **Config usage** — Are signal parameters from `cfg.signals.*`, not hardcoded?
5. **DuckDB patterns**:
   - `INSERT OR REPLACE INTO analytics` for idempotency
   - Date params as `YYYY-MM-DD` strings
   - No `Connection` across `.await` boundaries
6. **Edge cases**:
   - Empty data (no prices for date) → returns `Ok(0)`, not error
   - Division by zero in z-scores → `zscore_clamped` handles std=0
   - NaN/Inf propagation → clamped or skipped

## How to Run

```bash
# Review a specific module
claude "Review src/analytics/momentum.rs using the review-analytics skill"

# Review all modules with codex-par
# (use the run-with-codex-par skill for parallel review)
```

## Output Format

For each module, report:
- PASS / FAIL per checklist item
- Specific line numbers for issues
- Suggested fixes (code snippets if needed)
