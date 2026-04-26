# quant-stack-py

Thin PyO3 bindings for `quant-stack-core`.

Production daily jobs should keep using the `quant-stack` CLI. This package is
for notebooks, tests, and legacy Python code that need direct access to the Rust
core without reimplementing alpha maturity, champion/challenger selection,
execution gate, bulletin, or report-model logic.

Build locally:

```bash
cd crates/quant-stack-py
python -m pip install maturin
python -m maturin develop
```

Example:

```python
from quant_stack_py import evaluate_alpha

bulletin = evaluate_alpha(
    date="2026-04-24",
    markets=["us", "cn"],
    auto_select=True,
    emit_bulletin=True,
)
print(bulletin["execution_alpha"])
```

Contract:

- Python callers get the same JSON shape as the CLI.
- Headline context is advisory, not a hard execution blocker.
- A-share shadow options are risk diagnostics, not real single-name option
  trades.
- `Execution Alpha` means stable research candidate, not an order.
