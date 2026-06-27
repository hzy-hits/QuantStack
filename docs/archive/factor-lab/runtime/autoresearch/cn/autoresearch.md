# Factor Lab Autoresearch Session — A-Share (CN)

## Objective
Discover and validate new A-Share (CN) alpha factors with durable OOS edge.

## Optimization Target
- Promote factors that pass IS gates and OOS validation.
- Prefer factors that improve composite quality without exceeding correlation limits.
- Treat `eval_factor.py` as the canonical evaluator. Do not bypass its gates.

## Files In Scope
- [eval_factor.py](/home/ivena/coding/quant-stack/factor-lab/eval_factor.py)
- [src/agent/loop.py](/home/ivena/coding/quant-stack/factor-lab/src/agent/loop.py)
- [research_journal.md](/home/ivena/coding/quant-stack/factor-lab/research_journal.md)
- [experiments.jsonl](/home/ivena/coding/quant-stack/factor-lab/experiments.jsonl)

## Benchmark Harness
- Run [autoresearch.sh](./autoresearch.sh) with `FORMULA='rank(...)'`.
- The script emits `METRIC is_ic=...`, `METRIC is_ic_ir=...`, `METRIC gates_pass=...`.

## Checks Harness
- Run [autoresearch.checks.sh](./autoresearch.checks.sh) after promising results.
- Checks must stay green before anything is considered keep-worthy.

## Notes
- This file is the resumable session context for autoresearch mode.
- Update assumptions, promising factor families, and dead ends here.
