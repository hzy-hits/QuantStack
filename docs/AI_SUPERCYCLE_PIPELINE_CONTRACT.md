# AI Supercycle Pipeline Contract

This contract makes AI infrastructure the first research priority across US,
CN and Factor Lab without turning unverified supplier stories into facts.

## Investment Mandate

The investment philosophy lives in
`docs/AI_INFRA_INVESTMENT_MANDATE.md`. The short version: the pipeline should
not hunt the entire market every day. It should start from the AI-infra
supercycle universe, then use price/volume/flow, theme strength, evidence
quality and risk attribution to decide what is tradeable.

The upstream research workbench lives in `ai_infra/`. It is the target state for
this migration: a source-review-gated AI-infra universe, not broad all-market
screening. Production screens should treat `ai_infra/data/global_universe_v2.jsonl`,
`ai_infra/reports/source_verification_queue_v1.csv`, and
`ai_infra/reports/us_alpha_mining_queue_v1.csv` as the upstream research queue
to map into market-specific rankers.

The detailed mainline research map lives in
`docs/AI_INFRA_RESEARCH_MAINLINES.md`; it is the canonical human-readable source
for the 16-layer AI infra map, the 14 continuous research themes, the priority
ladder for 10x candidate hunting, and the AI-infra bear-case dashboard.
The first source-review-gated deep dive lives in
`docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md`; it defines the HBM structural-standard
thesis and the CoWoS / advanced-packaging bottleneck map used by the first two
mainline research themes.

The quant implementation bridge lives in
`docs/AI_INFRA_QUANT_FUND_INTEGRATION.md`. It defines how the `ai_infra/`
universe becomes ranker input, what Factor Lab must mine, what non-price data
is required, and how a sleeve must be evaluated as long alpha minus beta hedge
before promotion.

The AI-lab publication index is auxiliary. It can improve model/cloud company
research, but it is not the core strategy and must not block the AI-infra
pipeline.

## Scope

- US: theme-basket sleeve reads `data/us_theme_seed_map.yaml`.
- CN: tape sleeve and ranker map industries into granular AI-supercycle layers
  such as optical/CPO, chip equipment/materials/packaging, datacenter hardware,
  power/nuclear/grid, industrial capex, hard assets and excluded consumer.
- Factor Lab: agents are instructed to mine AI-infra supply-chain factors first,
  while putting unverified supplier/customer relationships into
  `DATA_REQUIREMENTS` and the AI supply-chain discovery fields.

## Evidence Rule

Theme membership is only a screening prior. A final report can mention a
supplier/customer relationship only when there is local cached evidence or a
fresh primary/news source. If evidence is missing, the output must say that the
relationship is a research requirement, not an established reason to trade.

## Priority Ladder

1. AI labs, cloud, model distribution, accelerators, memory, optical/CPO and
   datacenter networking.
2. Semiconductor equipment/materials/packaging/test, datacenter physical
   infrastructure, power/grid/nuclear and industrial capex.
3. Space connectivity/future orbital infrastructure and hard-asset input
   scarcity.
9. Daily consumption is excluded from the current mandate unless a separate
   sleeve proves it.

## Current Artifacts

- `ai_infra/`: upstream AI-infra research workbench from
  `git@github.com:hzy-hits/QuantStack.git`. It contains the BFS framework,
  research checklist, source-evidence templates, 146-row global universe,
  source verification queue, US alpha mining queue, core candidate queue and
  evidence-card drafts. It is a research queue and evidence OS, not a direct
  trade-output folder.
- `data/ai_supercycle_taxonomy.yaml`: cross-market layer contract.
- `data/ai_infra_research_themes.yaml`: machine-readable version of the
  14 mainline research themes from `docs/AI_INFRA_RESEARCH_MAINLINES.md`.
- `docs/AI_INFRA_HBM_COWOS_DEEP_DIVE.md`: detailed source-review-gated memo for
  `hbm_structural_supercycle` and `cowos_advanced_packaging_bottleneck`. It can
  guide research and report framing, but cannot create source-confirmed
  supplier evidence until the listed primary sources are reviewed.
- `docs/AI_INFRA_QUANT_FUND_INTEGRATION.md`: engineering contract that binds
  the `ai_infra/` research OS to rankers, Factor Lab, options/credit/financial
  data requirements and beta-hedged portfolio attribution.
- `data/us_theme_seed_map.yaml`: US theme universe and metadata.
- `scripts/sleeves/us_theme_cluster.py`: propagates theme metadata into
  current candidates.
- `scripts/sleeves/cn_tape_leadership.py`: treats A-share news as a lagging
  label and emits granular `supercycle_layer` fields for price/flow-first tape
  leadership, including optical/CPO, chip equipment/materials/packaging,
  datacenter hardware, power/nuclear/grid, industrial capex and hard assets.
- `quant-research-v1/src/quant_bot/analytics/ai_infra_universe.py`: bridge that
  loads `ai_infra/data/global_universe_v2.jsonl`, normalizes US/CN symbols,
  filters non-AI names and expands ranker candidates to all mandate names.
- `quant-research-v1/src/quant_bot/analytics/us_opportunity_ranker.py`: adds
  AI-supercycle priority to ranking and public output, and in production mode
  ranks the full local AI-infra US universe instead of only the old current
  candidate list.
- `quant-research-v1/src/quant_bot/analytics/cn_opportunity_ranker.py`: maps CN
  industries to AI-supercycle layers, adds priority to ranking, and in
  production mode filters/expands through the local AI-infra A-share universe.
- `factor-lab/src/agent/prompts.py`: tells autoresearch to search for AI-infra
  factors from `ai_infra/`, fill `AI_SUPERCYCLE_LAYER` /
  `SUPPLY_CHAIN_HYPOTHESIS`, and demand financials, CDS/credit, filings/news,
  options IV/VRP/flow, option-leg PnL and beta-hedge data when a price-only DSL
  factor cannot verify the thesis.
- `factor-lab/src/autoresearch/ai_infra_context.py`: loads the full
  `ai_infra/data/global_universe_v2.jsonl`, normalizes US/CN and ADR aliases,
  prepends a compact BFS/universe context to every autoresearch session, and
  filters Factor Lab price/forward-return data to the AI-infra universe when
  `FACTOR_LAB_AI_INFRA_ONLY=1`.
- `factor-lab/scripts/autoresearch.sh`: defaults
  `FACTOR_LAB_AI_INFRA_ONLY=1` and `FACTOR_LAB_AI_INFRA_ROOT=../ai_infra`, so
  cron-driven Factor Lab sessions mine the AI-infra book first instead of
  restarting broad all-market scans.
- `factor-lab/scripts/daily_factors.sh`, `factor-lab/src/market_data.py`,
  `factor-lab/src/mining/daily_pipeline.py`,
  `factor-lab/src/mining/export_to_pipeline.py`, and
  `factor-lab/src/mining/export_sleeve_returns.py`: keep scheduled factor
  mining, health checks, diagnostics, pipeline export and sleeve-return ledgers
  inside the AI-infra universe when `FACTOR_LAB_AI_INFRA_ONLY=1`.
- `factor-lab/src/autoresearch/ai_supply_chain.py` and
  `factor-lab/scripts/export_ai_supply_chain_discovery.py`: exports
  autoresearch AI-infra hypotheses into a source-required discovery queue.
- `factor-lab/reports/autoresearch_exports/ai_supply_chain/*`: generated
  discovery queue. It is not a confirmed relationship ledger.
- `data/ai_supply_chain_relationships.yaml`: source-linked relationship ledger
  for official/news-backed supply-chain evidence.
- `data/ai_supply_chain_relationships_raw.example.csv` and
  `scripts/build_ai_supply_chain_relationships.py`: offline ingestion path
  from source-confirmed filings/news/transcript relationship rows into the YAML
  ledger. Rows without `source_url`, `source_type`, and high/medium confidence
  are rejected. If a candidate CSV carries `review_state`, only
  `source_confirmed` rows are accepted; rows still marked
  `news_review_candidate` are rejected even if confidence was edited.
- `scripts/extract_ai_supply_chain_relationship_candidates.py`: local
  US/CN news-table and US SEC-metadata scanner that writes unreviewed
  relationship candidates to
  `reports/review_dashboard/ai_supply_chain_candidates/<date>/`. News rows
  require AI-infra terms plus relationship terms, filter generic
  liquidity/money supply noise, and mark every row
  `needs_human_source_review` with `confidence=unreviewed`. SEC rows are
  material-agreement review prompts only; the filing still has to be opened and
  source-confirmed before promotion. The extractor also writes a Markdown
  review brief so the highest-priority relationships can be checked without
  reading raw JSON.
- `scripts/promote_ai_supply_chain_relationship_candidates.py`: promotion
  helper for reviewed candidate CSVs. It keeps only rows marked
  `review_state=source_confirmed`, reuses the strict relationship-ingest
  validation, rejects rows still marked `*_review_candidate`, and writes a
  builder-ready source-confirmed CSV for
  `scripts/build_ai_supply_chain_relationships.py`.
- `ops/tasks.yaml` task `research.ai_supply_chain_candidates`: runs the local
  candidate extractor on a daily schedule. This creates a review queue only; it
  does not update `data/ai_supply_chain_relationships.yaml`.
- `data/ai_lab_quality_seed.yaml`: auxiliary seed contract for a NeurIPS /
  ICML / ICLR / CVPR industrial-lab quality index. This is a research enhancer,
  not a promotion gate for the AI-infra mandate.
- `data/ai_lab_publications.example.csv`: schema for loading accepted-paper
  counts into the lab index. A real `data/ai_lab_publications.csv` is runtime
  data and remains ignored by git.
- `data/ai_lab_publications_raw.example.csv` and
  `scripts/build_ai_lab_publications.py`: offline ingestion path from a raw
  accepted-paper affiliation CSV/JSON/JSONL export into the normalized
  `data/ai_lab_publications.csv` file consumed by the lab index. The parser
  handles flat CSV, OpenAlex-style JSON/JSONL, Semantic Scholar-style JSON, and
  OpenReview note JSON when affiliations are present.
- `scripts/fetch_ai_lab_publications_openalex.py`: optional OpenAlex fetcher
  for top-conference industrial-lab works. It resolves conference sources,
  queries works by publication year and raw affiliation strings, and writes
  raw JSONL for `scripts/build_ai_lab_publications.py`. This is a data-fetch
  helper only; the normalized `data/ai_lab_publications.csv` is still runtime
  data and should be inspected before the lab score is trusted. Use
  `--symbols` and `--conferences` for targeted fetches before attempting a
  broad pull. `--fallback-search` is opt-in because broad works search can
  return non-conference publications; the default path favors precision.
- `ai_supercycle_evidence.*` and `ai_lab_quality_index.*` in each
  `main_strategy_v2/<date>/` output folder: auditable evidence ledgers.
- Source-linked relationship rows can enter `ai_supercycle_evidence.*` as
  `relationship_research_seed` even when they are not current ranker trades;
  these rows are research-only and cannot create production R.
- `ai_supercycle_value_radar.*` in each `main_strategy_v2/<date>/` output
  folder: long-horizon 10x research radar. It ranks research priority only and
  cannot create same-day production R. When publication data exists, the radar
  carries `lab_quality_score` as a scoring component for AI-lab/cloud/model
  distribution companies.
- `ai_supercycle_layer_attribution.*` in each `main_strategy_v2/<date>/`
  output folder: historical sleeve evidence by AI-supercycle layer, so the
  report can distinguish layers with real LCB80 support from story-only layers.
- `scripts/verify_ai_supercycle_readiness.py` and ops task
  `research.ai_supercycle_readiness`: daily readiness verifier that maps the
  mandate to concrete artifacts. It treats missing source-confirmed
  relationship evidence as a hard blocker, and treats missing top-conference
  publication data as an auxiliary warning.

## Not Done Yet

- A live filings crawler and source-review agent that opens candidate links and
  fills the reviewed CSV. Local candidate extraction and a strict promotion
  helper now exist, but the actual source review remains human/agent work.
- A real publication-affiliation dataset for the optional top-conference AI-lab
  quality index. The OpenAlex fetcher and ingestion job exist, but the actual
  dataset is runtime data and is not committed. Missing publication data should
  show as a warning, not a blocker.
