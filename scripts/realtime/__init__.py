"""Real-time TOS RTD bridge → setup detector → LLM advisor → Telegram.

Pipeline:
  TOS desktop  -- RTD --> Excel cells (live refresh)
  Excel        -- VBA macro every 10s --> snapshot.csv
  csv_watcher.py  -- file watcher --> DuckDB realtime_quotes table
  setup_detector.py  -- state machine --> emits Setup events
  llm_advisor.py  -- on Setup event --> trade-idea JSON
  notify_telegram.py  -- on JSON --> push to user mobile
  user             -- TOS manual order placement

Each module is independently runnable for testing. See doc/REALTIME_SETUP.md
for the Excel-side configuration (RTD formulas + VBA macro template).
"""
