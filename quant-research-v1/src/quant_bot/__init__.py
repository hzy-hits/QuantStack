"""Package init — central noise suppression for benign library chatter.

The daily pipeline log was accumulating hundreds of identical lines per
run from two libraries:

  * yfinance — every delisted ticker in the ~500-symbol universe prints a
    WARNING line ("possibly delisted; no timezone found") plus a batch
    "Failed download:" summary. None of these stop the pipeline.
  * statsmodels.tsa.stattools — emits a FutureWarning every call because
    of an internally-deprecated `verbose` kwarg.

Both are audited-benign and recurring. Suppressing them here surfaces the
real errors (which were drowning under ~400 noise lines per run).
"""
from __future__ import annotations

import logging
import warnings

# yfinance routes its delisted-ticker / failed-download warnings through
# the `yfinance` logger. Raising to ERROR keeps genuine failures visible
# but drops the per-ticker noise.
logging.getLogger("yfinance").setLevel(logging.ERROR)

# statsmodels deprecation warning on every tsa.stattools call.
warnings.filterwarnings(
    "ignore", category=FutureWarning, module=r"statsmodels(\..*)?",
)
