from __future__ import annotations

import warnings
import unittest

import pandas as pd
from scipy.stats import ConstantInputWarning

from src.evaluate.quintile import compute_quintile_returns


class QuintileEvaluationTests(unittest.TestCase):
    def test_flat_bucket_returns_do_not_emit_constant_input_warning(self) -> None:
        dates = []
        factors = []
        returns = []
        for date in ["2026-05-06", "2026-05-07"]:
            dates.extend([date] * 50)
            factors.extend(range(50))
            returns.extend([0.01] * 50)

        with warnings.catch_warnings():
            warnings.simplefilter("error", ConstantInputWarning)
            result = compute_quintile_returns(
                pd.Series(factors),
                pd.Series(returns),
                pd.Series(dates),
            )

        self.assertEqual(result["monotonicity"], 0.0)
        self.assertEqual(result["n_days"], 2)


if __name__ == "__main__":
    unittest.main()
