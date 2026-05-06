import math
import unittest

import numpy as np

from quant_bot.analytics.momentum_risk import _log_price_features


class MomentumLogFeatureTests(unittest.TestCase):
    def test_log_price_features_are_percentage_scale_and_causal(self):
        closes = np.array([100.0 * (1.01 ** i) for i in range(40)])

        features = _log_price_features(closes)

        self.assertTrue(
            math.isclose(features["log_return_1d_pct"], math.log(1.01) * 100.0, rel_tol=1e-9)
        )
        self.assertTrue(
            math.isclose(features["log_return_20d_pct"], math.log(1.01) * 20.0 * 100.0, rel_tol=1e-9)
        )
        self.assertIsNotNone(features["denoised_log_slope_10d_pct"])
        self.assertGreater(features["denoised_log_slope_10d_pct"], 0.0)
        self.assertIsNotNone(features["fft_low_freq_power"])
        self.assertIsNotNone(features["fft_high_freq_power"])

    def test_log_price_features_identify_noisy_tail_energy(self):
        choppy = np.array([100.0 * (1.02 if i % 2 == 0 else 0.98) for i in range(40)])

        features = _log_price_features(choppy)

        self.assertIsNotNone(features["haar_noise_energy"])
        self.assertGreater(features["haar_noise_energy"], 0.5)
        self.assertEqual(features["log_feature_window"], 32.0)
