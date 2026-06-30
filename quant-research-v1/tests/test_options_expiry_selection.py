from __future__ import annotations

from datetime import date, timedelta

from quant_bot.data_ingestion.options import _select_expiries_for_coverage


def test_select_expiries_preserves_long_dated_anchors() -> None:
    as_of = date(2026, 7, 1)
    expiries = [
        (as_of + timedelta(days=days)).isoformat()
        for days in [1, 3, 5, 7, 10, 14, 21, 28, 35, 42, 49, 56, 63, 90, 180, 221, 365, 540, 730]
    ]

    selected = _select_expiries_for_coverage(expiries, as_of, max_expiries=12)
    selected_dtes = [(date.fromisoformat(exp) - as_of).days for exp in selected]

    assert len(selected) == 12
    assert selected_dtes[:4] == [1, 3, 5, 7]
    assert 221 in selected_dtes
    assert 365 in selected_dtes
    assert 540 in selected_dtes
    assert 730 in selected_dtes
