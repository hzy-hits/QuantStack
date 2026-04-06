from __future__ import annotations

import random
import requests
import time

from quant_bot.data_ingestion import options


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}

    def json(self) -> dict:
        return self._payload


def test_fetch_cboe_single_honors_retry_after_header(monkeypatch):
    responses = [
        _FakeResponse(429, headers={"Retry-After": "7"}),
        _FakeResponse(200, payload={"data": {"symbol": "SPY"}}),
    ]
    sleeps: list[float] = []

    monkeypatch.setattr(options, "_respect_cboe_rate_limit", lambda: None)
    monkeypatch.setattr(requests, "get", lambda url, timeout: responses.pop(0))
    monkeypatch.setattr(random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(time, "sleep", lambda seconds: sleeps.append(seconds))

    raw = options._fetch_cboe_single("SPY")

    assert raw == {"data": {"symbol": "SPY"}}
    assert sleeps == [7.0]


def test_fetch_cboe_single_uses_short_exponential_backoff(monkeypatch):
    responses = [
        _FakeResponse(429),
        _FakeResponse(429),
        _FakeResponse(200, payload={"data": {"symbol": "QQQ"}}),
    ]
    sleeps: list[float] = []

    monkeypatch.setattr(options, "_respect_cboe_rate_limit", lambda: None)
    monkeypatch.setattr(requests, "get", lambda url, timeout: responses.pop(0))
    monkeypatch.setattr(random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(time, "sleep", lambda seconds: sleeps.append(seconds))

    raw = options._fetch_cboe_single("QQQ")

    assert raw == {"data": {"symbol": "QQQ"}}
    assert sleeps == [2.0, 4.0]
