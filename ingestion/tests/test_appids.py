"""
Unit tests for appids module.

ALL HTTP calls are intercepted by `responses` — no real SteamSpy calls.
"""

from __future__ import annotations

import pytest
import responses as resp_lib

from ingestion.appids import (
    STATIC_APPIDS,
    STEAMSPY_URL,
    from_static,
    from_steamspy,
    get_appids,
)

# Minimal fake SteamSpy response: {str(appid): {name: ...}}
_FAKE_STEAMSPY = {str(appid): {"name": f"Game {appid}", "owners": "1,000,000"} for appid in range(1000, 1020)}


class TestFromStatic:
    def test_returns_list_of_ints(self):
        result = from_static()
        assert all(isinstance(a, int) for a in result)

    def test_top_n_limits_count(self):
        result = from_static(top_n=5)
        assert len(result) == 5

    def test_returns_copy_not_reference(self):
        a = from_static()
        b = from_static()
        assert a is not b


class TestFromSteamspy:
    @resp_lib.activate
    def test_parses_appids_from_response(self):
        resp_lib.add(resp_lib.GET, STEAMSPY_URL, json=_FAKE_STEAMSPY, status=200)
        result = from_steamspy(top_n=10)
        assert len(result) == 10
        assert all(isinstance(a, int) for a in result)

    @resp_lib.activate
    def test_top_n_caps_result(self):
        resp_lib.add(resp_lib.GET, STEAMSPY_URL, json=_FAKE_STEAMSPY, status=200)
        result = from_steamspy(top_n=5)
        assert len(result) == 5

    @resp_lib.activate
    def test_http_error_propagates(self):
        resp_lib.add(resp_lib.GET, STEAMSPY_URL, status=500)
        with pytest.raises(Exception):
            from_steamspy()


class TestGetAppids:
    def test_use_static_skips_network(self):
        # No responses registered → any real network call would raise
        result = get_appids(top_n=5, use_static=True)
        assert len(result) == 5
        assert set(result).issubset(set(STATIC_APPIDS))

    @resp_lib.activate
    def test_falls_back_to_static_on_network_error(self):
        resp_lib.add(resp_lib.GET, STEAMSPY_URL, status=503)
        result = get_appids(top_n=5, use_static=False)
        # Should not raise; returns static fallback
        assert len(result) == 5
        assert all(isinstance(a, int) for a in result)

    @resp_lib.activate
    def test_live_mode_uses_steamspy(self):
        resp_lib.add(resp_lib.GET, STEAMSPY_URL, json=_FAKE_STEAMSPY, status=200)
        result = get_appids(top_n=10, use_static=False)
        assert len(result) == 10
        # All appids come from the fake response, not STATIC_APPIDS
        assert set(result).issubset({int(k) for k in _FAKE_STEAMSPY})
