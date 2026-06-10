"""
Unit tests for LocalCheckpoint (disk-backed checkpoint backend).

No network calls. Uses pytest tmp_path for file isolation.
"""

from __future__ import annotations

import json

import pytest

from ingestion.checkpoint import CheckpointBackend, LocalCheckpoint, make_checkpoint


APPID = 730
DT = "2024-01-01"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(tmp_path, dt=DT):
    return LocalCheckpoint(base_dir=tmp_path, dt=dt)


def _state(cursor="cursor_A", page=0):
    return {"cursor": cursor, "next_page_num": page, "appid": APPID}


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------

class TestProtocol:
    def test_local_checkpoint_satisfies_protocol(self, tmp_path):
        cp = _make(tmp_path)
        assert isinstance(cp, CheckpointBackend)


# ---------------------------------------------------------------------------
# Load / Save / Clear
# ---------------------------------------------------------------------------

class TestLoadSaveClear:
    def test_load_nonexistent_returns_none(self, tmp_path):
        cp = _make(tmp_path)
        assert cp.load(APPID) is None

    def test_save_then_load_roundtrip(self, tmp_path):
        cp = _make(tmp_path)
        cp.save(APPID, _state("CURSOR_X", 5))
        loaded = cp.load(APPID)
        assert loaded is not None
        assert loaded["cursor"] == "CURSOR_X"
        assert loaded["next_page_num"] == 5
        assert loaded["appid"] == APPID

    def test_save_overwrites_previous(self, tmp_path):
        cp = _make(tmp_path)
        cp.save(APPID, _state("first", 0))
        cp.save(APPID, _state("second", 1))
        loaded = cp.load(APPID)
        assert loaded["cursor"] == "second"
        assert loaded["next_page_num"] == 1

    def test_clear_removes_checkpoint(self, tmp_path):
        cp = _make(tmp_path)
        cp.save(APPID, _state())
        cp.clear(APPID)
        assert cp.load(APPID) is None

    def test_clear_nonexistent_is_noop(self, tmp_path):
        cp = _make(tmp_path)
        cp.clear(APPID)  # must not raise

    def test_different_appids_are_independent(self, tmp_path):
        cp = _make(tmp_path)
        cp.save(730, _state("cursor_730", 3))
        cp.save(570, _state("cursor_570", 7))
        assert cp.load(730)["cursor"] == "cursor_730"
        assert cp.load(570)["cursor"] == "cursor_570"

    def test_different_dts_are_independent(self, tmp_path):
        cp_a = LocalCheckpoint(tmp_path, "2024-01-01")
        cp_b = LocalCheckpoint(tmp_path, "2024-01-02")
        cp_a.save(APPID, _state("cursor_day1", 2))
        cp_b.save(APPID, _state("cursor_day2", 0))
        assert cp_a.load(APPID)["cursor"] == "cursor_day1"
        assert cp_b.load(APPID)["cursor"] == "cursor_day2"


# ---------------------------------------------------------------------------
# Atomic write behaviour
# ---------------------------------------------------------------------------

class TestAtomicWrite:
    def test_checkpoint_file_is_valid_json(self, tmp_path):
        cp = _make(tmp_path)
        cp.save(APPID, _state("some_cursor", 4))
        raw = cp.checkpoint_path(APPID).read_text(encoding="utf-8")
        parsed = json.loads(raw)  # must not raise
        assert parsed["cursor"] == "some_cursor"

    def test_corrupted_file_returns_none(self, tmp_path):
        cp = _make(tmp_path)
        cp.save(APPID, _state())
        # Corrupt the file
        cp.checkpoint_path(APPID).write_text("NOT JSON {{{", encoding="utf-8")
        assert cp.load(APPID) is None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

class TestFactory:
    def test_make_checkpoint_local(self, tmp_path):
        cp = make_checkpoint("local", base_dir=tmp_path, dt=DT)
        assert isinstance(cp, LocalCheckpoint)

    def test_make_checkpoint_unknown_backend_raises(self, tmp_path):
        with pytest.raises(NotImplementedError, match="gcs"):
            make_checkpoint("gcs", base_dir=tmp_path, dt=DT)
