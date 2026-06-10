"""
Cursor checkpoint — persists per-appid pagination state across runs.

Design: a thin Protocol defines the backend interface so LocalCheckpoint
(JSON files on disk) can be swapped for a GCS-backed implementation without
touching the ingester logic.

State stored per appid:
    {
        "cursor":        str   — the cursor to send on the NEXT request,
        "next_page_num": int   — integer used to name the next output file,
        "appid":         int   — for self-documentation
    }

Bronze resume guarantee:
    After a crash, re-running ingest_appid() loads the saved state, starts
    paging from `cursor` and writes files starting at `next_page_num`, so
    previously written part-*.json files are never overwritten.
"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile
from typing import Protocol, TypedDict, runtime_checkable

# ---------------------------------------------------------------------------
# Shared types
# ---------------------------------------------------------------------------


class CheckpointState(TypedDict):
    cursor: str  # pass this as the `cursor` query-param on next request
    next_page_num: int  # next file will be named part-{next_page_num:05d}.json
    appid: int


# ---------------------------------------------------------------------------
# Protocol — swap implementations without changing caller code
# ---------------------------------------------------------------------------


@runtime_checkable
class CheckpointBackend(Protocol):
    def load(self, appid: int) -> CheckpointState | None:
        """Return saved state for appid, or None if no checkpoint exists."""
        ...

    def save(self, appid: int, state: CheckpointState) -> None:
        """Atomically persist state for appid."""
        ...

    def clear(self, appid: int) -> None:
        """Delete saved state for appid (called after a successful full run)."""
        ...


# ---------------------------------------------------------------------------
# LocalCheckpoint — JSON files under {base_dir}/_checkpoints/dt={dt}/
# ---------------------------------------------------------------------------


class LocalCheckpoint:
    """File-backed checkpoint using one JSON file per (dt, appid).

    Layout:
        {base_dir}/_checkpoints/dt={dt}/appid={appid}.json

    Writes are atomic: we write to a sibling .tmp file and os.replace() it,
    so a crash mid-write never leaves a corrupted checkpoint.

    Args:
        base_dir: Root directory (mirrors the bronze output root).
        dt:       Partition date string "YYYY-MM-DD".
    """

    def __init__(self, base_dir: str | pathlib.Path, dt: str) -> None:
        self._dir = pathlib.Path(base_dir) / "_checkpoints" / f"dt={dt}"
        self._dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # CheckpointBackend protocol implementation
    # ------------------------------------------------------------------

    def load(self, appid: int) -> CheckpointState | None:
        p = self._path(appid)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            # Corrupted checkpoint — start fresh rather than crashing.
            return None

    def save(self, appid: int, state: CheckpointState) -> None:
        """Atomic write: tmp → replace."""
        target = self._path(appid)
        fd, tmp = tempfile.mkstemp(dir=self._dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f)
            os.replace(tmp, target)
        except Exception:
            # Clean up temp file if something went wrong.
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def clear(self, appid: int) -> None:
        p = self._path(appid)
        if p.exists():
            p.unlink()

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def checkpoint_path(self, appid: int) -> pathlib.Path:
        """Return the path to the checkpoint file (useful for debugging)."""
        return self._path(appid)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _path(self, appid: int) -> pathlib.Path:
        return self._dir / f"appid={appid}.json"


# ---------------------------------------------------------------------------
# Factory — keeps caller code backend-agnostic
# ---------------------------------------------------------------------------


def make_checkpoint(
    backend: str = "local",
    *,
    base_dir: str | pathlib.Path,
    dt: str,
) -> LocalCheckpoint:
    """Instantiate a checkpoint backend by name.

    Currently only "local" is supported. "gcs" backend is not yet implemented.

    Args:
        backend:  "local" (disk) or "gcs" (future).
        base_dir: Root output directory.
        dt:       Partition date "YYYY-MM-DD".
    """
    if backend == "local":
        return LocalCheckpoint(base_dir=base_dir, dt=dt)
    raise NotImplementedError(f"Checkpoint backend '{backend}' not yet implemented. Use 'local'.")
