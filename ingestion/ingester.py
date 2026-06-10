"""
Steam review ingester — rate-limited, cursor-paginated, checkpoint-resumable.

Bronze output path (mirrors DESIGN.md §4):
    {out_dir}/bronze/reviews/dt={dt}/appid={appid}/part-{n:05d}.json

Each file is one raw Steam API page response (JSON), append-only.
On restart the ingester reads the checkpoint and picks up from the last
saved cursor, writing files starting at the saved next_page_num so
previously written files are never overwritten.

CLI:
    python -m ingestion.ingester \\
        --appids 730 570 \\
        --dt 2024-01-01 \\
        --out ./data/bronze \\
        [--language english] \\
        [--rps 1.0] \\
        [--max-pages 50]

    python -m ingestion.ingester \\
        --appids-file appids.txt \\
        --dt 2024-01-01 \\
        --out ./data/bronze
"""

from __future__ import annotations

import argparse
import json
import logging
import pathlib
import sys
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.checkpoint import (
    CheckpointBackend,
    CheckpointState,
    make_checkpoint,
)
from ingestion.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STEAM_REVIEW_URL = "https://store.steampowered.com/appreviews/{appid}"
_INITIAL_CURSOR = "*"
_DEFAULT_NUM_PER_PAGE = 100


# ---------------------------------------------------------------------------
# Retry logic (tenacity)
# ---------------------------------------------------------------------------


def _is_retryable(exc: BaseException) -> bool:
    """Retry on HTTP 429 / 5xx and transient network errors."""
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in (429, 500, 502, 503, 504)
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def _make_retry(*, max_attempts: int = 5, wait_min: float = 2.0, wait_max: float = 60.0):
    """Return a tenacity retry decorator configured for Steam API calls."""
    return retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


# Module-level retry decorator used in production.
# Tests can override via monkeypatching or by calling _fetch_page directly
# with a patched session; time.sleep is also commonly patched in tests.
_PROD_RETRY = _make_retry()


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


def _fetch_page(
    session: requests.Session,
    appid: int,
    cursor: str,
    language: str = "english",
    num_per_page: int = _DEFAULT_NUM_PER_PAGE,
    *,
    _retry_decorator=None,
) -> dict[str, Any]:
    """Fetch one page of reviews from the Steam API.

    Args:
        session:          requests.Session (injectable for testing).
        appid:            Steam application ID.
        cursor:           Pagination cursor. Use "*" for the first page.
        language:         Review language filter ("english", "all", etc.).
        num_per_page:     Reviews per page (max 100).
        _retry_decorator: Override retry behaviour (tests use wait_none()).

    Returns:
        Parsed JSON response dict.

    Raises:
        requests.HTTPError: for non-retryable HTTP errors.
        tenacity.RetryError: if all retry attempts are exhausted.
    """
    retry_dec = _retry_decorator if _retry_decorator is not None else _PROD_RETRY

    @retry_dec
    def _do_get() -> requests.Response:
        resp = session.get(
            STEAM_REVIEW_URL.format(appid=appid),
            params={
                "json": "1",
                "cursor": cursor,
                "filter": "recent",
                "language": language,
                "review_type": "all",
                "purchase_type": "all",
                "num_per_page": num_per_page,
            },
            timeout=30,
        )
        # Raise HTTPError for 4xx/5xx so tenacity can inspect it.
        resp.raise_for_status()
        return resp

    return _do_get().json()


# ---------------------------------------------------------------------------
# Bronze file I/O
# ---------------------------------------------------------------------------


def _bronze_dir(out_dir: pathlib.Path, dt: str, appid: int) -> pathlib.Path:
    """Return (and create) the directory for one appid's bronze files."""
    p = out_dir / "bronze" / "reviews" / f"dt={dt}" / f"appid={appid}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write_page(
    data: dict, out_dir: pathlib.Path, dt: str, appid: int, page_num: int
) -> pathlib.Path:
    """Write a raw API response dict to a bronze part file.

    Returns the path written.
    """
    dest = _bronze_dir(out_dir, dt, appid) / f"part-{page_num:05d}.json"
    dest.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    logger.debug("Wrote %s", dest)
    return dest


# ---------------------------------------------------------------------------
# Core ingestion loop
# ---------------------------------------------------------------------------


def ingest_appid(
    appid: int,
    dt: str,
    out_dir: pathlib.Path | str,
    checkpoint: CheckpointBackend,
    rate_limiter: RateLimiter,
    *,
    language: str = "english",
    num_per_page: int = _DEFAULT_NUM_PER_PAGE,
    max_pages: int | None = None,
    session: requests.Session | None = None,
    _retry_decorator=None,
) -> int:
    """Ingest all review pages for one appid into the bronze layer.

    Resumes from the checkpoint if one exists; otherwise starts from "*".
    Saves checkpoint after every successfully written page.

    Args:
        appid:             Steam application ID.
        dt:                Partition date "YYYY-MM-DD".
        out_dir:           Base output directory.
        checkpoint:        Backend for saving/loading pagination state.
        rate_limiter:      Throttles outgoing requests.
        language:          Review language (default "english").
        num_per_page:      Reviews per page (max 100).
        max_pages:         Hard cap on pages per appid (useful for smoke tests).
        session:           requests.Session; a new one is created if None.
        _retry_decorator:  Override retry for tests.

    Returns:
        Number of pages written in this run.
    """
    out_dir = pathlib.Path(out_dir)
    session = session or requests.Session()

    # --- resume or start fresh ---
    saved = checkpoint.load(appid)
    if saved:
        cursor = saved["cursor"]
        next_page_num = saved["next_page_num"]
        logger.info(
            "appid=%s: resuming from checkpoint — cursor=%r next_page_num=%d",
            appid,
            cursor,
            next_page_num,
        )
    else:
        cursor = _INITIAL_CURSOR
        next_page_num = 0
        logger.info("appid=%s: starting fresh (no checkpoint)", appid)

    pages_written = 0
    # Tracks cursors we have already successfully used as a request param.
    # Stops a long multi-hop cycle: A→B→C→A without needing to re-request A.
    used_cursors: set[str] = set()

    while True:
        if max_pages is not None and pages_written >= max_pages:
            logger.info("appid=%s: reached max_pages=%d, stopping", appid, max_pages)
            break

        # --- rate limit ---
        rate_limiter.acquire()

        # --- fetch ---
        logger.debug("appid=%s: fetching cursor=%r", appid, cursor)
        data = _fetch_page(
            session,
            appid,
            cursor,
            language,
            num_per_page,
            _retry_decorator=_retry_decorator,
        )

        # --- stop conditions (all checked BEFORE writing) ---
        reviews = data.get("reviews", [])
        new_cursor = data.get("cursor", "")

        if not reviews:
            logger.info("appid=%s: no reviews returned — pagination complete", appid)
            break

        # Primary loop-detection: API returned the same cursor we just sent.
        # This is the Steam API's canonical "end of pagination" signal.
        if new_cursor == cursor:
            logger.info("appid=%s: cursor unchanged (%r) — pagination complete", appid, cursor)
            break

        # Secondary: detect longer cycles (A→B→C→A) across many pages.
        if new_cursor and new_cursor in used_cursors:
            logger.info(
                "appid=%s: cursor %r already used — loop detected, stopping", appid, new_cursor
            )
            break

        # --- write page ---
        _write_page(data, out_dir, dt, appid, next_page_num)
        pages_written += 1

        # --- save checkpoint (cursor for the NEXT request) ---
        used_cursors.add(cursor)
        next_state: CheckpointState = {
            "cursor": new_cursor,
            "next_page_num": next_page_num + 1,
            "appid": appid,
        }
        checkpoint.save(appid, next_state)
        logger.info(
            "appid=%s: page %d written, %d reviews, next_cursor=%r",
            appid,
            next_page_num,
            len(reviews),
            new_cursor,
        )

        cursor = new_cursor
        next_page_num += 1

    # Clear checkpoint only after a successful complete run.
    if pages_written > 0:
        checkpoint.clear(appid)
        logger.info("appid=%s: checkpoint cleared, %d pages total", appid, pages_written)

    return pages_written


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def ingest(
    appids: list[int],
    dt: str,
    out_dir: str | pathlib.Path,
    *,
    checkpoint_dir: str | pathlib.Path | None = None,
    language: str = "english",
    rps: float = 1.0,
    jitter: float = 0.2,
    max_pages: int | None = None,
) -> dict[int, int]:
    """Ingest multiple appids. Returns {appid: pages_written}."""
    out_dir = pathlib.Path(out_dir)
    cp_dir = pathlib.Path(checkpoint_dir) if checkpoint_dir else out_dir
    rate_limiter = RateLimiter(rate=rps, jitter=jitter)

    results: dict[int, int] = {}
    for appid in appids:
        checkpoint = make_checkpoint("local", base_dir=cp_dir, dt=dt)
        try:
            n = ingest_appid(
                appid,
                dt,
                out_dir,
                checkpoint,
                rate_limiter,
                language=language,
                max_pages=max_pages,
            )
            results[appid] = n
        except Exception:
            logger.exception("appid=%s: ingestion failed", appid)
            results[appid] = -1

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m ingestion.ingester",
        description="Ingest Steam reviews into the bronze layer.",
    )
    id_group = p.add_mutually_exclusive_group(required=True)
    id_group.add_argument(
        "--appids",
        nargs="+",
        type=int,
        metavar="APPID",
        help="One or more Steam appids, e.g. --appids 730 570",
    )
    id_group.add_argument(
        "--appids-file",
        type=pathlib.Path,
        metavar="FILE",
        help="Text file with one appid per line.",
    )
    p.add_argument(
        "--dt",
        required=True,
        help="Partition date YYYY-MM-DD, e.g. 2024-01-01",
    )
    p.add_argument(
        "--out",
        type=pathlib.Path,
        default=pathlib.Path("./data/bronze"),
        help="Base output directory (default: ./data/bronze)",
    )
    p.add_argument(
        "--checkpoint-dir",
        type=pathlib.Path,
        default=None,
        help="Directory for checkpoint files (default: same as --out)",
    )
    p.add_argument(
        "--language",
        default="english",
        help="Review language filter (default: english)",
    )
    p.add_argument(
        "--rps",
        type=float,
        default=1.0,
        help="Requests per second (default: 1.0 — be conservative!)",
    )
    p.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Max pages per appid, useful for smoke tests.",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.appids:
        appids = args.appids
    else:
        raw = args.appids_file.read_text(encoding="utf-8").splitlines()
        appids = [int(line.strip()) for line in raw if line.strip() and not line.startswith("#")]

    logger.info("Starting ingestion: %d appids, dt=%s, out=%s", len(appids), args.dt, args.out)

    results = ingest(
        appids=appids,
        dt=args.dt,
        out_dir=args.out,
        checkpoint_dir=args.checkpoint_dir,
        language=args.language,
        rps=args.rps,
        max_pages=args.max_pages,
    )

    ok = sum(v >= 0 for v in results.values())
    fail = sum(v < 0 for v in results.values())
    total_pages = sum(v for v in results.values() if v >= 0)
    logger.info("Done: %d succeeded, %d failed, %d total pages written", ok, fail, total_pages)

    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
