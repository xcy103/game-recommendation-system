"""
Target appid list — provides the set of Steam games to ingest.

Two modes:
    1. Live  : call SteamSpy top100in2weeks API, return top-N appids.
    2. Static: return STATIC_APPIDS (no network, good for CI / smoke tests).

SteamSpy endpoint: https://steamspy.com/api.php?request=top100in2weeks
Response: dict of {str(appid): {name, owners, ...}}
"""

from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Static fallback list (used in CI, offline dev, and as a SteamSpy fallback)
# Covers a cross-section of genres with large review volumes.
# ---------------------------------------------------------------------------

STATIC_APPIDS: list[int] = [
    730,  # Counter-Strike 2
    570,  # Dota 2
    440,  # Team Fortress 2
    271590,  # Grand Theft Auto V
    1172470,  # Apex Legends
    1245620,  # Elden Ring
    292030,  # The Witcher 3: Wild Hunt
    578080,  # PUBG: Battlegrounds
    252950,  # Rocket League
    413150,  # Stardew Valley
    1091500,  # Cyberpunk 2077
    1174180,  # Red Dead Redemption 2
    892970,  # Valheim
    945360,  # Among Us
    1517290,  # Battlefield 2042
]

STEAMSPY_URL = "https://steamspy.com/api.php"
_STEAMSPY_PARAMS = {"request": "top100in2weeks"}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def from_steamspy(
    top_n: int = 100,
    session: requests.Session | None = None,
    timeout: int = 15,
) -> list[int]:
    """Fetch top-N appids from SteamSpy's top100in2weeks endpoint.

    Args:
        top_n:   Maximum number of appids to return.
        session: Optional requests.Session (injectable for testing).
        timeout: HTTP timeout in seconds.

    Returns:
        List of appid integers, ordered by SteamSpy rank (arbitrary dict
        order in Python 3.7+ preserves insertion order from the API).

    Raises:
        requests.RequestException: on network / HTTP errors.
    """
    sess = session or requests.Session()
    logger.info("Fetching top %d appids from SteamSpy …", top_n)
    resp = sess.get(STEAMSPY_URL, params=_STEAMSPY_PARAMS, timeout=timeout)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    appids = [int(k) for k in data.keys()][:top_n]
    logger.info("SteamSpy returned %d appids (capped at %d)", len(appids), top_n)
    return appids


def from_static(top_n: int | None = None) -> list[int]:
    """Return a slice of the built-in static appid list.

    Args:
        top_n: Max count to return. None returns all STATIC_APPIDS.
    """
    return STATIC_APPIDS[:top_n] if top_n is not None else list(STATIC_APPIDS)


def get_appids(
    top_n: int = 100,
    *,
    use_static: bool = False,
    session: requests.Session | None = None,
) -> list[int]:
    """Main entry point: return a list of appids to ingest.

    If use_static is True, or if the SteamSpy request fails, falls back
    to the static list so CI and offline dev never require a network call.

    Args:
        top_n:       Target count.
        use_static:  Skip network call entirely.
        session:     Optional requests.Session for testing.
    """
    if use_static:
        result = from_static(top_n)
        logger.info("Using static appid list (%d appids)", len(result))
        return result

    try:
        return from_steamspy(top_n=top_n, session=session)
    except Exception as exc:
        logger.warning("SteamSpy fetch failed (%s); falling back to static list", exc)
        return from_static(top_n)
