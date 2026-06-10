"""
FastAPI recommender service — GET /recommendations/{user_sk} (DESIGN.md §8).

Queries the BigQuery `game_recommendations` table and returns the top-N
pre-computed game recommendations for a given user.

Endpoints:
  GET /health            → {"status": "ok"}
  GET /recommendations/{user_sk}?top_n=10
      → [{"rank": 1, "appid": 730, "score": 0.94}, ...]

Runtime: Cloud Run (scales to zero). SA: steam-api-dev (BQ dataViewer on gold).

TODO: implement BQ query, response models, error handling.
"""

from fastapi import FastAPI

app = FastAPI(
    title="Steam Game Recommender API",
    description="Returns pre-computed ALS recommendations from BigQuery gold.",
    version="0.1.0",
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/recommendations/{user_sk}")
async def get_recommendations(user_sk: int, top_n: int = 10) -> list[dict]:
    """Return top-N game recommendations for user_sk.

    TODO: query BigQuery game_recommendations table and return results.
    """
    raise NotImplementedError("Not yet implemented")
