"""
Spark MLlib ALS recommender for Steam game recommendations — DESIGN.md §8.

Pipeline:
  1. Load silver Parquet from GCS → implicit rating matrix
  2. Train/test split (80/20, seed=42)
  3. Train ALS (implicit-feedback mode)
  4. Evaluate: Precision@10 + NDCG@10 (pyspark.mllib.evaluation.RankingMetrics)
  5. Generate top-N recommendations for ALL users
  6. Join with dim_users Parquet (pre-exported from BQ) to resolve user_sk
  7. Write recommendation Parquet to GCS (bq load handles the final BQ write)

ID mapping note:
  Spark MLlib ALS requires non-negative INT32 user/item IDs.
  FARM_FINGERPRINT produces signed INT64 (potentially negative), so we use
  dense_rank() over steamid/appid to build consecutive [0, N) integer indices
  for internal model training, then map back to BQ keys via a dim_users join.

IAM note:
  The Spark SA only needs: silver bucket (read), staging bucket (read/write).
  BQ operations (bq extract dim_users, bq load recommendations) run locally
  with the user's gcloud credentials — see scripts/dataproc_als_train.sh.

Usage (Dataproc Serverless):
  See scripts/dataproc_als_train.sh — it handles bq extract / bq load framing.
"""

from __future__ import annotations

import argparse
import logging
import math
import pathlib
import sys
from datetime import UTC, datetime

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.ml.recommendation import ALS
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

# Implicit rating formula (matches build_matrix.py)
_MAX_LOG_HOURS: float = math.log(1.0 + 500.0)

# ALS hyper-parameters
_ALS_RANK = 50
_ALS_ITER = 10
_ALS_REG = 0.01
_ALS_ALPHA = 40.0

_EVAL_K = 10


# ---------------------------------------------------------------------------
# Step 1 — Load and aggregate implicit ratings
# ---------------------------------------------------------------------------


def _build_interactions(spark: SparkSession, silver_path: str) -> DataFrame:
    """Load silver Parquet and return aggregated (steamid, appid, rating) pairs.

    Implicit rating formula (DESIGN.md §6):
        voted_up_score  = 1.0 if voted_up else 0.0
        playtime_boost  = clamp(ln(1 + minutes/60) / ln(501), 0, 1)
        implicit_rating = 0.6 * voted_up_score + 0.4 * playtime_boost

    Aggregation: max implicit_rating per (steamid, appid) pair to handle
    any duplicate reviews across multiple dt partitions.
    """
    raw = spark.read.parquet(silver_path)

    with_rating = (
        raw.withColumn(
            "voted_up_score",
            F.when(F.col("voted_up"), F.lit(1.0)).otherwise(F.lit(0.0)).cast(T.DoubleType()),
        )
        .withColumn(
            "playtime_boost",
            F.least(
                F.log(
                    F.lit(1.0)
                    + F.coalesce(F.col("playtime_at_review"), F.lit(0)).cast(T.DoubleType())
                    / F.lit(60.0),
                )
                / F.lit(_MAX_LOG_HOURS),
                F.lit(1.0),
            ).cast(T.DoubleType()),
        )
        .withColumn(
            "implicit_rating",
            (F.lit(0.6) * F.col("voted_up_score") + F.lit(0.4) * F.col("playtime_boost")).cast(
                T.DoubleType()
            ),
        )
        .filter(F.col("steamid").isNotNull())
    )

    interactions = with_rating.groupBy("steamid", "appid").agg(
        F.max("implicit_rating").alias("rating")
    )

    n = interactions.count()
    logger.info("Loaded %d (user, game) interaction pairs from %s", n, silver_path)
    return interactions


# ---------------------------------------------------------------------------
# Step 2 — Integer ID mapping for ALS
# ---------------------------------------------------------------------------


def _add_integer_ids(
    interactions: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Map steamid/appid to consecutive non-negative INT32 indices for ALS.

    Returns:
        als_data  — (user_idx INT, item_idx INT, rating DOUBLE, steamid, appid)
        user_map  — (steamid, user_idx)
        item_map  — (appid,   item_idx)
    """
    # dense_rank returns BIGINT; subtract 1 for 0-based indexing
    user_map = (
        interactions.select("steamid")
        .distinct()
        .withColumn(
            "user_idx",
            (F.dense_rank().over(Window.orderBy("steamid")) - 1).cast(T.IntegerType()),
        )
    )
    item_map = (
        interactions.select("appid")
        .distinct()
        .withColumn(
            "item_idx",
            (F.dense_rank().over(Window.orderBy("appid")) - 1).cast(T.IntegerType()),
        )
    )

    als_data = (
        interactions.join(user_map, "steamid")
        .join(item_map, "appid")
        .select("user_idx", "item_idx", "rating", "steamid", "appid")
        .cache()
    )

    n_users = user_map.count()
    n_items = item_map.count()
    logger.info("Indexed %d users and %d games for ALS", n_users, n_items)
    return als_data, user_map, item_map


# ---------------------------------------------------------------------------
# Step 3 — Train ALS and evaluate offline
# ---------------------------------------------------------------------------


def _train_and_evaluate(train: DataFrame, test: DataFrame, k: int = _EVAL_K) -> tuple:
    """Train implicit-feedback ALS and compute ranking metrics on the test set.

    Returns (model, precision_at_k, ndcg_at_k).
    """
    als = ALS(
        rank=_ALS_RANK,
        maxIter=_ALS_ITER,
        regParam=_ALS_REG,
        alpha=_ALS_ALPHA,
        implicitPrefs=True,
        userCol="user_idx",
        itemCol="item_idx",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
        seed=42,
    )

    logger.info(
        "Training ALS — rank=%d  maxIter=%d  regParam=%.4f  alpha=%.1f",
        _ALS_RANK,
        _ALS_ITER,
        _ALS_REG,
        _ALS_ALPHA,
    )
    model = als.fit(train)
    logger.info("ALS training complete")

    # Generate top-k predictions for users that appear in the test set
    test_users = test.select("user_idx").distinct()
    user_preds = model.recommendForUserSubset(test_users, k)

    predicted = user_preds.select(
        "user_idx",
        F.col("recommendations.item_idx").alias("predicted_items"),
    )
    actual = test.groupBy("user_idx").agg(F.collect_list("item_idx").alias("actual_items"))

    eval_df = predicted.join(actual, "user_idx")
    pred_actual_rdd = eval_df.select("predicted_items", "actual_items").rdd.map(tuple)
    metrics = RankingMetrics(pred_actual_rdd)

    p_at_k = metrics.precisionAt(k)
    ndcg_at_k = metrics.ndcgAt(k)
    logger.info("Precision@%d = %.4f  NDCG@%d = %.4f", k, p_at_k, k, ndcg_at_k)
    return model, p_at_k, ndcg_at_k


# ---------------------------------------------------------------------------
# Step 4 — Generate top-N recommendations and write to BigQuery
# ---------------------------------------------------------------------------


def _write_recommendations_gcs(
    all_recs: DataFrame,
    user_map: DataFrame,
    item_map: DataFrame,
    dim_users: DataFrame,
    seen_interactions: DataFrame,
    output_path: str,
    top_n: int,
) -> int:
    """Filter seen items, re-rank, and write top-N unseen recommendations to GCS.

    Pipeline:
      1. Explode all candidate (user_idx, item_idx, score) rows from ALS output
      2. Anti-join against seen_interactions to remove (user_idx, item_idx) pairs
         the user has already interacted with (any interaction, train or test)
      3. Re-rank per user by descending score using a window function
      4. Keep top_n rows per user
      5. Map integer indices back to (user_sk, appid) and write Parquet

    seen_interactions must contain at least columns: user_idx (INT), item_idx (INT).
    The caller passes the full als_data (train ∪ test) so no previously-seen game
    can appear in the output regardless of which split it fell into.

    The caller (dataproc_als_train.sh) handles the final `bq load` step.

    Returns the number of recommendation rows written.
    """
    generated_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Explode all candidates (already scored/sorted inside the ALS struct)
    exploded = all_recs.select(
        "user_idx",
        F.posexplode("recommendations").alias("_pos", "rec"),
    ).select(
        "user_idx",
        F.col("rec.item_idx").alias("item_idx"),
        F.col("rec.rating").cast(T.DoubleType()).alias("score"),
    )

    # Remove items the user has already interacted with (anti-join)
    seen = seen_interactions.select("user_idx", "item_idx").distinct()
    unseen = exploded.join(seen, ["user_idx", "item_idx"], "left_anti")

    # Re-rank per user by score (descending) and keep top_n
    rank_win = Window.partitionBy("user_idx").orderBy(F.col("score").desc())
    top_unseen = unseen.withColumn("rank", F.row_number().over(rank_win).cast(T.LongType())).filter(
        F.col("rank") <= top_n
    )

    # Map indices back to original IDs and resolve user_sk
    result = (
        top_unseen.join(user_map, "user_idx")  # → steamid
        .join(item_map, "item_idx")  # → appid
        .join(dim_users, "steamid")  # → user_sk (FARM_FINGERPRINT from BQ dbt)
        .filter(F.col("user_sk").isNotNull())
        .select(
            F.col("user_sk").cast(T.LongType()),
            F.col("appid").cast(T.LongType()).alias("ranked_appid"),
            F.col("rank"),
            F.col("score"),
            F.to_timestamp(F.lit(generated_ts)).alias("generated_at"),
        )
    )

    n_rows = result.count()
    logger.info(
        "Writing %d recommendation rows (seen-items filtered) to GCS: %s",
        n_rows,
        output_path,
    )
    result.write.mode("overwrite").parquet(output_path)
    logger.info("Wrote recommendations Parquet to %s", output_path)
    return n_rows


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run(spark: SparkSession, args: argparse.Namespace) -> None:
    """Full ALS training pipeline."""
    silver_path = f"gs://{args.silver_bucket}/reviews/dt={args.dt}/"
    logger.info("=== ALS training  dt=%s ===", args.dt)

    # 1. Load interactions
    interactions = _build_interactions(spark, silver_path)
    als_data, user_map, item_map = _add_integer_ids(interactions)

    # 2. 80/20 train/test split
    train, test = als_data.randomSplit([0.8, 0.2], seed=42)
    n_train = train.count()
    n_test = test.count()
    logger.info("Split → train=%d  test=%d interactions", n_train, n_test)

    # 3. Train ALS + offline evaluation
    model, p_at_k, ndcg_at_k = _train_and_evaluate(train, test, k=_EVAL_K)

    print(f"\n{'='*60}")
    print(f"  ALS Offline Evaluation (k={_EVAL_K})")
    print(f"  Precision@{_EVAL_K:2d} : {p_at_k:.4f}")
    print(f"  NDCG@{_EVAL_K:2d}      : {ndcg_at_k:.4f}")
    print(f"{'='*60}\n")

    # 4. Generate ALL item candidates per user so the seen-item filter always has
    #    enough unseen items to fill top_n (dataset has 101 games; even a user
    #    who has reviewed 90 of them still gets 11 unseen candidates).
    n_items = item_map.count()
    logger.info(
        "Generating all %d item candidates per user (seen-item filter applied later)…",
        n_items,
    )
    all_recs = model.recommendForAllUsers(n_items)

    # 5. Load user_sk mapping from pre-exported dim_users Parquet (GCS)
    #    The export is done by dataproc_als_train.sh before submitting this job.
    logger.info("Reading dim_users from %s", args.dim_users_path)
    dim_users = spark.read.parquet(args.dim_users_path).select("steamid", F.col("user_sk"))

    # 6. Write top-N unseen recommendations Parquet to GCS
    output_path = f"gs://{args.staging_bucket}/recommendations/{args.dt}/"
    n_rows = _write_recommendations_gcs(
        all_recs,
        user_map,
        item_map,
        dim_users,
        seen_interactions=als_data,  # full train ∪ test — no seen item survives
        output_path=output_path,
        top_n=args.top_n,
    )

    print(f"\n{'='*60}")
    print("  Recommendations Parquet written to GCS (seen-items filtered)")
    print(f"  Path      : {output_path}")
    print(f"  Rows      : {n_rows:,}")
    print(f"  Top-N     : {args.top_n}")
    print("  Next step : bq load → game_recommendations")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Train Spark MLlib ALS and write top-N recommendations to GCS Parquet"
    )
    p.add_argument(
        "--dt", required=True, help="Snapshot date YYYY-MM-DD (selects silver partition)"
    )
    p.add_argument(
        "--silver-bucket", required=True, help="GCS bucket name (no gs://) for silver Parquet"
    )
    p.add_argument("--staging-bucket", required=True, help="GCS bucket name for output Parquet")
    p.add_argument(
        "--dim-users-path",
        required=True,
        help="GCS path to dim_users Parquet (pre-exported from BQ by the submit script)",
    )
    p.add_argument("--top-n", type=int, default=10, help="Top-N recommendations per user")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    spark = (
        SparkSession.builder.appName("steam-als-recommender")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        run(spark, args)
    finally:
        spark.stop()
