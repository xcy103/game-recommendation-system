"""
Build the user-item interaction matrix for ALS collaborative filtering (DESIGN.md §6, step 7).

Reads silver Parquet and writes a compact interaction table:
    | user_sk         | appid | implicit_rating | dt |
    |-----------------|-------|-----------------|-----|
    | steamid (str)   | int   | float [0, 1]    | str |

Note: user_sk here is the raw steamid string. recommender.py replaces it with an
integer surrogate key after joining with dim_users.

Implicit rating formula (DESIGN.md §6):
    voted_up_score  = 1.0 if voted_up else 0.0
    playtime_boost  = clamp(ln(1 + hours_played) / ln(501), 0, 1)
    implicit_rating = 0.6 * voted_up_score + 0.4 * playtime_boost

Output path:
    {output}/user_item_matrix/   partitioned by dt

Usage (local):
    python spark/build_matrix.py \\
        --input ./data/silver \\
        --output ./data/silver \\
        --dt 2024-01-01
"""
from __future__ import annotations

import argparse
import logging
import math
import pathlib
import sys

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)

# Normalisation ceiling: log(1 + 500 hours) ≈ 6.22
# A player with 500+ hours gets a playtime_boost capped at 1.0.
_MAX_LOG_HOURS = math.log(1.0 + 500.0)


def build_user_item_matrix(df: DataFrame) -> DataFrame:
    """Transform a silver reviews DataFrame into a user-item interaction table.

    Aggregates per (steamid, appid) pair by summing implicit ratings so a user
    who reviewed the same game across multiple dt partitions gets one row.
    """
    df = (
        df.withColumn(
            "voted_up_score",
            F.when(F.col("voted_up"), F.lit(1.0)).otherwise(F.lit(0.0)).cast(T.DoubleType()),
        )
        .withColumn(
            "playtime_boost",
            F.least(
                F.log(
                    F.lit(1.0) + F.coalesce(F.col("playtime_at_review"), F.lit(0)).cast(T.DoubleType()) / F.lit(60.0)
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
    )

    # One row per (user, game) — keep the max rating if duplicates exist.
    return (
        df.groupBy(F.col("steamid").alias("user_sk"), F.col("appid"), F.col("dt"))
        .agg(F.max("implicit_rating").alias("implicit_rating"))
        .select("user_sk", "appid", "implicit_rating", "dt")
    )


def run(
    spark: SparkSession,
    dt: str,
    input_path: str,
    output_path: str,
) -> int:
    """Build and write the user-item matrix for the given dt.

    Returns the number of (user, appid) pairs written.
    """
    # Read from the explicit dt= partition path (silver is NOT written with
    # Spark partitionBy — dt is a real DATE column inside the Parquet files).
    silver_path = f"{input_path}/reviews/dt={dt}/"
    logger.info("Reading silver Parquet from %s", silver_path)
    df = spark.read.parquet(silver_path)

    matrix = build_user_item_matrix(df)

    out = f"{output_path}/user_item_matrix/"
    logger.info("Writing user-item matrix (partitioned by dt) to %s", out)
    matrix.write.mode("overwrite").partitionBy("dt").parquet(out)

    n = matrix.count()
    logger.info("Written %d (user, appid) pairs for dt=%s", n, dt)
    return n


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Build user-item interaction matrix from silver Parquet")
    p.add_argument("--dt", required=True, help="Partition date YYYY-MM-DD")
    p.add_argument("--input", required=True, help="Silver base path (contains reviews/ subdir)")
    p.add_argument("--output", required=True, help="Output base path for user_item_matrix/")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    spark = (
        SparkSession.builder.appName("steam-build-matrix")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        n = run(spark, args.dt, args.input, args.output)
        # Print the first rows for quick visual inspection.
        matrix = spark.read.parquet(f"{args.output}/user_item_matrix/").filter(
            F.col("dt") == args.dt
        )
        print(f"\n{'='*60}")
        print(f"  User-item matrix  dt={args.dt}  rows={n}")
        print(f"{'='*60}")
        matrix.show(10, truncate=False)
    finally:
        spark.stop()
