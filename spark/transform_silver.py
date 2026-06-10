"""
PySpark job: bronze JSON → silver Parquet (DESIGN.md §6).

Works locally (local SparkSession) and on Dataproc — paths are abstract
strings so the same code runs with gs:// or local file: URIs.

Bronze layout read:
    {input}/reviews/dt={dt}/appid=*/*.json     (one JSON object per file)

Silver layout written:
    {output}/reviews/          partitioned by dt
        └── dt={dt}/part-*.parquet

Usage (local):
    python spark/transform_silver.py \\
        --input ./data/bronze \\
        --output ./data/silver \\
        --dt 2024-01-01

Usage (Dataproc / spark-submit):
    spark-submit spark/transform_silver.py \\
        --input gs://<bronze-bucket> \\
        --output gs://<silver-bucket> \\
        --dt 2024-01-01
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys

# Make `from spark.X import ...` work when running as a script from the project root.
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from spark.sentiment import add_sentiment_columns

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step 1 — Read bronze
# ---------------------------------------------------------------------------


def read_bronze(spark: SparkSession, input_path: str, dt: str) -> DataFrame:
    """Read all bronze JSON pages for a given dt partition.

    Each file is one full Steam API page (a single JSON object that may span
    multiple lines), so multiLine=true is required.
    """
    pattern = f"{input_path}/reviews/dt={dt}/appid=*/*.json"
    logger.info("Reading bronze pattern: %s", pattern)
    return (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .json(pattern)
        .withColumn("_file_path", F.input_file_name())
    )


# ---------------------------------------------------------------------------
# Step 2 — Flatten author.* and enforce schema
# ---------------------------------------------------------------------------


def flatten_reviews(raw_df: DataFrame, dt: str) -> DataFrame:
    """Explode the reviews array and flatten nested author.* fields.

    Extracts appid from the Hive-style partition path so the column is
    available for both local and GCS paths.
    """
    exploded = raw_df.select(
        F.explode("reviews").alias("r"),
        F.regexp_extract(F.col("_file_path"), r"appid=(\d+)", 1)
        .cast(T.IntegerType())
        .alias("appid"),
    )
    return exploded.select(
        F.col("r.recommendationid").cast(T.StringType()).alias("recommendationid"),
        F.col("r.author.steamid").cast(T.StringType()).alias("steamid"),
        F.coalesce(F.col("r.author.playtime_forever"), F.lit(0)).cast(T.LongType()).alias(
            "playtime_forever"
        ),
        F.coalesce(F.col("r.author.playtime_at_review"), F.lit(0)).cast(T.LongType()).alias(
            "playtime_at_review"
        ),
        F.coalesce(F.col("r.author.num_reviews"), F.lit(0)).cast(T.LongType()).alias("num_reviews"),
        F.col("r.language").cast(T.StringType()).alias("language"),
        F.col("r.review").cast(T.StringType()).alias("review"),
        F.col("r.timestamp_created").cast(T.LongType()).alias("timestamp_created"),
        F.col("r.timestamp_updated").cast(T.LongType()).alias("timestamp_updated"),
        F.col("r.voted_up").cast(T.BooleanType()).alias("voted_up"),
        F.coalesce(F.col("r.votes_up"), F.lit(0)).cast(T.LongType()).alias("votes_up"),
        F.coalesce(F.col("r.votes_funny"), F.lit(0)).cast(T.LongType()).alias("votes_funny"),
        F.col("r.weighted_vote_score").cast(T.DoubleType()).alias("weighted_vote_score"),
        F.coalesce(F.col("r.comment_count"), F.lit(0)).cast(T.LongType()).alias("comment_count"),
        F.col("r.steam_purchase").cast(T.BooleanType()).alias("steam_purchase"),
        F.col("r.received_for_free").cast(T.BooleanType()).alias("received_for_free"),
        F.col("r.written_during_early_access").cast(T.BooleanType()).alias(
            "written_during_early_access"
        ),
        F.col("appid"),
        # Cast dt to DATE so the column type matches the BQ stg_reviews schema.
        # We write to an explicit dt= path (not Spark partitionBy) so `dt` is
        # stored as a real column inside the Parquet files — bq load reads it
        # directly without needing hive-partitioning mode.
        F.to_date(F.lit(dt)).alias("dt"),
    )


# ---------------------------------------------------------------------------
# Step 3 — Deduplicate
# ---------------------------------------------------------------------------


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the most recently updated row per recommendationid.

    Uses a window function rather than groupBy+join to preserve all columns
    without a self-join.
    """
    window = Window.partitionBy("recommendationid").orderBy(F.desc("timestamp_updated"))
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


# ---------------------------------------------------------------------------
# Step 4 — Filter junk
# ---------------------------------------------------------------------------


def filter_junk(df: DataFrame, language: str = "english") -> DataFrame:
    """Remove empty/whitespace reviews and non-target-language rows.

    Uses regexp_replace to strip ALL Unicode whitespace (spaces, tabs, newlines)
    before checking length — F.trim only strips ASCII spaces.
    """
    non_whitespace = F.length(
        F.regexp_replace(F.coalesce(F.col("review"), F.lit("")), r"\s+", "")
    )
    return df.filter(F.col("language") == language).filter(non_whitespace > 0)


# ---------------------------------------------------------------------------
# Step 6 — Derived features
# ---------------------------------------------------------------------------


def add_derived_features(df: DataFrame) -> DataFrame:
    """Add review_length, playtime_hours, playtime_bucket, helpfulness_ratio."""
    return (
        df.withColumn("review_length", F.length(F.col("review")).cast(T.LongType()))
        .withColumn(
            "playtime_hours",
            (F.col("playtime_at_review") / F.lit(60.0)).cast(T.DoubleType()),
        )
        .withColumn(
            "playtime_bucket",
            F.when(F.col("playtime_hours") < 1, F.lit("< 1hr"))
            .when(F.col("playtime_hours") < 10, F.lit("1-10hr"))
            .when(F.col("playtime_hours") < 100, F.lit("10-100hr"))
            .otherwise(F.lit("100+hr")),
        )
        .withColumn(
            "helpfulness_ratio",
            (
                F.col("votes_up").cast(T.DoubleType())
                / (F.col("votes_up") + F.col("votes_funny") + F.lit(1)).cast(T.DoubleType())
            ).cast(T.DoubleType()),
        )
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def transform(
    spark: SparkSession,
    dt: str,
    input_path: str,
    output_path: str,
    language: str = "english",
) -> dict[str, int]:
    """Run the full bronze → silver pipeline for one dt.

    Returns a stats dict with row counts at each stage so callers can log or
    assert on them.
    """
    raw = read_bronze(spark, input_path, dt)
    df_flat = flatten_reviews(raw, dt)
    df_flat = df_flat.cache()
    n_raw = df_flat.count()
    logger.info("Rows after flatten: %d", n_raw)

    df_dedup = deduplicate(df_flat)
    df_flat.unpersist()
    df_dedup = df_dedup.cache()
    n_dedup = df_dedup.count()
    logger.info("Rows after dedup: %d  (removed %d duplicates)", n_dedup, n_raw - n_dedup)

    df_filtered = filter_junk(df_dedup, language=language)
    df_dedup.unpersist()
    df_filtered = df_filtered.cache()
    n_filtered = df_filtered.count()
    logger.info(
        "Rows after junk filter: %d  (removed %d)", n_filtered, n_dedup - n_filtered
    )

    df_enriched = add_derived_features(df_filtered)
    df_enriched = add_sentiment_columns(df_enriched)
    df_filtered.unpersist()

    # Write to explicit dt= path WITHOUT Spark's partitionBy so `dt` is
    # included as a DATE column inside the Parquet files.  This lets
    # `bq load` populate the BQ time-partition field without needing
    # --hive_partitioning_mode.  The gs://bucket path convention is
    # preserved for both local and GCS paths.
    out = f"{output_path}/reviews/dt={dt}/"
    logger.info("Writing silver Parquet to %s", out)
    df_enriched.write.mode("overwrite").parquet(out)
    logger.info("Done. dt=%s written to %s", dt, out)

    return {"n_raw": n_raw, "n_dedup": n_dedup, "n_filtered": n_filtered}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bronze → Silver PySpark transform")
    p.add_argument("--dt", required=True, help="Partition date YYYY-MM-DD")
    p.add_argument(
        "--input",
        required=True,
        help="Bronze base path — local (./data/bronze) or GCS (gs://bucket/bronze)",
    )
    p.add_argument(
        "--output",
        required=True,
        help="Silver base path — local (./data/silver) or GCS (gs://bucket/silver)",
    )
    p.add_argument("--language", default="english", help="Review language filter (default: english)")
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
        SparkSession.builder.appName("steam-transform-silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        stats = transform(spark, args.dt, args.input, args.output, args.language)
        print(
            f"\n{'='*60}\n"
            f"  dt={args.dt}  raw={stats['n_raw']}  "
            f"after_dedup={stats['n_dedup']}  "
            f"after_filter={stats['n_filtered']}\n"
            f"{'='*60}\n"
        )
    finally:
        spark.stop()
