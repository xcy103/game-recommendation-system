"""
Unit tests for the bronze → silver PySpark transform.

All tests use a local[1] SparkSession — no cluster needed, no real I/O.
Fixture data is created in-memory so tests are hermetic.

Coverage:
  - Schema: required columns present after full enrichment pipeline
  - Dedup: keep latest timestamp_updated per recommendationid
  - Junk filter: empty reviews and wrong-language reviews removed
  - Derived features: review_length, playtime_hours, playtime_bucket, helpfulness_ratio
  - Sentiment: sentiment_score and sentiment_label columns added with correct values
  - Build matrix: user_sk, appid, implicit_rating columns; rating in [0, 1]
"""

from __future__ import annotations

import pathlib
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from spark.build_matrix import build_user_item_matrix
from spark.sentiment import add_sentiment_columns
from spark.transform_silver import (
    add_derived_features,
    deduplicate,
    filter_junk,
)

# ---------------------------------------------------------------------------
# Session-scoped SparkSession (one JVM for all tests — ~5 s startup amortised)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-silver-transform")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Shared schema and row factory
# ---------------------------------------------------------------------------

_SCHEMA = T.StructType(
    [
        T.StructField("recommendationid", T.StringType(), False),
        T.StructField("steamid", T.StringType(), True),
        T.StructField("playtime_forever", T.LongType(), True),
        T.StructField("playtime_at_review", T.LongType(), True),
        T.StructField("num_reviews", T.LongType(), True),
        T.StructField("language", T.StringType(), True),
        T.StructField("review", T.StringType(), True),
        T.StructField("timestamp_created", T.LongType(), True),
        T.StructField("timestamp_updated", T.LongType(), True),
        T.StructField("voted_up", T.BooleanType(), True),
        T.StructField("votes_up", T.LongType(), True),
        T.StructField("votes_funny", T.LongType(), True),
        T.StructField("weighted_vote_score", T.DoubleType(), True),
        T.StructField("comment_count", T.LongType(), True),
        T.StructField("steam_purchase", T.BooleanType(), True),
        T.StructField("received_for_free", T.BooleanType(), True),
        T.StructField("written_during_early_access", T.BooleanType(), True),
        T.StructField("appid", T.IntegerType(), True),
        T.StructField("dt", T.StringType(), True),
    ]
)


def _row(
    recommendationid: str = "r1",
    steamid: str = "user1",
    playtime_forever: int = 1000,
    playtime_at_review: int = 500,
    num_reviews: int = 1,
    language: str = "english",
    review: str = "Good game",
    timestamp_created: int = 1_000_000,
    timestamp_updated: int = 1_000_000,
    voted_up: bool = True,
    votes_up: int = 10,
    votes_funny: int = 1,
    weighted_vote_score: float = 0.8,
    comment_count: int = 0,
    steam_purchase: bool = True,
    received_for_free: bool = False,
    written_during_early_access: bool = False,
    appid: int = 730,
    dt: str = "2024-01-01",
) -> tuple:
    return (
        recommendationid,
        steamid,
        playtime_forever,
        playtime_at_review,
        num_reviews,
        language,
        review,
        timestamp_created,
        timestamp_updated,
        voted_up,
        votes_up,
        votes_funny,
        weighted_vote_score,
        comment_count,
        steam_purchase,
        received_for_free,
        written_during_early_access,
        appid,
        dt,
    )


# ---------------------------------------------------------------------------
# 1. Deduplication
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestDedup:
    def test_keeps_latest_when_duplicate(self, spark):
        """Two rows with the same recommendationid — keep the newer one."""
        data = [
            _row("r1", timestamp_updated=100, review="Old review"),
            _row("r1", timestamp_updated=200, review="Updated review"),  # newer
            _row("r2", timestamp_updated=150, review="Unrelated"),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = deduplicate(df)

        assert result.count() == 2
        r1_row = result.filter(F.col("recommendationid") == "r1").first()
        assert r1_row.review == "Updated review"
        assert r1_row.timestamp_updated == 200

    def test_no_duplicates_unchanged(self, spark):
        data = [_row("r1"), _row("r2"), _row("r3")]
        df = spark.createDataFrame(data, _SCHEMA)
        assert deduplicate(df).count() == 3

    def test_three_way_duplicate_keeps_latest(self, spark):
        """Three versions of the same review — only the newest survives."""
        data = [
            _row("dup", timestamp_updated=50, review="v1"),
            _row("dup", timestamp_updated=300, review="v3"),
            _row("dup", timestamp_updated=100, review="v2"),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = deduplicate(df)
        assert result.count() == 1
        assert result.first().review == "v3"
        assert result.first().timestamp_updated == 300


# ---------------------------------------------------------------------------
# 2. Junk filter
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestFilterJunk:
    def test_removes_empty_review(self, spark):
        data = [
            _row("r1", review="Good game"),
            _row("r2", review=""),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = filter_junk(df)
        assert result.count() == 1
        assert result.first().recommendationid == "r1"

    def test_removes_whitespace_only_review(self, spark):
        data = [
            _row("r1", review="Real review"),
            _row("r2", review="   "),
            _row("r3", review="\t\n"),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        assert filter_junk(df).count() == 1

    def test_removes_non_target_language(self, spark):
        data = [
            _row("r1", language="english", review="Good"),
            _row("r2", language="russian", review="Хорошо"),
            _row("r3", language="schinese", review="好游戏"),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = filter_junk(df, language="english")
        assert result.count() == 1
        assert result.first().recommendationid == "r1"

    def test_combined_empty_and_language_filter(self, spark):
        """Fixture_004 equivalent: English but empty review — must be removed."""
        data = [
            _row("good", language="english", review="Worth it"),
            _row("empty_en", language="english", review=""),
            _row("foreign", language="russian", review="Отличная игра"),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = filter_junk(df, language="english")
        assert result.count() == 1
        assert result.first().recommendationid == "good"


# ---------------------------------------------------------------------------
# 3. Derived features
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestDerivedFeatures:
    def test_review_length(self, spark):
        data = [_row("r1", review="Hello")]  # 5 chars
        df = spark.createDataFrame(data, _SCHEMA)
        result = add_derived_features(df).first()
        assert result.review_length == 5

    def test_playtime_buckets(self, spark):
        data = [
            _row("lt1hr", playtime_at_review=30),  # 0.5 h  → "< 1hr"
            _row("1to10", playtime_at_review=300),  # 5 h    → "1-10hr"
            _row("10to100", playtime_at_review=3600),  # 60 h   → "10-100hr"
            _row("100plus", playtime_at_review=36000),  # 600 h  → "100+hr"
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = add_derived_features(df)
        buckets = {r.recommendationid: r.playtime_bucket for r in result.collect()}
        assert buckets["lt1hr"] == "< 1hr"
        assert buckets["1to10"] == "1-10hr"
        assert buckets["10to100"] == "10-100hr"
        assert buckets["100plus"] == "100+hr"

    def test_helpfulness_ratio_range(self, spark):
        data = [
            _row("r1", votes_up=100, votes_funny=0),  # ~1.0 / 101 → 0.99
            _row("r2", votes_up=0, votes_funny=0),  # 0 / 1 = 0.0
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = add_derived_features(df)
        for row in result.collect():
            assert 0.0 <= row.helpfulness_ratio <= 1.0

    def test_playtime_hours_correct(self, spark):
        data = [_row("r1", playtime_at_review=120)]  # 2 hours
        df = spark.createDataFrame(data, _SCHEMA)
        result = add_derived_features(df).first()
        assert abs(result.playtime_hours - 2.0) < 1e-6

    def test_required_derived_columns_present(self, spark):
        df = spark.createDataFrame([_row()], _SCHEMA)
        result = add_derived_features(df)
        for col in ("review_length", "playtime_hours", "playtime_bucket", "helpfulness_ratio"):
            assert col in result.columns, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# 4. Sentiment
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestSentiment:
    def test_columns_added(self, spark):
        df = spark.createDataFrame([_row("r1", review="Nice game")], _SCHEMA)
        result = add_sentiment_columns(df)
        assert "sentiment_score" in result.columns
        assert "sentiment_label" in result.columns

    def test_positive_review_scores_pos(self, spark):
        df = spark.createDataFrame(
            [_row("r1", review="Excellent, amazing, fantastic, wonderful game!")], _SCHEMA
        )
        row = add_sentiment_columns(df).first()
        assert row.sentiment_label == "pos"
        assert row.sentiment_score >= 0.05

    def test_negative_review_scores_neg(self, spark):
        df = spark.createDataFrame(
            [_row("r1", review="Terrible, awful, horrible, broken, worst game ever!")], _SCHEMA
        )
        row = add_sentiment_columns(df).first()
        assert row.sentiment_label == "neg"
        assert row.sentiment_score <= -0.05

    def test_score_in_valid_range(self, spark):
        data = [
            _row("r1", review="Great experience!"),
            _row("r2", review="I have played this."),  # neutral
            _row("r3", review="Worst game ever!"),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        for row in add_sentiment_columns(df).collect():
            assert -1.0 <= row.sentiment_score <= 1.0
            assert row.sentiment_label in ("pos", "neu", "neg")

    def test_empty_review_does_not_crash(self, spark):
        """Sentiment UDF must handle empty strings without raising."""
        df = spark.createDataFrame([_row("r1", review="")], _SCHEMA)
        row = add_sentiment_columns(df).first()
        assert row.sentiment_score is not None
        assert row.sentiment_label in ("pos", "neu", "neg")


# ---------------------------------------------------------------------------
# 5. Full-pipeline schema check
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestFullPipelineSchema:
    def test_all_required_columns_present_after_full_enrichment(self, spark):
        df = spark.createDataFrame([_row()], _SCHEMA)
        df = add_derived_features(df)
        df = add_sentiment_columns(df)
        required = {
            "recommendationid",
            "steamid",
            "language",
            "review",
            "timestamp_created",
            "timestamp_updated",
            "voted_up",
            "votes_up",
            "votes_funny",
            "comment_count",
            "appid",
            "dt",
            "review_length",
            "playtime_hours",
            "playtime_bucket",
            "helpfulness_ratio",
            "sentiment_score",
            "sentiment_label",
        }
        missing = required - set(df.columns)
        assert not missing, f"Missing columns after full enrichment: {missing}"

    def test_sentiment_score_type_is_double(self, spark):
        df = spark.createDataFrame([_row()], _SCHEMA)
        df = add_sentiment_columns(df)
        field = next(f for f in df.schema.fields if f.name == "sentiment_score")
        assert isinstance(field.dataType, T.DoubleType)

    def test_review_length_type_is_long(self, spark):
        df = spark.createDataFrame([_row()], _SCHEMA)
        df = add_derived_features(df)
        field = next(f for f in df.schema.fields if f.name == "review_length")
        assert isinstance(field.dataType, T.LongType)


# ---------------------------------------------------------------------------
# 6. User-item matrix
# ---------------------------------------------------------------------------


@pytest.mark.slow
class TestBuildMatrix:
    def test_output_columns(self, spark):
        df = spark.createDataFrame([_row("r1", steamid="u1", appid=730)], _SCHEMA)
        result = build_user_item_matrix(df)
        assert "user_sk" in result.columns
        assert "appid" in result.columns
        assert "implicit_rating" in result.columns

    def test_implicit_rating_in_unit_interval(self, spark):
        data = [
            _row("r1", steamid="u1", voted_up=True, playtime_at_review=36000),  # 600 h
            _row("r2", steamid="u2", voted_up=False, playtime_at_review=0),
            _row("r3", steamid="u3", voted_up=True, playtime_at_review=60),  # 1 h
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        for row in build_user_item_matrix(df).collect():
            assert (
                0.0 <= row.implicit_rating <= 1.0
            ), f"implicit_rating={row.implicit_rating} out of [0, 1] for user {row.user_sk}"

    def test_voted_up_true_higher_than_false(self, spark):
        """A thumbs-up review should produce a higher rating than thumbs-down
        when both have the same (zero) playtime."""
        data = [
            _row("r1", steamid="u1", appid=730, voted_up=True, playtime_at_review=0),
            _row("r2", steamid="u2", appid=730, voted_up=False, playtime_at_review=0),
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = {r.user_sk: r.implicit_rating for r in build_user_item_matrix(df).collect()}
        assert result["u1"] > result["u2"]

    def test_deduplicates_user_appid_pairs(self, spark):
        """Multiple reviews of the same game by the same user → single row."""
        data = [
            _row("r1", steamid="u1", appid=730, dt="2024-01-01"),
            _row("r2", steamid="u1", appid=730, dt="2024-01-01"),  # same user+game
        ]
        df = spark.createDataFrame(data, _SCHEMA)
        result = build_user_item_matrix(df)
        assert result.count() == 1

    def test_steamid_maps_to_user_sk(self, spark):
        df = spark.createDataFrame([_row("r1", steamid="76561198000000001")], _SCHEMA)
        row = build_user_item_matrix(df).first()
        assert row.user_sk == "76561198000000001"
