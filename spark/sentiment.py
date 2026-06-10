"""
Sentiment scoring for Steam review text (DESIGN.md §6, step 5).

Baseline scorer: VADER (rule-based, no GPU, fast at any scale).
Swap-out path: replace _vader_score_udf with a HuggingFace-backed pandas UDF
and keep the same add_sentiment_columns(df) interface — callers don't change.

Exported:
    label_from_score(score) → SentimentLabel        pure, no heavy deps
    score_batch(texts)      → [(score, label), ...]  VADER, for non-Spark use
    add_sentiment_columns(df) → DataFrame            PySpark entry point
"""
from __future__ import annotations

from typing import Literal

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

SentimentLabel = Literal["pos", "neu", "neg"]

# VADER compound score thresholds (industry convention)
_POS_THRESHOLD = 0.05
_NEG_THRESHOLD = -0.05


def label_from_score(score: float) -> SentimentLabel:
    """Map a VADER compound score to a categorical label."""
    if score >= _POS_THRESHOLD:
        return "pos"
    if score <= _NEG_THRESHOLD:
        return "neg"
    return "neu"


def _make_vader_analyzer():
    """Create SentimentIntensityAnalyzer, handling zip-packaged environments.

    When running on Dataproc Serverless, the spark.zip is put on sys.path via
    --py-files. Python can import from the zip, but open() cannot read files
    inside it (zipimport maps __file__ into the archive path which the OS
    rejects as NotADirectoryError).

    vaderSentiment 3.3.x stores lexicon content in self.lexicon_full_filepath
    (a plain string) and parses it with split('\\n') — no file handle is kept.
    So we can bypass __init__'s codecs.open() calls by:
      1. Reading the lexicon bytes with pkgutil.get_data (zip-aware)
      2. Constructing the object without calling __init__
      3. Setting the content attributes directly
    """
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    try:
        return SentimentIntensityAnalyzer()
    except (NotADirectoryError, FileNotFoundError, OSError):
        import pkgutil
        obj = object.__new__(SentimentIntensityAnalyzer)
        lex_bytes = pkgutil.get_data("vaderSentiment", "vader_lexicon.txt")
        obj.lexicon_full_filepath = (lex_bytes or b"").decode("utf-8")
        obj.lexicon = obj.make_lex_dict()
        emoji_bytes = pkgutil.get_data("vaderSentiment", "emoji_utf8_lexicon.txt")
        obj.emoji_full_filepath = (emoji_bytes or b"").decode("utf-8")
        obj.emojis = obj.make_emoji_dict()
        return obj


def score_batch(texts: list[str]) -> list[tuple[float, SentimentLabel]]:
    """Score a list of review texts using VADER.

    Returns (compound_score, label) pairs in the same order as input.
    Lazy-imports vaderSentiment so tests that don't call this function
    don't need it installed.
    """
    analyzer = _make_vader_analyzer()
    result = []
    for t in texts:
        compound = float(analyzer.polarity_scores(t or "")["compound"])
        result.append((compound, label_from_score(compound)))
    return result


# ---------------------------------------------------------------------------
# PySpark pandas UDFs — lazy import of VADER happens inside each UDF body
# so the driver JVM doesn't need it; only executor Python processes do.
# ---------------------------------------------------------------------------


@F.pandas_udf(T.DoubleType())
def _vader_score_udf(texts: pd.Series) -> pd.Series:
    from spark.sentiment import _make_vader_analyzer
    analyzer = _make_vader_analyzer()
    return texts.fillna("").apply(lambda t: float(analyzer.polarity_scores(t)["compound"]))


@F.pandas_udf(T.StringType())
def _label_udf(scores: pd.Series) -> pd.Series:
    return scores.apply(label_from_score)


def add_sentiment_columns(df: DataFrame) -> DataFrame:
    """Add sentiment_score (DOUBLE) and sentiment_label (STRING) to df.

    Uses pandas UDFs so VADER runs in each executor's Python process,
    not on the driver. To swap for a HuggingFace model: replace
    _vader_score_udf with a new pandas UDF and leave this function's
    signature unchanged.
    """
    return (
        df.withColumn("sentiment_score", _vader_score_udf(F.col("review"))).withColumn(
            "sentiment_label", _label_udf(F.col("sentiment_score"))
        )
    )
