locals {
  staging_dataset = "steam_staging_${var.environment}"
  gold_dataset    = "steam_gold_${var.environment}"
}

# ---------------------------------------------------------------------------
# DESIGN §5: Staging dataset — loaded from silver Parquet via bq load / GCSToBQ
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "staging" {
  dataset_id                 = local.staging_dataset
  friendly_name              = "Steam Reviews — Staging (${var.environment})"
  description                = "Raw-ish silver data loaded before dbt transformations. stg_ prefix tables."
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = var.environment != "prod"

  labels = {
    environment = var.environment
    layer       = "staging"
    managed_by  = "terraform"
  }
}

# stg_reviews: 1:1 with silver rows, lightly typed (DESIGN §5)
# This table is the target for `bq load` / GCSToBigQueryOperator in Airflow.
# dbt's staging models read from here.
resource "google_bigquery_table" "stg_reviews" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_reviews"
  project             = var.project_id
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "dt"
  }

  schema = jsonencode([
    { name = "recommendationid", type = "STRING", mode = "NULLABLE", description = "Unique review ID from Steam API" },
    { name = "appid", type = "INT64", mode = "NULLABLE", description = "Steam application ID" },
    { name = "steamid", type = "STRING", mode = "NULLABLE", description = "Author Steam ID (hashed in gold)" },
    { name = "playtime_forever", type = "INT64", mode = "NULLABLE", description = "Total playtime in minutes" },
    { name = "playtime_at_review", type = "INT64", mode = "NULLABLE", description = "Playtime at review time in minutes" },
    { name = "num_reviews", type = "INT64", mode = "NULLABLE", description = "Number of reviews author has written" },
    { name = "language", type = "STRING", mode = "NULLABLE" },
    { name = "review", type = "STRING", mode = "NULLABLE", description = "Free-text review body" },
    { name = "timestamp_created", type = "INT64", mode = "NULLABLE", description = "Unix timestamp of review creation" },
    { name = "timestamp_updated", type = "INT64", mode = "NULLABLE" },
    { name = "voted_up", type = "BOOL", mode = "NULLABLE", description = "True = recommend, False = not recommend" },
    { name = "votes_up", type = "INT64", mode = "NULLABLE", description = "Helpful votes" },
    { name = "votes_funny", type = "INT64", mode = "NULLABLE" },
    { name = "weighted_vote_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "comment_count", type = "INT64", mode = "NULLABLE" },
    { name = "steam_purchase", type = "BOOL", mode = "NULLABLE" },
    { name = "received_for_free", type = "BOOL", mode = "NULLABLE" },
    { name = "written_during_early_access", type = "BOOL", mode = "NULLABLE" },
    # Derived fields added by PySpark silver job (DESIGN §6, steps 5–6)
    { name = "sentiment_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "sentiment_label", type = "STRING", mode = "NULLABLE", description = "pos/neu/neg" },
    { name = "review_length", type = "INT64", mode = "NULLABLE" },
    { name = "playtime_hours", type = "FLOAT64", mode = "NULLABLE", description = "playtime_at_review converted to hours" },
    { name = "playtime_bucket", type = "STRING", mode = "NULLABLE", description = "< 1hr / 1-10hr / 10-100hr / 100+hr" },
    { name = "helpfulness_ratio", type = "FLOAT64", mode = "NULLABLE", description = "votes_up / (votes_up + votes_funny + 1)" },
    { name = "dt", type = "DATE", mode = "NULLABLE", description = "Partition key — ingestion date" }
  ])

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ---------------------------------------------------------------------------
# DESIGN §5: Gold dataset — dbt star schema (dim_games, dim_users, fact_reviews)
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "gold" {
  dataset_id                 = local.gold_dataset
  friendly_name              = "Steam Reviews — Gold / Star Schema (${var.environment})"
  description                = "dbt-built star schema. Query here for analytics, the recommender, and the FastAPI demo."
  location                   = var.region
  project                    = var.project_id
  delete_contents_on_destroy = var.environment != "prod"

  labels = {
    environment = var.environment
    layer       = "gold"
    managed_by  = "terraform"
  }
}

# game_recommendations: written by Spark ALS job (DESIGN §5, §8)
# Pre-created here so the schema/IAM are consistent before the Spark ALS job runs.
resource "google_bigquery_table" "game_recommendations" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "game_recommendations"
  project             = var.project_id
  deletion_protection = var.environment == "prod"

  # Cluster by user_sk for fast user-level lookups by the FastAPI demo
  clustering = ["user_sk"]

  time_partitioning {
    type  = "DAY"
    field = "generated_at"
  }

  schema = jsonencode([
    { name = "user_sk", type = "INT64", mode = "REQUIRED", description = "FK → dim_users.user_sk" },
    { name = "ranked_appid", type = "INT64", mode = "REQUIRED", description = "Recommended Steam app ID" },
    { name = "rank", type = "INT64", mode = "REQUIRED", description = "1-based recommendation rank per user" },
    { name = "score", type = "FLOAT64", mode = "NULLABLE", description = "ALS predicted score" },
    { name = "generated_at", type = "TIMESTAMP", mode = "REQUIRED", description = "Partition key — when this batch was generated" }
  ])

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# NOTE: dim_games, dim_users, fact_reviews are managed by dbt.
# dbt config blocks set: fact_reviews partition_by=dt, cluster_by=[game_sk].
# Do NOT pre-create them here to avoid schema conflicts with dbt's CREATE OR REPLACE.
