# Steam Reviews Data Platform — Design Document

A batch-first data engineering platform on GCP that ingests large volumes of Steam
game reviews, processes them through a medallion (bronze/silver/gold) pipeline using
PySpark and dbt, lands modeled data in BigQuery, and serves a lightweight game
recommendation demo as a downstream consumer.

This document is the single source of truth for the project. It is written so an
engineer can implement the system without further architectural decisions.

---

## 1. Goals & Non-Goals

### Goals
- Build a **production-shaped batch data pipeline** end to end on GCP.
- Demonstrate core data engineering competencies: ingestion with rate-limiting and
  resumability, distributed transformation (PySpark), warehouse modeling
  (BigQuery star schema), SQL transformation framework (dbt), orchestration
  (Airflow), infrastructure as code (Terraform), and CI/CD (GitHub Actions).
- Produce a **recommendation demo** as proof the data is usable, not as the project's
  centerpiece.
- Be **cost-conscious**: ephemeral compute, partitioned/clustered tables, teardown.

### Non-Goals
- Real-time / streaming ingestion (explicitly deferred; architecture leaves room for it).
- A polished consumer-facing product or sophisticated ML. The recommender is a demo.
- Multi-cloud. This is GCP-native by design; portability is not a goal.

---

## 2. High-Level Architecture

```
                          ┌──────────────────────────────────────────┐
                          │              Orchestration                │
                          │        Cloud Composer (Airflow DAG)       │
                          └───────────────┬──────────────────────────┘
                                          │ schedules & monitors
   ┌──────────┐   raw JSON   ┌────────────▼───────────┐   Parquet   ┌──────────────┐
   │ Steam    │─────────────▶│  GCS — bronze (landing)│────────────▶│ Dataproc /   │
   │ Web API  │  (Python      │  dt=YYYY-MM-DD/        │             │ PySpark      │
   │          │   ingester)   │  appid=NNN/            │             │ (transform)  │
   └──────────┘               └────────────────────────┘             └──────┬───────┘
                                                                            │ cleaned
                                                                            ▼
                                                              ┌──────────────────────┐
                                                              │ GCS — silver (Parquet)│
                                                              └───────────┬───────────┘
                                                                          │ load
                                                                          ▼
                                                              ┌──────────────────────┐
                                                              │ BigQuery — staging    │
                                                              └───────────┬───────────┘
                                                                          │ dbt (SQL)
                                                                          ▼
                                                              ┌──────────────────────┐
                                                              │ BigQuery — gold       │
                                                              │ star schema (marts)   │
                                                              └─────┬──────────┬──────┘
                                                                    │          │
                                                       analytics    │          │ user-item matrix
                                                       queries/BI ◀──┘          ▼
                                                                        ┌───────────────┐
                                                                        │ Recommender   │
                                                                        │ (Spark ALS)   │
                                                                        │ → BQ rec table│
                                                                        │ → FastAPI demo│
                                                                        └───────────────┘

   Cross-cutting: Terraform (all GCP resources) · GitHub Actions (test + deploy)
```

### ETL vs ELT in this project
This pipeline is a deliberate **hybrid**:
- **Heavy, non-SQL-friendly transforms** (JSON flattening, dedup, sentiment scoring,
  text features, building the user-item interaction matrix) run in **PySpark** —
  classic **ETL** (transform before final load).
- **Aggregation and dimensional modeling** run **inside BigQuery via dbt** —
  modern **ELT** (load raw-ish, transform in-warehouse with SQL).

Rationale to state out loud: put each transform where it is cheapest and clearest —
text/ML work in Spark, set-based modeling in the warehouse — rather than forcing
everything through one engine.

---

## 3. Data Source: Steam Reviews API

- **Endpoint:** `https://store.steampowered.com/appreviews/{appid}?json=1`
- **No API key required** for the review endpoint.
- **Pagination:** cursor-based. Each response returns a `cursor` value; pass it as the
  `cursor` param to fetch the next page. Track seen cursors; stop when a cursor repeats
  or `num_reviews == 0`.
- **Page size:** `num_per_page` up to 100.
- **Key params:** `filter=recent` (stable pagination — important: `filter=all` returns
  only a limited window), `language=english` (or `all`), `review_type=all`,
  `purchase_type=all`, `day_range`, `num_per_page=100`.
- **Rate limit reality:** ~10 reviews/sec sustainable; aggressive scraping triggers
  HTTP 429 and possible temporary shadow-bans. Design the ingester to be polite:
  conservative request spacing, jitter, exponential backoff on 429/5xx.
- **App IDs:** obtain a target list (top N popular games) from SteamSpy
  (`https://steamspy.com/api.php?request=top100in2weeks`) or a static curated list.
  Start with ~500–1000 appids to reach review counts in the low millions.

### Raw review record (fields of interest)
```
recommendationid        (string, unique review id)
author.steamid          (string)
author.playtime_forever (int, minutes)
author.playtime_at_review (int, minutes)
author.num_reviews      (int)
language                (string)
review                  (string, free text)
timestamp_created       (int, unix)
timestamp_updated       (int, unix)
voted_up                (bool, the recommend/not-recommend signal)
votes_up                (int, helpfulness)
votes_funny             (int)
weighted_vote_score     (float)
comment_count           (int)
steam_purchase          (bool)
received_for_free       (bool)
written_during_early_access (bool)
```
Plus per-app `query_summary`: `review_score`, `total_positive`, `total_negative`,
`total_reviews`.

---

## 4. Medallion Layers & Storage

### Bronze (GCS, raw)
- Path: `gs://<bucket>/bronze/reviews/dt=YYYY-MM-DD/appid=<appid>/part-*.json`
- Raw API JSON, append-only, never mutated. Source of truth for replay.
- One ingestion run = one `dt` partition.

### Silver (GCS, cleaned Parquet)
- Path: `gs://<bucket>/silver/reviews/dt=YYYY-MM-DD/`
- Produced by PySpark. Schema-enforced, deduplicated, typed, enriched with sentiment
  and derived features. Columnar Parquet, partitioned by `dt`.

### Gold (BigQuery, modeled)
- dbt-built marts in a star schema. This is what analytics and the recommender read.

---

## 5. BigQuery Data Model (Gold — Star Schema)

### Staging (loaded from silver Parquet, `stg_` prefix)
- `stg_reviews` — 1:1 with silver rows, lightly typed.

### Dimensions
**`dim_games`**
| column | type | notes |
|---|---|---|
| game_sk | INT64 | surrogate key |
| appid | INT64 | natural key |
| name | STRING | |
| total_reviews | INT64 | from query_summary |
| total_positive | INT64 | |
| positive_rate | FLOAT64 | computed |
| review_score_desc | STRING | |
| first_seen_dt | DATE | |
| last_seen_dt | DATE | |

**`dim_users`**
| column | type | notes |
|---|---|---|
| user_sk | INT64 | surrogate key |
| steamid | STRING | natural key (hashed/anonymized) |
| num_reviews | INT64 | |
| first_review_ts | TIMESTAMP | |

### Fact
**`fact_reviews`** (grain: one row per review)
| column | type | notes |
|---|---|---|
| review_id | STRING | recommendationid |
| game_sk | INT64 | FK → dim_games |
| user_sk | INT64 | FK → dim_users |
| dt | DATE | partition key |
| voted_up | BOOL | |
| playtime_at_review_min | INT64 | |
| votes_up | INT64 | |
| weighted_vote_score | FLOAT64 | |
| sentiment_score | FLOAT64 | from PySpark |
| sentiment_label | STRING | pos/neu/neg |
| review_length | INT64 | |
| created_ts | TIMESTAMP | |

### Physical optimization (must implement and be able to explain)
- `fact_reviews`: **partition by `dt`**, **cluster by `game_sk`**. Cuts BigQuery scan
  cost dramatically when filtering by date/game.
- Always filter on the partition column in queries.

### Recommendation output
**`game_recommendations`**
| column | type |
|---|---|
| user_sk | INT64 |
| ranked_appid | INT64 |
| rank | INT64 |
| score | FLOAT64 |
| generated_at | TIMESTAMP |

---

## 6. PySpark Transformation (Silver build)

Runs on Dataproc (ephemeral cluster or Dataproc Serverless). Responsibilities:
1. Read bronze JSON for a given `dt`.
2. Flatten nested `author.*` fields; enforce schema and types.
3. **Deduplicate** on `recommendationid` (keep latest `timestamp_updated`).
4. Filter junk: empty/whitespace reviews, non-target languages, bot-like patterns.
5. **Sentiment scoring**: batch score `review` text (VADER for simplicity, or a small
   HuggingFace model via pandas UDF if you want a stronger signal). Output
   `sentiment_score` + `sentiment_label`.
6. Derived features: `review_length`, playtime buckets, helpfulness ratio.
7. Build the **user-item interaction matrix** input (user, appid, implicit rating from
   `voted_up` + playtime) and write it for the recommender.
8. Write silver Parquet partitioned by `dt`.

**Cost note:** prefer Dataproc Serverless or an ephemeral cluster that is created for
the job and deleted on completion. Never leave a cluster running.

---

## 7. dbt (Gold build, in BigQuery)

- dbt project with `staging/`, `marts/` model folders.
- `staging` models: light cleanup/renaming on `stg_reviews`.
- `marts` models: `dim_games`, `dim_users`, `fact_reviews` (incremental, partitioned,
  clustered), plus analytics models (e.g., `mart_game_sentiment_trends`).
- **Tests**: `not_null`, `unique` on keys; `relationships` (FK integrity from
  fact to dims); a couple of custom data-quality tests (e.g., `positive_rate`
  between 0 and 1).
- `dbt docs generate` for lineage documentation (screenshot for the README).

---

## 8. Recommendation Demo

- **Model:** Spark MLlib **ALS** (collaborative filtering) on the implicit-feedback
  user-item matrix. Implicit feedback = `voted_up` weighted by playtime.
- Train on Dataproc, write top-N per user to `game_recommendations` in BigQuery.
- **Serving (optional but nice):** a small **FastAPI** endpoint
  `GET /recommendations/{user_sk}` that queries the BQ table. Containerize and run on
  **Cloud Run** (cheap, scales to zero).
- Keep it lightweight — this validates the pipeline output; it is not the deliverable.

---

## 9. Orchestration (Airflow on Cloud Composer)

Single daily DAG `steam_reviews_pipeline`:
```
ingest_reviews (PythonOperator / KubernetesPodOperator)
        │
        ▼
spark_transform_silver (DataprocCreateBatch / DataprocSubmitJob)
        │
        ▼
load_silver_to_bq (GCSToBigQuery / bq load)
        │
        ▼
dbt_run + dbt_test (BashOperator / dbt Cloud op)
        │
        ▼
train_recommender (Dataproc)
        │
        ▼
publish_recommendations_to_bq
```
- Idempotent tasks keyed by execution date (`dt`). Re-running a date overwrites that
  partition cleanly.
- **Cost note:** Composer runs continuously and is the most expensive component.
  Acceptable alternatives: run Airflow locally in Docker for development, or use
  Cloud Workflows + Cloud Scheduler. If using Composer, tear it down when not
  actively demoing.

---

## 10. Infrastructure as Code (Terraform)

All GCP resources defined in Terraform, organized into modules:
- `modules/storage` — GCS buckets (bronze/silver), lifecycle rules.
- `modules/bigquery` — datasets (staging, gold), table/partition config.
- `modules/dataproc` — serverless batch config / cluster template.
- `modules/composer` — Composer environment (gated behind a variable so it can be
  disabled to save cost).
- `modules/iam` — service accounts and least-privilege roles.
- `modules/cloudrun` — recommender API (optional).
- Remote state in a GCS backend. `dev` and (optional) `prod` workspaces.
- `terraform apply` stands up the whole environment; `terraform destroy` tears it down.

---

## 11. CI/CD (GitHub Actions)

- **On PR:** lint (ruff/black), unit tests for ingester + PySpark transforms
  (using `chispa`/pytest on tiny fixture data), `terraform validate` + `fmt -check`,
  `dbt parse`.
- **On merge to main:** build & push ingester/recommender images to Artifact Registry;
  `terraform plan` (and gated `apply`); deploy DAG to Composer's GCS bucket; optionally
  deploy Cloud Run.
- Use Workload Identity Federation (no long-lived JSON keys) for GitHub→GCP auth.

---

## 12. Repository Layout

```
steam-reviews-platform/
├── README.md
├── DESIGN.md                      # this file
├── ingestion/
│   ├── ingester.py                # rate-limited, resumable Steam API client
│   ├── rate_limiter.py            # token bucket
│   ├── checkpoint.py              # cursor checkpoint to GCS
│   ├── appids.py                  # fetch/curate target appid list
│   └── tests/
├── spark/
│   ├── transform_silver.py        # bronze → silver PySpark job
│   ├── sentiment.py               # sentiment UDF
│   ├── build_matrix.py            # user-item matrix
│   ├── recommender.py             # ALS training job
│   └── tests/
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/
│       └── marts/
├── airflow/
│   └── dags/steam_reviews_pipeline.py
├── api/                           # optional FastAPI recommender service
│   ├── main.py
│   └── Dockerfile
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── backend.tf
│   └── modules/
├── .github/workflows/
│   ├── ci.yml
│   └── deploy.yml
├── docker/
│   └── ingester.Dockerfile
└── scripts/
    └── local_run.sh               # run pipeline end-to-end locally on sample data
```

---

## 13. Cost Guardrails (read before you `apply`)
- BigQuery bills on bytes scanned: always filter the partition column; never
  `SELECT *` on `fact_reviews`.
- Use Dataproc **Serverless** or ephemeral clusters; delete on job completion.
- Composer is the priciest piece — keep it off unless actively demoing; develop with
  local Airflow.
- Set a **GCP budget alert**. Use the $300 free trial credit. `terraform destroy`
  between work sessions.
