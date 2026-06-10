# Steam Reviews Data Platform

A production-shaped batch data engineering platform on GCP that ingests millions
of Steam game reviews, processes them through a medallion pipeline, and serves a
game recommendation demo as a downstream consumer.

Built to demonstrate end-to-end data engineering: Python ingestion, PySpark
transforms, dbt modeling, BigQuery star schema, Airflow orchestration,
Terraform IaC, and GitHub Actions CI/CD.

---

## Architecture

```
                      ┌──────────────────────────────────────────┐
                      │        Cloud Composer (Airflow DAG)       │
                      └───────────────┬──────────────────────────┘
                                      │ schedules & monitors
┌──────────┐  raw JSON  ┌─────────────▼──────────┐  Parquet  ┌──────────────┐
│  Steam   │───────────▶│ GCS — bronze (landing)  │──────────▶│  Dataproc /  │
│  Web API │            │ dt=YYYY-MM-DD/appid=NNN/ │           │  PySpark     │
└──────────┘            └─────────────────────────┘           └──────┬───────┘
                                                                      │ cleaned
                                                                      ▼
                                                        ┌─────────────────────┐
                                                        │ GCS — silver        │
                                                        │ (Parquet, dt=...)   │
                                                        └──────────┬──────────┘
                                                                   │ bq load
                                                                   ▼
                                                        ┌─────────────────────┐
                                                        │ BigQuery — staging  │
                                                        │ stg_reviews         │
                                                        └──────────┬──────────┘
                                                                   │ dbt
                                                                   ▼
                                                        ┌─────────────────────┐
                                                        │ BigQuery — gold     │
                                                        │ dim_games           │
                                                        │ dim_users           │
                                                        │ fact_reviews ①②    │
                                                        └────┬───────────┬────┘
                                                             │           │
                                               analytics /  │           │ ALS matrix
                                               BI queries ◀─┘           ▼
                                                                ┌───────────────┐
                                                                │  Spark ALS    │
                                                                │ recommender   │
                                                                └──────┬────────┘
                                                                       │
                                                                       ▼
                                                              game_recommendations
                                                                 → FastAPI / Cloud Run
```

① `fact_reviews` partitioned by `dt`  
② `fact_reviews` clustered by `game_sk` — cuts BQ scan cost dramatically

**Infrastructure:** all GCP resources provisioned by Terraform.  
**CI/CD:** GitHub Actions — lint + unit tests on PR; build + deploy on merge to main.

---

## Repository Layout

```
.
├── ingestion/                  # Steam API ingester
│   ├── ingester.py             # rate-limited, cursor-paginated, resumable
│   ├── rate_limiter.py         # token bucket + jitter
│   ├── checkpoint.py           # cursor state to/from GCS
│   ├── appids.py               # SteamSpy top-N fetch / static list
│   └── tests/
├── spark/                      # PySpark jobs
│   ├── transform_silver.py     # bronze JSON → silver Parquet
│   ├── sentiment.py            # VADER sentiment UDF
│   ├── build_matrix.py         # user-item interaction matrix
│   ├── recommender.py          # ALS training → game_recommendations
│   └── tests/
├── dbt/                        # BigQuery gold star schema
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/            # stg_reviews
│       └── marts/              # dim_games, dim_users, fact_reviews
├── airflow/                    # Orchestration DAG
│   └── dags/steam_reviews_pipeline.py
├── api/                        # FastAPI recommender demo
│   ├── main.py
│   └── Dockerfile
├── terraform/                  # all GCP resources (Terraform applied)
│   ├── main.tf
│   ├── variables.tf
│   ├── backend.tf
│   ├── terraform.tfvars.example
│   └── modules/
│       ├── storage/            # bronze + silver GCS buckets
│       ├── bigquery/           # staging + gold datasets, key tables
│       ├── iam/                # least-privilege service accounts
│       ├── composer/           # Cloud Composer (gated, default off)
│       └── cloudrun/           # Cloud Run API (gated, default off)
├── docker/
│   └── ingester.Dockerfile
├── scripts/
│   └── local_run.sh            # run pipeline locally on fixture data
├── tests/
│   └── fixtures/
│       └── sample_api_response.json  # 5 fake reviews — no real API needed
├── .github/workflows/
│   ├── ci.yml                  # lint + unit tests + terraform validate
│   └── deploy.yml              # build images + terraform plan/apply
├── pyproject.toml
└── DESIGN.md                   # single source of truth — read this first
```

---

## Local Development

### Prerequisites

- Python 3.11+
- Java 11+ (for local PySpark)
- [Terraform 1.5+](https://developer.hashicorp.com/terraform/install)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)

### Setup

```bash
# Clone and install Python deps
git clone <repo>
cd steam-reviews-platform
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Authenticate with GCP (uses Application Default Credentials)
gcloud auth application-default login
gcloud config set project steam-reviews-platform
```

### Run tests (no GCP needed)

```bash
# Unit tests only — uses fixture data, no real API or cloud calls
pytest -m "not slow and not integration"

# With coverage
pytest -m "not slow and not integration" --cov=ingestion --cov=spark
```

### Lint

```bash
ruff check .
black --check .
```

### Full local pipeline (scaffold)

```bash
./scripts/local_run.sh --dt 2024-01-01
```

### Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars  # fill in your values
terraform init
terraform plan     # always review before apply
```

---

## GCP Resources

| Resource | Name |
|----------|------|
| Bronze bucket | `steam-reviews-platform-bronze-dev` |
| Silver bucket | `steam-reviews-platform-silver-dev` |
| BQ staging dataset | `steam_staging_dev` |
| BQ gold dataset | `steam_gold_dev` |
| Ingester SA | `steam-ingester-dev@steam-reviews-platform.iam.gserviceaccount.com` |
| Spark SA | `steam-spark-dev@steam-reviews-platform.iam.gserviceaccount.com` |
| dbt SA | `steam-dbt-dev@steam-reviews-platform.iam.gserviceaccount.com` |
| Composer SA | `steam-composer-dev@steam-reviews-platform.iam.gserviceaccount.com` |
| API SA | `steam-api-dev@steam-reviews-platform.iam.gserviceaccount.com` |

Remote Terraform state: `gs://steam-reviews-platform-tfstate/terraform/state`

> **Cost note (DESIGN §15):** Composer and Cloud Run are off by default.
> Run `terraform destroy` between work sessions to avoid idle charges.
> Set a GCP budget alert in `terraform.tfvars` (`billing_account_id`).
