# Teardown & Cost Guide

This document covers what costs money, how much, and how to stop all spending.

---

## Cost overview

| Resource | Cost model | Est. monthly | On by default |
|----------|-----------|--------------|---------------|
| BigQuery storage | $0.02/GB · ~30 MB stored | < $0.01 | Yes (after apply) |
| BigQuery queries | $5/TB scanned · tables are partitioned | < $0.05 | Yes (after apply) |
| GCS (bronze + silver + staging) | $0.02/GB · ~500 MB | < $0.01 | Yes (after apply) |
| Dataproc Serverless | ~$0.05/vCPU-hr · ~5 min per run | ~$0.50/run | Gated (`enable_dataproc`) |
| Artifact Registry | $0.10/GB/month · one image | < $0.01 | After WIF apply |
| Cloud Composer | ~$300+/month (always-on) | $300+ | **Off** (`enable_composer=false`) |
| Cloud Run | Per-request, scales to zero | ~$0 | **Off** (`enable_cloudrun=false`) |

**Normal dev spend** (Composer off, Dataproc gated): well under $1/day.  
**Budget alert** fires at 50% / 80% / 100% of `monthly_budget_usd` (default $50).

---

## Local teardown (Airflow Docker)

```bash
# Stop Airflow containers and remove volumes
docker compose -f docker/docker-compose-airflow.yml down -v

# Remove the built images (optional)
docker rmi apache/airflow:2.8.4-python3.11 postgres:16
```

No cloud costs. All data is local.

---

## Cloud teardown

### Step 1 — disable expensive resources first

In `terraform/terraform.tfvars`, set all gates to false:

```hcl
enable_dataproc = false
enable_composer = false
enable_cloudrun = false
```

Then apply:

```bash
cd terraform
terraform apply -var-file=terraform.tfvars
```

This stops Cloud Composer (~$300/month) and any Dataproc clusters without touching storage or IAM.

### Step 2 — verify no running jobs

```bash
# Check for running Dataproc batches
gcloud dataproc batches list --region=us-central1 --project=steam-reviews-platform

# Check Composer is gone (if it was enabled)
gcloud composer environments list --locations=us-central1 --project=steam-reviews-platform
```

### Step 3 — full destroy

```bash
cd terraform

# Preview what will be deleted
terraform plan -destroy

# Destroy everything (prompts for confirmation)
terraform destroy
```

`delete_contents_on_destroy = true` is set for dev datasets, so BigQuery tables are dropped automatically.

### Step 4 — verify $0 spend

```bash
# No buckets
gcloud storage buckets list --project=steam-reviews-platform

# No BQ datasets
bq ls --project_id=steam-reviews-platform

# No service accounts (optional — SAs don't cost money)
gcloud iam service-accounts list --project=steam-reviews-platform
```

---

## Emergency stop (fastest path to zero)

If the budget alert fires, fastest option:

```bash
# 1. Kill Composer immediately (if running)
gcloud composer environments delete steam-composer-dev \
  --location=us-central1 --project=steam-reviews-platform

# 2. Cancel all running Dataproc batches
gcloud dataproc batches cancel --all --region=us-central1 --project=steam-reviews-platform 2>/dev/null || true

# 3. Full terraform destroy
cd terraform && terraform destroy -auto-approve
```

Or from GCP Console: **Billing → Budget alerts → Disable billing** (nuclear option — disables entire project).

---

## Cost guardrails in the codebase

| Guardrail | Where |
|-----------|-------|
| `fact_reviews` partitioned by `dt`, clustered by `game_sk` | `dbt/models/marts/fact_reviews.sql` |
| `game_recommendations` has `require_partition_filter=true` | `terraform/modules/bigquery/main.tf` |
| Dataproc Serverless — billed per vCPU-second, no idle cluster | `terraform/modules/dataproc/main.tf` |
| Composer off by default | `terraform/variables.tf` (`enable_composer=false`) |
| GCS bronze lifecycle: NEARLINE at 30 days, delete at 90 days | `terraform/modules/storage/main.tf` |
| Budget alert at 50% / 80% / 100% of limit | `terraform/main.tf` |

**Never do** `SELECT *` on `fact_reviews` or `stg_reviews` without a `WHERE dt = '...'` filter — they scan the full history.

---

## Re-creating from scratch

If you need to start over:

```bash
# 1. Destroy everything
cd terraform && terraform destroy

# 2. Delete tfstate (if stored locally)
rm -f terraform/terraform.tfstate terraform/terraform.tfstate.backup

# 3. Re-apply from scratch
terraform apply
```

The pipeline is fully reproducible from the Terraform + dbt + Spark code.
