-- Dimension: one row per reviewer seen in the data.
-- user_sk is a deterministic INT64 surrogate key via FARM_FINGERPRINT.
-- steamid is kept as-is (anonymisation / hashing not yet implemented).
with user_stats as (
    select
        steamid,
        max(num_reviews)                                    as num_reviews,
        min(
            timestamp_seconds(timestamp_created)
        )                                                   as first_review_ts
    from {{ ref('stg_reviews') }}
    where steamid is not null
    group by steamid
)
select
    farm_fingerprint(steamid)                               as user_sk,
    steamid,
    num_reviews,
    first_review_ts
from user_stats
