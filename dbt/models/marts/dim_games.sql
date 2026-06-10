-- Dimension: one row per Steam game seen in the review data.
-- game_sk is a deterministic INT64 surrogate key via FARM_FINGERPRINT.
-- name and review_score_desc are not available from review data alone;
-- they remain NULL until a SteamSpy enrichment model is added.
with game_stats as (
    select
        appid,
        count(*)                                            as total_reviews,
        countif(voted_up)                                   as total_positive,
        min(dt)                                             as first_seen_dt,
        max(dt)                                             as last_seen_dt
    from {{ ref('stg_reviews') }}
    group by appid
)
select
    farm_fingerprint(cast(appid as string))                 as game_sk,
    appid,
    cast(null as string)                                    as name,
    total_reviews,
    total_positive,
    safe_divide(total_positive, total_reviews)              as positive_rate,
    cast(null as string)                                    as review_score_desc,
    first_seen_dt,
    last_seen_dt
from game_stats
