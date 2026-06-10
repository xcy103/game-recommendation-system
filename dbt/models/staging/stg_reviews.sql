-- Light cleanup and renaming on the Spark-loaded stg_reviews source.
-- Drops rows with null keys; field names are kept 1:1 with silver so downstream
-- marts don't need to know about the source naming.
select
    recommendationid                                          as review_id,
    appid,
    steamid,
    coalesce(playtime_forever, 0)                            as playtime_forever,
    coalesce(playtime_at_review, 0)                          as playtime_at_review,
    coalesce(num_reviews, 0)                                 as num_reviews,
    language,
    review,
    timestamp_created,
    timestamp_updated,
    voted_up,
    coalesce(votes_up, 0)                                    as votes_up,
    coalesce(votes_funny, 0)                                 as votes_funny,
    weighted_vote_score,
    coalesce(comment_count, 0)                               as comment_count,
    steam_purchase,
    received_for_free,
    written_during_early_access,
    sentiment_score,
    sentiment_label,
    coalesce(review_length, 0)                               as review_length,
    playtime_hours,
    playtime_bucket,
    helpfulness_ratio,
    dt
from {{ source('staging', 'stg_reviews') }}
where
    recommendationid is not null
    and appid is not null
