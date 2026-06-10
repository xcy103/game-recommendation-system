-- Fact table: one row per review, grain = recommendationid.
-- Partitioned by dt (DATE), clustered by game_sk — DESIGN.md §5.
-- Incremental strategy: insert_overwrite replaces the dt partition on each run,
-- so re-running for the same dt is idempotent.
{{
    config(
        materialized        = 'incremental',
        partition_by        = {'field': 'dt', 'data_type': 'date'},
        cluster_by          = ['game_sk'],
        incremental_strategy= 'insert_overwrite',
        on_schema_change    = 'fail'
    )
}}

with source as (
    select * from {{ ref('stg_reviews') }}

    -- On incremental runs, only reprocess the latest (and any newer) partitions.
    {% if is_incremental() %}
    where dt >= (select max(dt) from {{ this }})
    {% endif %}
)
select
    s.review_id,
    farm_fingerprint(cast(s.appid as string))               as game_sk,
    if(
        s.steamid is not null,
        farm_fingerprint(s.steamid),
        cast(null as int64)
    )                                                        as user_sk,
    s.dt,
    s.voted_up,
    s.playtime_at_review                                     as playtime_at_review_min,
    s.votes_up,
    s.weighted_vote_score,
    s.sentiment_score,
    s.sentiment_label,
    s.review_length,
    timestamp_seconds(s.timestamp_created)                   as created_ts
from source s
