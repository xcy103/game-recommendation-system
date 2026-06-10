-- Analytics mart: daily sentiment trends per game.
-- Useful for time-series dashboards and as a feature for the recommender.
select
    f.game_sk,
    g.appid,
    g.name,
    f.dt,
    count(*)                                                as total_reviews,
    countif(f.voted_up)                                     as positive_reviews,
    count(*) - countif(f.voted_up)                          as negative_reviews,
    safe_divide(countif(f.voted_up), count(*))              as positive_rate,
    avg(f.sentiment_score)                                  as avg_sentiment_score,
    countif(f.sentiment_label = 'pos')                      as sentiment_pos_count,
    countif(f.sentiment_label = 'neu')                      as sentiment_neu_count,
    countif(f.sentiment_label = 'neg')                      as sentiment_neg_count
from {{ ref('fact_reviews') }} f
inner join {{ ref('dim_games') }} g on f.game_sk = g.game_sk
group by f.game_sk, g.appid, g.name, f.dt
