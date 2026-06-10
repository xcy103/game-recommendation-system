-- Generic test: assert that a ratio column is in [0, 1].
-- Returns rows that FAIL the assertion (i.e., value outside [0, 1]).
-- A passing test returns 0 rows.
{% test positive_rate_between_0_and_1(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
      {{ column_name }} < 0
      or {{ column_name }} > 1
  )
{% endtest %}
