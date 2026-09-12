select
  type,
  round(count(id) * 24.0 / {{range}}, 3) as n
from
  gha_events
where
  {{period:created_at}}
group by
  type
order by
  n desc,
  type asc
;
