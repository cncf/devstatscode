select
  'evtr,' || type,
  round(count(id) * 24.0 / {{range}}, 3) as n
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
group by
  type
order by
  n desc,
  type asc
;
