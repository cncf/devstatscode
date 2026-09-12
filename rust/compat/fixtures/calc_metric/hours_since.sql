select
  coalesce(round(avg(extract(epoch from ('{{to}}'::timestamp - created_at)) / 3600.0)::numeric, 2), 0)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
;
