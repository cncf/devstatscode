select
  actor_id::float
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
order by
  id
;
