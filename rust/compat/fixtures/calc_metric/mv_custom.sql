select
  'mvc,' || type,
  min(created_at),
  count(id),
  min(dup_actor_login)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
group by
  type
order by
  type asc
;
