select
  'hmvs;typ:s,evs:f,acts:f,who:s;types',
  type,
  count(id),
  count(distinct actor_id),
  min(dup_actor_login)
from
  gha_events
where
  {{period:created_at}}
group by
  type
order by
  type asc
;
