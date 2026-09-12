create temp table ev_{{rnd}} as
select
  id,
  type,
  actor_id
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
;
select
  'tt,' || type,
  count(id)
from
  ev_{{rnd}}
group by
  type
order by
  type asc
;
