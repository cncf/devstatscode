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
create index on ev_{{rnd}}(type);
analyze ev_{{rnd}};
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
drop table ev_{{rnd}};
