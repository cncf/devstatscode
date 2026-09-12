select
  type,
  count(id) as n
from
  gha_events
where
  created_at >= '2015-08-13'::timestamp - '{{period}}'::interval
  and (lower(dup_actor_login) {{exclude_bots}})
group by
  type
order by
  n desc,
  type asc
;
