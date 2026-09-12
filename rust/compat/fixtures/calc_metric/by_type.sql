select
  'evt,' || type,
  round(count(id) / {{n}}, 2) as n
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
  and (lower(dup_actor_login) {{exclude_bots}})
group by
  type
order by
  n desc,
  type asc
;
