select
  'hm,' || dup_repo_name,
  type,
  count(id)
from
  gha_events
where
  {{period:created_at}}
  and (lower(dup_actor_login) {{exclude_bots}})
group by
  dup_repo_name,
  type
order by
  dup_repo_name asc,
  count(id) desc,
  type asc
;
