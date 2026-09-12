select
  'hmc,' || dup_repo_name,
  dup_actor_login,
  created_at,
  actor_id::float,
  type
from
  gha_events
where
  {{period:created_at}}
  and (lower(dup_actor_login) {{exclude_bots}})
order by
  dup_repo_name asc,
  created_at asc
;
