select
  'ncd,' || type as metric,
  created_at,
  actor_id::float as value,
  dup_actor_login || ' (' || dup_repo_name || ')' as contributor
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
  and (lower(dup_actor_login) {{exclude_bots}})
order by
  metric asc,
  created_at asc,
  contributor asc
;
