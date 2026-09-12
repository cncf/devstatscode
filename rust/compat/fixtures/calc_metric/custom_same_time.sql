select
  'ncd,All' as metric,
  '{{from}}'::timestamp as dt,
  actor_id::float as value,
  dup_actor_login || ' (' || dup_repo_name || ')' as contributor
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
order by
  contributor asc,
  value asc
;
