select
  'mvm;' || dup_repo_name || ';events,actors',
  count(id),
  count(distinct actor_id)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
group by
  dup_repo_name
order by
  dup_repo_name asc
;
