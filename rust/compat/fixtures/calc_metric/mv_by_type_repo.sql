select
  'mv,' || type || '`' || dup_repo_name,
  count(id)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
group by
  type,
  dup_repo_name
order by
  type asc,
  dup_repo_name asc
;
