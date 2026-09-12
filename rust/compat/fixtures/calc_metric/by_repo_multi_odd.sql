select
  case dup_repo_name when 'org/repo1' then ';' || dup_repo_name || ';events,actors' when 'other/repo3' then 'rg;(-);events,actors' else 'rg;' || dup_repo_name || ';events,actors' end,
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
