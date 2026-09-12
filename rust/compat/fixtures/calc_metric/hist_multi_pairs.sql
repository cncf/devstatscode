select
  'hmp;' || dup_repo_name || ';pushes,others',
  'push',
  sum(case type when 'PushEvent' then 1 else 0 end),
  'other',
  sum(case type when 'PushEvent' then 0 else 1 end)
from
  gha_events
where
  {{period:created_at}}
group by
  dup_repo_name
order by
  dup_repo_name asc
;
