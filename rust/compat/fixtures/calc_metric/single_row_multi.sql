select
  'pushes,prs,issues',
  sum(case type when 'PushEvent' then 1 else 0 end),
  sum(case type when 'PullRequestEvent' then 1 else 0 end),
  sum(case type when 'IssuesEvent' then 1 else 0 end)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
;
