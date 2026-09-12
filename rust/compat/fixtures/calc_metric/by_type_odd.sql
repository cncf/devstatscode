select
  case type when 'PushEvent' then ',' || type when 'IssuesEvent' then 'evt,-/-' else 'evt,' || type end,
  count(id)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
group by
  type
order by
  type asc
;
