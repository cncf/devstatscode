select
  'hm,' || dup_repo_name,
  type,
  count(id)
from
  gha_events
where
  created_at >= '2015-08-13'::timestamp - '{{period}}'::interval
group by
  dup_repo_name,
  type
order by
  dup_repo_name asc,
  count(id) desc,
  type asc
;
