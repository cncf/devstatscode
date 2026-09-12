select
  'ncm;' || dup_repo_name || ';first,last',
  min(created_at),
  count(id),
  min(dup_actor_login),
  max(created_at),
  count(distinct actor_id),
  max(dup_actor_login)
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
