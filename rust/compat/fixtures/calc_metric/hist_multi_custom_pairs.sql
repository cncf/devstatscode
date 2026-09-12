select
  'hmcp;' || dup_repo_name || ';first,last',
  min(dup_actor_login),
  min(created_at),
  count(id),
  'f',
  max(dup_actor_login),
  max(created_at),
  count(distinct actor_id),
  'l'
from
  gha_events
where
  {{period:created_at}}
group by
  dup_repo_name
order by
  dup_repo_name asc
;
