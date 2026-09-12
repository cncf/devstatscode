select
  'hmv,evs:f`' || dup_repo_name,
  count(id),
  count(distinct actor_id),
  min(dup_actor_login)
from
  gha_events
where
  {{period:created_at}}
group by
  dup_repo_name
order by
  dup_repo_name asc
;
