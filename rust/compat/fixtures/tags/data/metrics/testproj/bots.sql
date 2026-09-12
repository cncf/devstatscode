select
  dup_actor_login
from
  gha_events
where
  (lower(dup_actor_login) {{exclude_bots}})
group by
  dup_actor_login
order by
  dup_actor_login
;
