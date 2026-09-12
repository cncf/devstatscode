select
  count(id)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
  and (lower(dup_actor_login) {{exclude_bots}})
;
