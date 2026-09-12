select
  round((count(id) * {{project_scale}} / {{n}})::numeric, 3)
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
;
