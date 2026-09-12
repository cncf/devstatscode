select
  count(*) as cnt,
  {{range}} as hours,
  '{{from}}' as sfrom,
  '{{to}}' as sto
from
  gha_events e
where
  {{period:e.created_at}}
;
