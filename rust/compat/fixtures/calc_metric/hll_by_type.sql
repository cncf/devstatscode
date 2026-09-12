select
  'hllt,' || type,
  hll_add_agg(hll_hash_bigint(actor_id))
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
