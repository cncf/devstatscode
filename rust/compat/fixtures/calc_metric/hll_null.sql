select
  hll_add_agg(hll_hash_bigint(actor_id))
from
  gha_events
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
  and type = 'NoSuchEvent'
;
