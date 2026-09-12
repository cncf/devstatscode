select
  'hllr;' || dup_repo_name || ';actors,types',
  hll_add_agg(hll_hash_bigint(actor_id)),
  hll_add_agg(hll_hash_text(type))
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
