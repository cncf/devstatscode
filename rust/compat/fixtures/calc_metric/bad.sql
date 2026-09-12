select
  count(id)
from
  no_such_table
where
  created_at >= '{{from}}'
  and created_at < '{{to}}'
;
