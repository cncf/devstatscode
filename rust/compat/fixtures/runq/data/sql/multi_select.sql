select
  s.id, s.t
from (
  select
    id, t
  from
    runq_types
  where id <= 2
) s
order by
  s.id
;
