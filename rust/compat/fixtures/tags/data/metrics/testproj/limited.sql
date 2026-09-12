select
  type
from
  gha_events
group by
  type
order by
  count(*) desc,
  type asc
limit {{lim}}
;
