select
  type,
  round(count(id) / {{n}}, 2) as n
from
  gha_events
where
  created_at >= '2015-08-13'::timestamp - '{{period}}'::interval
group by
  type
order by
  n desc,
  type asc
;
