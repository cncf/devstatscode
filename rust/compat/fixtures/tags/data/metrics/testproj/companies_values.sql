select
  name,
  lower(name) as lname,
  length(name) as len,
  upper(name) as uname,
  substring(name from 1 for 1) as first
from
  gha_companies
where
  name != ''
order by
  name
;
