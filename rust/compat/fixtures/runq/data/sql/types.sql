select
  id, i2, i4, i8, num, f4, f8, b, t, vc, ch, c1, nm, d, ts, tstz, tm, tmtz, iv, by, j, jb, u, ip, ia, ta, oid_
from
  runq_types
where
  id {{cond}}
order by
  id
;
