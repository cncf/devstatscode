select s as n, repeat('x', s % 7) as x, s * 1.5 as f, (s % 2 = 0) as even, now() - (s || ' days')::interval < now() as past
from generate_series(1, {{n}}) s order by s;
