select 'a%b' as v, '100%' as w, '%s %d %v' as verbs, E'%\n%' as nl, '%%' as "%%" from generate_series(1, {{n}});
