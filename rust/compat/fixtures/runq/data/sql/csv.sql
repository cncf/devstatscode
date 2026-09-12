select
  'plain' as a, 'with,comma' as b, 'with "quotes"' as c, E'multi\nline' as d, E'cr\rhere' as e,
  ' leading space' as f, 'trailing space ' as g, E'\ttab' as h, '\.' as i, '' as j, null as k,
  'Zażółć' as l, E'\u00a0nbsp' as m, 'a''b' as n
union all
select
  'row2', '', '"', E'\n', E'\r\n', '  ', ' ', '', '\\.', 'x', null, 'ż,ź', '"ż"', ''
;
