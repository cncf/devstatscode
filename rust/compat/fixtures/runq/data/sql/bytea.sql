select id, by, encode(by, 'hex') as hex from runq_types where id <= 3 order by id;
